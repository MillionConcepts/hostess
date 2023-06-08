"""working subutils module based on invoke"""
from typing import Optional

from cytoolz import keyfilter, identity
from dustgoggles.composition import Composition
from dustgoggles.structures import listify
import invoke


class Dispatcher(Composition):
    """
    Composition capable of skipping steps. can also cache outputs by default.
    intended for applications like handling console streams.
    """

    def __init__(self, *args, cached=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.caches = {}
        if cached is True:
            self.reset_caches()

    def _bind_special_runtime_kwargs(self, kwargs):
        steps = listify(kwargs.get("steps"))
        if self.singular is True:
            if (len(steps) > 1) or (steps == [None]):
                raise ValueError(
                    "singular Dispatcher, must be passed exactly 1 step"
                )
        self.active_steps = steps

    def _do_step(self, step_name, state):
        if self.active_steps != [None]:
            if step_name not in self.active_steps:
                return state
        return super()._do_step(step_name, state)

    def reset_caches(self):
        self.sends = {}
        for s in self.steps:
            self.caches[s] = []
            self.add_send(s, target=self.caches[s])

    def __getattr__(self, attr):
        try:
            return self.caches[attr]
        except KeyError:
            pass
        raise AttributeError(f"No attribute or cache '{attr}'")

    active_steps = ()
    singular = False


def console_stream_handler(
    handle_out=identity, handle_err=identity, out=None, err=None
):
    """
    produce a Composition suited for handling stdout and stderr.
    optionally add inline callbacks.
    """
    out = [] if out is None else out
    err = [] if err is None else err
    handler = Dispatcher({"out": handle_out, "err": handle_err})
    handler.add_send("out", target=out)
    handler.add_send("err", target=err)
    handler.singular = True
    return handler


class CBuffers:
    """
    simple class to accept writes and execute a Composition in response.
    streamlined alternative to invoke's Watchers etc. if no Composition
    is passed during initialization, creates a default console handler.
    """

    def __init__(self, composition: Optional[Composition] = None):
        if composition is None:
            composition = console_stream_handler()
        self.composition = composition
        self.caches = self.composition.caches
        self.deferred_sends = None

    def __getattr__(self, attr):
        return self.composition.__getattr__(attr)

    def execute(self, *args, stream, **kwargs):
        return self.composition.execute(*args, **kwargs, steps=stream)

    def cacheoff(self):
        self.deferred_sends = self.composition.sends
        self.composition.sends = {}

    def cacheon(self):
        if self.deferred_sends is None:
            return
        self.composition.sends = self.deferred_sends
        self.deferred_sends = None

    def makebuffer(self, stream):
        class PseudoBuffer:
            @staticmethod
            def write(message):
                return self.execute(message, stream=stream)

            @staticmethod
            def flush():
                return

        return PseudoBuffer()


class Command:
    """factory for command executions via Invoke."""

    def __init__(self, command, ctx=None, runclass=None, **kwargs):
        self.command = command
        if ctx is None:
            self.ctx = invoke.context.Context()
        else:
            self.ctx = ctx
        if runclass is None:
            self.runclass = list(self.ctx["runners"].values())[0]
        else:
            self.runclass = runclass
        self.args, self.kwargs = (), kwargs

    def bind(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs

    def cstring(self, *args, **kwargs):
        args = self.args + args
        kwargs = keyfilter(
            lambda k: not k.startswith("_"), self.kwargs | kwargs
        )
        astring = " ".join(args)
        kstring = " --".join([f"{k}={v}" for k, v in kwargs.items()])
        cstring = self.command
        for s in (astring, kstring):
            cstring = cstring + f" {s}" if s != "" else cstring
        return cstring

    def __call__(
        self, *args, **kwargs
    ) -> invoke.runners.Runner | invoke.runners.Result:
        rkwargs = keyfilter(lambda k: k.startswith("_"), self.kwargs | kwargs)
        rkwargs = {k[1:]: v for k, v in rkwargs.items()}
        return self.runclass(self.ctx).run(
            self.cstring(*args, **kwargs), **rkwargs
        )

    def __str__(self):
        return f"Command: {self.cstring()}"

    def __repr__(self):
        return self.__str__()
