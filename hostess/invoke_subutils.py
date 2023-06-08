"""working subutils module based on invoke"""
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable

from cytoolz import keyfilter, identity
from dustgoggles.composition import Composition
from dustgoggles.func import zero
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


class CBuffer:
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


def done(
    func: Callable, runner: invoke.runners.Result | invoke.runners.Runner
):
    """simple callback wrapper for invoke.runner / result completion"""
    def wait_then_call():
        try:
            runner.wait()
        except AttributeError:
            pass
        if "returncode" in dir(runner):
            returncode = runner.returncode
        else:
            returncode = runner.process.returncode
        return func(returncode, runner.stdout, runner.stderr)

    executor = ThreadPoolExecutor(1)
    executor.submit(wait_then_call)


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
        for s in filter(lambda k: k in rkwargs, ("_bg", "_async")):
            rkwargs['_asynchronous'] = rkwargs[s]
        rkwargs = {k[1:]: v for k, v in rkwargs.items()}
        try:
            done_callback = rkwargs.pop("done")
        except KeyError:
            done_callback = None
        output = self.runclass(self.ctx).run(
            self.cstring(*args, **kwargs), **rkwargs
        )
        if done_callback is not None:
            done(done_callback, output)
        return output

    def __str__(self):
        return f"Command: {self.cstring()}"

    def __repr__(self):
        return self.__str__()


### NOT DONE !!! DO NOT USE !!! ###
class Viewer:
    """
    encapsulates an invoke.runners.Runner. does a variety of automated
    output handling, initialization, and metadata tracking, and prevents it
    from throwing errors in REPL environments.
    """

    def __init__(
            self,
            runner: invoke.runners.Runner,
            cbuffer: cbuffer,
            metadata=None
    ):
        (
            self.runner, self.cbuffer, self.metadata
        ) = runner, cbuffer, metadata

    def __getattr__(self, attr):
        try:
            return getattr(self.runner, attr)
        except AttributeError:
            return getattr(self.process, attr)

    def _is_done(self):
        return self.runner.process_is_finished

    def _is_running(self):
        return not self.runner.process_is_finished

    def __str__(self):
        runstring = "running" if self.running else "finished"
        base = f"Viewer for {runstring} process {self.cmd}, PID {self.pid}"
        outlist = self.out[-20:]
        if len(self.out) > 20:
            outlist = ["..."] + outlist
        return base + "".join([f"\n{line}" for line in outlist])

    def __repr__(self):
        return self.__str__()

    def wait_for_output(self, use_err=False, polling_interval=0.05,
                        timeout=10):
        if self.done:
            return
        stream = self.out if use_err is False else self.err
        waiting, _ = timeout_factory(timeout)
        starting_length = len(stream)
        while (len(stream) == starting_length) and self.running:
            time.sleep(polling_interval)
            waiting()
        return

    @classmethod
    def from_command(
            cls,
            command,
            *args,
            ctx=None,
            runclass=None,
            **kwargs,
    ):
        viewer = object.__new__(cls)
        if not isinstance(command, Command):
            command = Command(command, ctx, runclass)

    done = property(_is_done)
    running = property(_is_running)
    initialized = False
    _children = None
    _get_children = False
    _pid_records = None

#     @property
#     def children(self):
#         if self._get_children is False:
#             return None
#         if (self._children not in ([], None)) and not self.process.is_alive():
#             return self._children
#         if (self._pid_records is None) and (self._get_children is True):
#             raise ValueError(
#                 "This object has not been initialized correctly; cannot find "
#                 "spawned child processes."
#             )
#         if self.remote_pid is None:
#             raise ValueError(
#                 "The remote process does not appear to have correctly "
#                 "returned a process identifier."
#             )
#         ps_records = ps_to_records(self._pid_records)
#         try:
#             ppids = [self.remote_pid]
#             children = list(filter(lambda p: p['pid'] in ppids, ps_records))
#             generation = tuple(
#                 filter(lambda p: p['ppid'] in ppids, ps_records)
#             )
#             while len(generation) > 0:
#                 children += generation
#                 ppids = [p['pid'] for p in generation]
#                 generation = tuple(
#                     filter(lambda p: p['ppid'] in ppids, ps_records)
#                 )
#             self._children = children
#         except (StopIteration, KeyError):
#             warnings.warn("couldn't identify child processes.")
#             self._children = []
#         return self._children
