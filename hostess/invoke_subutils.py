"""working subutils module based on invoke"""
import time
from concurrent.futures import ThreadPoolExecutor, Future
from functools import partial
from typing import Optional, Literal, Mapping, Collection, Hashable, Any, \
    Callable

import invoke
from cytoolz import keyfilter, identity
from dustgoggles.composition import Composition
from dustgoggles.func import zero
from dustgoggles.structures import listify

from hostess.utilities import timeout_factory


class Nullify:

    @staticmethod
    def read(_size=None):
        return b""

    @staticmethod
    def write(_obj):
        return

    @staticmethod
    def flush():
        return

    @staticmethod
    def seek(_hence):
        return


class DispatchBuffer:
    def __init__(self, dispatcher: "Dispatcher", stream: str):
        self.dispatcher, self.stream = dispatcher, stream

    def read(self, _=None):
        return self.dispatcher.caches[self.stream]

    def write(self, message):
        return self.dispatcher.execute(message, stream=self.stream)

    @staticmethod
    def seek(*args, **kwargs):
        return

    @staticmethod
    def flush():
        return


def _wait_on(it):
    try:
        it.wait()
    except AttributeError:
        return


def _submit_callback(callback: Callable, waitable: Any) -> Future:
    return ThreadPoolExecutor(1).submit(callback, waitable)


def dispatch_callback(
    dispatch: "Dispatcher",
    callback: Optional[Callable] = None,
    step: Optional[Hashable] = None,
    to_wait_on: Any = None,
) -> Any:
    """simple callback for a Dispatcher"""
    _wait_on(to_wait_on)
    if callback is None:
        return dispatch.execute(step=step)
    else:
        return dispatch.execute(callback(), step=step)


def done_callback(
    dispatch: "Dispatcher",
    runner: invoke.runners.Result | invoke.runners.Runner
) -> Future:
    """simple dispatcher callback for invoke.runner / result completion"""
    def callback():
        if "returncode" in dir(runner):
            returncode = runner.returncode
        else:
            # noinspection PyUnresolvedReferences
            returncode = runner.process.returncode
        return {
            'success': not runner.failed,
            'returncode': returncode,
            'command': runner.command
        }

    return dispatch_callback(dispatch, runner, callback, "done")


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

    def yield_buffer(self, step):
        return DispatchBuffer(self, step)

    active_steps = ()
    singular = False


def _nonelist(obj):
    return [] if obj is None else obj


def console_stream_handler(
    out=None,
    err=None,
    done=None,
    handle_out=None,
    handle_err=None,
    handle_done=None
) -> Dispatcher:
    """
    produce a Dispatcher suited for capturing stdout, stderr, and process
    completion, optionally with inline callbacks.
    """
    out, err, done = map(_nonelist, (out, err, done))
    handler = Dispatcher(
        steps={"out": identity, "err": identity, "done": identity}
    )
    handler.add_send("out", pipe=handle_out, target=out)
    handler.add_send("err", pipe=handle_err, target=err)
    handler.add_send("done", pipe=handle_done, target=done)
    handler.singular = True
    return handler


class CBuffer:
    """
    wrapper class for a Dispatcher that includes the ability to yield
    pseudo-buffers that execute the Dispatcher on "write".
    streamlined alternative to invoke's Watchers etc. if no Dispatcher
    is passed during initialization, CBuffer creates a simple console handler.
    the Dispatcher should have at least steps "out", "err", and "done".
    """
    def __init__(self, dispatcher: Optional[Dispatcher] = None):
        if dispatcher is None:
            dispatcher = console_stream_handler()
        self.dispatcher = dispatcher
        self.caches = self.dispatcher.caches
        self.deferred_sends = None
        self.buffers = {
            stream: self.makebuffer(stream)
            for stream in self.dispatcher.keys()
        }
        if not {"out", "err", "done"}.issubset(self.buffers.keys()):
            raise TypeError(
                "dispatcher must have at least out, err, and done steps."
            )

    def __getattr__(self, attr):
        return self.dispatcher.__getattr__(attr)

    def execute(self, *args, stream, **kwargs):
        return self.dispatcher.execute(*args, **kwargs, steps=stream)

    def __call__(self, *args, stream, **kwargs):
        return self.execute(*args, stream, **kwargs)

    def cacheoff(self):
        self.deferred_sends = self.dispatcher.sends
        self.dispatcher.sends = {}

    def cacheon(self):
        if self.deferred_sends is None:
            return
        self.dispatcher.sends = self.deferred_sends
        self.deferred_sends = None

    def makebuffer(self, stream):
        return DispatchBuffer(self.dispatcher, stream)


def trydelete(obj, target):
    try:
        del obj[target]
    except KeyError:
        pass


def replace_aliases(mapping: dict, aliasdict: Mapping[str, Collection[str]]):
    """
    swap in alias keys in a dict. intended primarily as a helper for aliased
    kwargs. impure; mutates mapping if any keys match aliasdict.
    """
    for target, aliases in aliasdict.items():
        for a in filter(lambda k: k in mapping, aliases):
            trydelete(mapping, target)
            mapping[target] = mapping[a]
            del mapping[a]


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
        replace_aliases(
            rkwargs,
            {
                "out_stream": ("out",),
                "err_stream": ("err",),
                "asynchronous": ("async", "bg")
            }
        )
        # do not print to stdout/stderr by default
        verbose = rkwargs.pop("verbose", False)
        if verbose is not True:
            for k in filter(
                lambda k: k not in rkwargs, ("out_stream", "err_stream")
            ):
                rkwargs[k] = Nullify()
        # simple done callback handling -- simple stdout/stderr is handled
        # by Invoke, but Invoke does not offer completion handling except
        # via the more complex Watcher system.
        dcallback = rkwargs.pop("done", None)
        output = self.runclass(self.ctx).run(
            self.cstring(*args, **kwargs), **rkwargs
        )
        # need the runner/result to actually create a thread to watch the
        # done callback. we also never want to actually return a Promise
        # object because it tends to behave badly.
        if "runner" in dir(output):
            output = output.runner
        if dcallback is not None:
            _submit_callback(dcallback, output)
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
        cbuffer: CBuffer,
        metadata: Optional[Mapping] = None
    ):
        self.runner, self.cbuffer = runner, cbuffer
        self.metadata = {} if metadata is None else metadata

    def __getattr__(self, attr):
        try:
            return getattr(self.runner, attr)
        except AttributeError:
            return getattr(self.process, attr)

    def _is_done(self) -> bool:
        return self.runner.process_is_finished

    def _is_running(self) -> bool:
        return not self.runner.process_is_finished

    def __str__(self) -> str:
        runstring = "running" if self.running else "finished"
        base = f"Viewer for {runstring} process {self.cmd}, PID {self.pid}"
        outlist = self.out[-20:]
        if len(self.out) > 20:
            outlist = ["..."] + outlist
        return base + "".join([f"\n{line}" for line in outlist])

    def __repr__(self) -> str:
        return self.__str__()

    def wait_for_output(
        self,
        stream: Literal["out", "err"] = "out",
        poll=0.05,
        timeout=10
    ):
        if self.done:
            return
        stream = self.out if stream == "out" else self.err
        waiting, _ = timeout_factory(timeout=timeout)
        starting_length = len(stream)
        while (len(stream) == starting_length) and self.running:
            time.sleep(poll)
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
