"""working subutils module based on invoke"""
import os
import time
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import redirect_stdout, redirect_stderr
from functools import partial, wraps
from multiprocessing import Process, Pipe
from typing import Optional, Literal, Mapping, Collection, Hashable, Any, \
    Callable, MutableMapping, MutableSequence

import invoke
from cytoolz import keyfilter, identity
from dustgoggles.composition import Composition
from dustgoggles.func import intersection
from dustgoggles.structures import listify

from hostess.utilities import timeout_factory, curry, Aliased


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
        return self.dispatcher.execute(message, steps=(self.stream,))

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
        return dispatch.execute(steps=(step,))
    else:
        return dispatch.execute(callback(), steps=(step,))


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

    return dispatch_callback(dispatch, callback, "done", runner)


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

    def yield_callback(self, callback, step):
        return partial(dispatch_callback, self, callback, step)

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
            stream: self.make_buffer(stream)
            for stream in self.dispatcher.steps
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

    def make_buffer(self, stream):
        return DispatchBuffer(self.dispatcher, stream)

    def make_callback(self, callback, stream):
        return self.dispatcher.yield_callback(callback, stream)


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


class RunCommand:
    """callable for managed command execution via Invoke."""

    def __init__(self, command="", ctx=None, runclass=None, **kwargs):
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
        cstring = self.cstring(*args, **kwargs)
        if cstring == "":
            raise ValueError("no command specified.")
        output = self.runclass(self.ctx).run(cstring, **rkwargs)
        # need the runner/result to actually create a thread to watch the
        # done callback. we also never want to actually return a Promise
        # object because it tends to behave badly.
        if "runner" in dir(output):
            output = output.runner
        if dcallback is not None:
            _submit_callback(dcallback, output)
        return output

    def __str__(self):
        if (cstring := self.cstring()) == "":
            cstring = "no curried command"
        return f"{self.__class__.__name__} ({cstring})"

    def __repr__(self):
        return self.__str__()


class Viewer:
    """
    encapsulates an invoke.runners.Runner. does a variety of automated
    output handling, initialization, and metadata tracking, and prevents it
    from throwing errors in REPL environments.
    """

    def __init__(
        self,
        cbuffer: CBuffer,
        runner: Optional[invoke.runners.Runner] = None,
        metadata: Optional[Mapping] = None
    ):
        self.runner, self.cbuffer = runner, cbuffer
        self.metadata = {} if metadata is None else metadata
        self.out = cbuffer.caches['out']
        self.err = cbuffer.caches['err']

    def __getattr__(self, attr):
        if attr == "runner":
            return super().__getattribute__(attr)
        elif attr == "process":
            return self.runner.process
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
        base = f"Viewer for {runstring} process {self.args}, PID {self.pid}"
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
        cbuffer=None,
        **kwargs,
    ):
        """
        todo: this constructor still necessary? we'll see about the children
         / disown / remote stuff I guess.
        """
        if cbuffer is None:
            cbuffer = CBuffer()
        if not isinstance(command, RunCommand):
            command = RunCommand(command, ctx, runclass)
        # note that Viewers _only_ run commands asynchronously. use the wait or
        # wait_for_output methods if you want to block.
        base_kwargs = {
            '_bg': True,
            '_out': cbuffer.make_buffer('out'),
            '_err': cbuffer.make_buffer('err')
        }
        viewer = object.__new__(cls)
        if "_done" in kwargs:
            kwargs["_done"] = cbuffer.make_callback(kwargs["_done"], "done")
        viewer.__init__(cbuffer)
        viewer.runner = command(*args, **kwargs | base_kwargs)
        return viewer

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


def defer(func, *args, **kwargs):
    """wrapper to defer function execution."""

    def deferred():
        return func(*args, **kwargs)

    return deferred


def deferinto(func, *args, _target, **kwargs):
    """wrapper to defer function execution and place its result into _target"""

    def deferred_into():
        _target.append(func(*args, **kwargs))

    return deferred_into


def make_piped_callback(func: Callable) -> tuple[Pipe, Callable]:
    """
    make a callback that's suitable as a target for a multiprocessing
    object, wrapped in such a way that it sends its output back to the Pipe
    `here`.
    """
    here, there = Pipe()

    def sendback(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as ex:
            result = ex
        return there.send(result)

    return here, sendback


def piped(func: Callable, block=True) -> Callable:
    """
    wrapper to run a function in another process and get its results
    back through a pipe. if created in non-blocking mode, returns the Process
    object rather than the result; in this case, the caller is responsible for
    polling the process.
    """

    @wraps(func)
    def through_pipe(*args, **kwargs):
        here, sendback = make_piped_callback(func)
        proc = Process(target=sendback, args=args, kwargs=kwargs)
        proc.start()
        if block is True:
            proc.join()
            result = here.recv()
            proc.close()
            return result
        return proc

    return through_pipe


def make_call_redirect(func, fork=False):
    """
    a more intensive version of `piped` that directs stdout and stderr
    rather than simply a return value, and automatically places all these
    streams into caches accessible in the caller's [rpcess]. intended for
    longer-term, speculative, or callback-focused processes. if fork is True,
    runs in a double-forked, mostly-daemonized process.
    """
    r_here, r_there = Pipe()
    o_here, o_there = Pipe()
    p_here, p_there = Pipe()
    e_here, e_there = Pipe()

    # noinspection PyTypeChecker
    @wraps(func)
    def run_redirected(*args, **kwargs):
        if fork is True:
            if os.fork() != 0:
                return
            p_there.send(os.getpid())
        with (
            redirect_stdout(Aliased(o_there, ("write",), "send")),
            redirect_stderr(Aliased(e_there, ("write",), "send"))
        ):
            try:
                result = func(*args, **kwargs)
            except Exception as ex:
                result = ex
            return r_there.send(result)
    proximal = {'result': r_here, 'out': o_here, 'err': e_here}
    if fork is True:
        proximal['pids'] = p_here
    return run_redirected, proximal


def make_watch_caches():
    """shorthand for constructing the correct dictionary"""
    return {'result': [], 'out': [], 'err': []}


@curry
def watched_process(
    func: Callable,
    *,
    caches: MutableMapping[str, MutableSequence],
    fork: bool = False
) -> Callable:
    """
    decorator to run a function in a subprocess, redirecting its stdout,
    stderr, and any return value to `caches`. if fork is True, double-fork the
    execution so it will not terminate when the original calling process
    terminates. adds the kwargs _blocking and _poll to the decorated function,
    setting auto-join/poll and polling interval respectively.
    if _blocking is False, simply return the Process object and a dict of
    pipes: in this case, the calling process is responsible for polling the
    pipes if it wishes to receive output.
    """
    assert len(intersection(caches.keys(), {'result', 'out', 'err'})) == 3
    target, proximal = make_call_redirect(func, fork)

    @wraps(func)
    def run_and_watch(*args, _blocking=True, _poll=0.05, **kwargs):
        process = Process(target=target, args=args, kwargs=kwargs)
        process.start()
        caches['pids'] = [process.pid]
        if _blocking is False:
            return process, caches
        while True:
            for k, v in proximal.items():
                if v.poll():
                    caches[k].append(v.recv())
            if not process.is_alive():
                break
            time.sleep(_poll)
        return process, proximal

    return run_and_watch
