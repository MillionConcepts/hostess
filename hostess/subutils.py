"""utilities for executing, managing, and watching subprocesses"""
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import redirect_stdout, redirect_stderr
from functools import partial, wraps
from multiprocessing import Process, Pipe
import os
import time
from typing import (
    Any,
    Callable,
    Collection,
    Hashable,
    Literal,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Union,
)

import invoke
from cytoolz import keyfilter, identity
from dustgoggles.composition import Composition
from dustgoggles.func import intersection
from dustgoggles.structures import listify

from hostess.utilities import Aliased, curry, timeout_factory


class Nullify:
    """A fake buffer."""

    @staticmethod
    def read(_size=None):
        """read nothing"""
        return b""

    @staticmethod
    def write(_obj):
        """write nothing"""
        return

    @staticmethod
    def flush():
        """remain flushed"""
        return

    @staticmethod
    def seek(_hence):
        """seek nowhere"""
        return


class DispatchBuffer:
    """
    A buffer-like interface to a Dispatcher.
    """

    def __init__(self, dispatcher: "Dispatcher", step: Union[int, str]):
        """

        Args:
            dispatcher: Dispatcher to associate with this buffer
            step: named step of dispatcher to associate with this buffer
        """
        self.dispatcher, self.step = dispatcher, step

    def read(self, _=None) -> Any:
        """return the dispatcher's cache for the associated step"""
        return self.dispatcher.caches[self.step]

    def write(self, message: Any) -> Any:
        """execute the associated step of dispatcher with message"""
        return self.dispatcher.execute(message, steps=(self.step,))

    @staticmethod
    def flush():
        """remain flushed"""
        return

    @staticmethod
    def seek(*args, **kwargs):
        """seek nowhere"""
        return


def _wait_on(it: Any):
    """
    Block until it completes.

    Args:
        it: immediately call it.wait(). if it has no .wait(), do nothing.
    """
    try:
        it.wait()
    except AttributeError:
        return


def _submit_callback(callback: Callable[[Any], Any], waitable: Any) -> Future:
    """
    syntactic sugar for running a function in a thread.

    Args:
        callback: function to run in the thread. must take at least one
            argument.
        waitable: object to call `callback` with, presumably something it's
            waiting on.

    Returns:
        A Future object for the threaded callback(waitable) call.
    """
    return ThreadPoolExecutor(1).submit(callback, waitable)


def dispatch_callback(
    dispatch: "Dispatcher",
    callback: Optional[Callable] = None,
    step: Optional[Hashable] = None,
    to_wait_on: Any = None,
) -> Any:
    """
    simple callback for a Dispatcher. See Dispatcher.yield_callback for full
    documentation.
    """
    _wait_on(to_wait_on)
    if callback is None:
        return dispatch.execute(steps=(step,))
    else:
        return dispatch.execute(callback(), steps=(step,))


def done_callback(
    dispatch: "Dispatcher",
    runner: Union[invoke.runners.Result, invoke.runners.Runner],
) -> Future:
    """
    simple Dispatcher callback for invoke.runner / result completion.

    Args:
        dispatch: Dispatcher to trigger with callback
        runner: Result or Runner object. fire callback with information about
            process result when it completes.

    Returns:
         result of dispatcher "done" step executed with return value of
              locally-defined callback() function.
    """

    def callback():
        if "returncode" in dir(runner):
            returncode = runner.returncode
        else:
            # noinspection PyUnresolvedReferences
            returncode = runner.process.returncode
        return {
            "success": not runner.failed,
            "returncode": returncode,
            "command": runner.command,
        }

    return dispatch_callback(dispatch, callback, "done", runner)


class Dispatcher(Composition):
    """
    Composition capable of skipping steps. By default creates empty caches
    that can be used as targets for sends but do not autocapture like the
    superclass's add_captures() method.
    intended for applications like handling console streams.
    """

    def __init__(self, *args: Any, cached: bool = True, **kwargs: Any):
        """
        See Composition's documentation for a full description of valid
        args and kwargs.

        Args:
            *args: args to pass to the Composition constructor.
            cached: if True, retain results of every step in self.caches.
            **kwargs: kwargs to pass to the composition constructor.
        """
        super().__init__(*args, **kwargs)
        self.caches = {}
        if cached is True:
            self.reset_caches()

    def _bind_special_runtime_kwargs(self, kwargs):
        """Composition method modified to permit skipping."""
        steps = listify(kwargs.get("steps"))
        if self.singular is True:
            if (len(steps) > 1) or (steps == [None]):
                raise ValueError(
                    "singular Dispatcher, must be passed exactly 1 step"
                )
        self.active_steps = steps

    def _do_step(self, step_name, state):
        """Composition method modified to permit skipping."""
        if self.active_steps != [None]:
            if step_name not in self.active_steps:
                return state
        return super()._do_step(step_name, state)

    def reset_caches(self):
        """reset and initialize caches."""
        self.sends = {}
        for s in self.steps:
            self.caches[s] = []

    def __getattr__(self, attr):
        try:
            return self.caches[attr]
        except KeyError:
            pass
        raise AttributeError(f"No attribute or cache '{attr}'")

    def yield_buffer(self, step: Union[str, int]) -> DispatchBuffer:
        """
        construct a buffer whose read and write methods access and execute a
        cache/step of this object.

        Args:
            step: name of step to associate with the DispatchBuffer.
        """
        return DispatchBuffer(self, step)

    def yield_callback(
        self, signaler: Optional[Callable[[], None]], step: Union[str, int]
    ) -> Callable:
        """
        construct a callback function that fires a step of this object.

        Args:
            signaler: an optional niladic function whose output acts as a
                signal to the step when the callback fires. Otherwise, simply
                execute the step with no signal.
            step: name of step to execute with this callback.

        Returns:
            a function that takes one or no arguments. If it is called
                with no argument, it immediately fires the callback. If it
                is called with an object with a .wait method, it first
                calls its .wait method. This function returns whatever
                executing the step returns.
        """
        return partial(dispatch_callback, self, signaler, step)

    active_steps = ()
    singular = False


def _nonelist(obj):
    return [] if obj is None else obj


def strip_newline(text: str) -> str:
    """
    just strip newlines. helper function for console streams.

    Args:
        text: string to strip.

    Returns:
        text stripped of newlines.
    """
    return text.strip("\n")


def console_stream_handler(
    out: Optional[MutableSequence] = None,
    err: Optional[MutableSequence] = None,
    done: Optional[MutableSequence] = None,
    handle_out: Optional[Callable[[str], Any]] = None,
    handle_err: Optional[Callable[[str], Any]] = None,
    handle_done: Optional[Callable] = None,
) -> Dispatcher:
    """
    produce a Dispatcher suited for capturing stdout, stderr, and process
    completion, optionally with inline callbacks. Used by Viewer.

    Args:
        out: optional existing list to use as cache for stdout; if none
            specified, creats a new list.
        err: same but for stderr.
        done: same but for results of done callback.
        handle_out: optional inline formatter/callback for stdout. by default
            just strips newlines.
        handle_err: same but for stderr.
        handle_done: optional callback for process completion; defaults to
            simply returning the results of the Dispatcher's "done" step.

    Returns:
        a Dispatcher ready to handle console streams.
    """
    out, err, done = map(_nonelist, (out, err, done))
    handler = Dispatcher(
        steps={"out": strip_newline, "err": strip_newline, "done": identity}
    )
    handler.caches['out'] = out
    handler.caches['err'] = err
    handler.caches['done'] = done
    handler.add_send("out", pipe=handle_out, target=out)
    handler.add_send("err", pipe=handle_err, target=err)
    handler.add_send("done", pipe=handle_done, target=done)
    handler.singular = True
    return handler


class CBuffer:
    """
    wrapper class for a Dispatcher that includes the ability to yield
    pseudo-buffers that execute the Dispatcher on "write". streamlined
    alternative to objects like Invoke's Watchers.
    """

    def __init__(self, dispatcher: Optional[Dispatcher] = None):
        """
        Args:
            dispatcher: Dispatcher to associate with any buffers this object
                produces. the Dispatcher should have at least steps "out",
                "err", and "done". If not specified, defaults to a simple
                console handler.
        """
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

    def execute(
        self, *args: Any, stream: Sequence[Union[str, int]], **kwargs: Any
    ) -> Any:
        """
        execute a specified step or steps of the underlying Dispatcher.

        Args:
            *args: args to pass to dispatcher.execute()
            stream: names of steps to execute
            **kwargs: kwargs to pass to dispatcher.execute()

        Returns:
            results of dispatcher.execute()
        """
        return self.dispatcher.execute(*args, **kwargs, steps=stream)

    def __call__(self, *args, stream, **kwargs):
        return self.execute(*args, stream, **kwargs)

    def cacheoff(self):
        """Pause caching."""
        self.deferred_sends = self.dispatcher.sends
        self.dispatcher.sends = {}

    def cacheon(self):
        """Resume caching."""
        if self.deferred_sends is None:
            return
        self.dispatcher.sends = self.deferred_sends
        self.deferred_sends = None

    def make_buffer(self, step: Union[str, int]) -> DispatchBuffer:
        """
        Create a buffer for a named step of self.dispatcher.

        Args:
            step: name of step.

        Returns:
            DispatchBuffer for that step.
        """
        return DispatchBuffer(self.dispatcher, step)

    def make_callback(
        self,
        signal_generator: Optional[Callable[[], Any]],
        step: Union[str, int],
    ) -> Callable:
        """
        create a callback for a named step of self.dispatcher.

        Args:
            signal_generator: optional niladic function whose output will
                be used as a signal to the named step of dispatcher. otherwise
                the callback will execute that step with no arguments.
            step:
                name of step to execute when callback fires.

        Returns:
            callback function that takes a waitable object as an optional
                argument and executes a step of dispatcher when it completes.
        """
        return self.dispatcher.yield_callback(signal_generator, step)


def trydelete(obj: Union[MutableSequence, MutableMapping], target: Hashable):
    """
    attempt to delete the key or item named "obj" from target. Do nothing if
    it doesn't exist.
    """
    try:
        del obj[target]
    except (KeyError, IndexError):
        pass


def replace_aliases(
    mapping: MutableMapping, aliasdict: Mapping[str, Collection[str]]
):
    """
    replace keys of a mapping with aliases. intended primarily as a helper for
    aliased kwargs. impure; mutates mapping if any keys match aliasdict.
    """
    for target, aliases in aliasdict.items():
        for a in filter(lambda k: k in mapping, aliases):
            trydelete(mapping, target)
            mapping[target] = mapping[a]
            del mapping[a]


class RunCommand:
    """
    Callable object for managed shell command execution.

    Encapsulates an Invoke runner and adds additional syntax and monitoring.
    Often, but not always, best used as the substrate for a Viewer.
    """

    def __init__(
        self,
        command: Optional[str] = None,
        ctx: Optional[invoke.context.Context] = None,
        runclass: Optional[type(invoke.Runner)] = None,
        **kwargs: Any,
    ):
        """
        Args:
            command: optional string form of a shell command to bind to
                this object. If you do not pass this, this object will be
                "generic", able to run any shell command. For instance,
                ls = RunCommand("ls") constructs a RunCommand _just_ for
                ls. ls(a=True) will run "ls -a".
            ctx: optional Context object (just makes a new one if not given)
            runclass: optional Runner subclass (uses the default of ctx if
                not given; if both are None, uses invoke.runners.Local)
            **kwargs: optional keyword arguments to bind to this object;
                Will be added to any kwargs passed to calls to this object.
                See __call__ for a complete description of behavior.
        """
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

    def cstring(
        self,
        *args: Union[int, float, str],
        args_at_end: bool = True,
        **kwargs: Union[int, float, str],
    ):
        """
        Create a shell command string from *args and **kwargs, including any
        command and kwargs curried into this object. Used as part of the
        `__call__()` workflow; can also be used to determine what shell
        command this object _would_ execute if you called it.

        See `__call__` for a complete description of arg and kwarg parsing.

        Args:
            args: args to parse into shell command
            args_at_end: place args after the first at the end of the shell
                command?
            kwargs: kwargs to parse into shell command
        """
        args = self.args + args
        if self.command is None:
            if len(args) == 0:
                # if we have no bound command, always treat the first arg as
                # the command name, not as an option
                raise ValueError("No bound command; must pass an argument.")
            command, args = args[0], args[1:]
        else:
            command = self.command
        kwargs = keyfilter(
            lambda a: (not a.startswith("_")) or a.strip("_").isnumeric(),
            self.kwargs | kwargs,
        )
        astring = "" if len(args) == 0 else f" {' '.join(map(str, args))}"
        kstring = ""
        for k, v in kwargs.items():
            k = k.strip("_").replace("_", "-")
            if v is True:
                if len(k) == 1:
                    kstring += f" -{k}"
                else:
                    kstring += f" --{k}"
            elif v is False:
                continue
            elif len(k) == 1:
                kstring += f" -{k} {v}"
            else:
                kstring += f" --{k}={v}"
        order = (kstring, astring) if args_at_end else (astring, kstring)
        return f"{command}{''.join(order)}"

    def __call__(self, *args, **kwargs) -> Optional["Processlike"]:
        """
        Execute a shell command parsed from `args` and `kwargs`.

        This method has two legal calling conventions along with a variety of
        keyword-argument meta-options that modify _how_ it executes the shell
        command.

        Calling conventions:
            These conventions are not mutually exclusive, although it is
            generally less confusing to pick one or the other.

            **1**

            Pass the shell command as a string. This can be simpler in many cases,
            and is mandatory for programs with non-standard calling conventions,
            like the `ffmpeg` command below.

            Examples:
                >>> cmd = RunCommand()
                >>> cmd('ls')
                >>> cmd('cp -r /path/to/folder /path/to/other/folder')
                >>> cmd('ffmpeg -i first.mp4 -filter:v "crop=100:10:20:200" second.mp4')

            **2**

            Construct the shell command using multiple arguments to `__call__`.
            This allows you to treat the shell command more like a Python
            function, which can be simpler and less error-prone than dynamic
            string formatting when you would like to pass variable parameters to a
            command.

            RunCommand uses the following rules to parse args and kwargs into
            shell command strings. They are compatible with most, although not
            all, shell programs:

            * RunCommand treats the first positional argument like a shell
              command name. This means that passing a positional argument is
              mandatory if you did not bind a command to the RunCommand when
              creating it. The parsed command string always starts with this
              argument.
            * Subsequent positional arguments are command parameters. By
              default, the parser places them at the end of the command string,
              after any command options. Pass `_args_at_end=False` to place
              them before command options.
            * Keyword arguments are command options. The parser transforms
              keyword argument names in order to make Python naming and calling
              conventions compatible with shell conventions.
                * It ignores `"_"` characters at the start and end of
                  names. This can be used to pass numeric options, like
                  `"_0=True"`, or options that share a name with a Python
                  reserved keyword, like `"dir_=/opt"`.
                * it treats single-character names (not counting prefixed or
                  suffixed `"_"`) as options preceded by a `"-"` and does not
                  use an `'='` to separate them from their values.
                  `cmd("ls", I="a*")` is equivalent to `"ls -I a*"`.
                * it treats longer names as options preceded by `"--"` and uses
                  an `'='` to separate them from their values.
                  `cmd("ls", width=20)` is equivalent to `"ls --width=20"`.
                * It replaces `"_"` characters within names with `"-"`.
                  `cmd("ls", time_style="iso")` is equivalent to
                  `"ls --time-style=iso"`.
                * if a keyword argument is True, RunCommand treats it as a
                  "switch". `cmd("ls", a=True)` is equivalent to `"ls -a"`.
                * if a keyword argument is False, RunCommand ignores it.
                  `cmd("ls", a=False)` is equivalent to `"ls"`.

        Meta-options:
            RunCommand understands any kwarg whose name begins with `'_'` as
            a meta-option, unless the remainder of the kwarg name is numeric
            (e.g. `'_0'`). All meta-options are optional. The meta-options are
            not defined in the signature in order to facilitate the parsing
            process. Internally-recognized meta-options are:

            * `_bg` (aliases `_asynchronous`, `_async`): if True, execute the
                 process in the background. Roughly equivalent to the '&'
                 control operator in bash. If False, block until process exit.
                 (Default False)
             * `_out` (alias `_out_stream`): Additional target for process
                stdout.  Must have a `write()` method. (If `_viewer=True`,
                defaults to `Viewer`'s default behavior. Otherwise, defaults
                to a `Nullify` object, which simply discards the stream).
             * `_err` (alias `_err_stream`): Same as `_out`, but for stderr.
             * `_viewer` (alias `_v`): if True, return a `Viewer` object
                permitting additional process inspection and management.
                `_viewer=True`  implies `_bg=True`. (default False)
             * `_args_at_end`: if True, place positional arguments other than
                the command string at the end of the parsed shell command,
                after any shell options parsed from kwargs. Otherwise, place
                them before shell options. (Default True)
             * `_done`: a niladic function to call on process exit. Valid only
                if `_viewer=True`.

             Any other meta-options are passed directly to the underlying
             `Runner` as keyword arguments with `"_"` stripped from their
             names. `_disown` is often particularly useful.

        Tip:
            In addition to these conventions, bear in mind that if you
            specified a command when you construct a RunCommand object, that
            command will always be used as the first positional argument to
            `__call__()`, and if you specified kwargs, they will be added to
            any kwargs you pass or don't pass to `__call__()`.

         Returns:
             Interface object for executed process. Of variable type: if
                 `_viewer=True`, a Viewer; if `_viewer=False` and `_bg=True`,
                 an Invoke `Result`; if `_viewer=False` and `_bg=False`, an
                 Invoke `Runner`.
        """
        rkwargs = keyfilter(
            lambda k: k.startswith("_") and not k.strip("_").isnumeric(),
            self.kwargs | kwargs,
        )
        kwargs = keyfilter(lambda k: k not in rkwargs, self.kwargs | kwargs)
        replace_aliases(
            rkwargs,
            {
                "_out_stream": ("_out",),
                "_err_stream": ("_err",),
                "_asynchronous": ("_async", "_bg"),
                "_viewer": ("_v",),
            },
        )
        # do not print to stdout/stderr by default
        verbose = rkwargs.pop("verbose", False)
        if verbose is not True:
            for stream in filter(
                lambda x: x not in rkwargs, ("_out_stream", "_err_stream")
            ):
                rkwargs[stream] = Nullify()
        # simple done callback handling -- simple stdout/stderr is handled
        # by Invoke, but Invoke does not offer completion handling except
        # via the more complex Watcher system.
        dcallback = rkwargs.pop("_done", None)
        cstring = self.cstring(
            *args, args_at_end=rkwargs.pop("_args_at_end", True), **kwargs
        )
        if cstring == "":
            raise ValueError("no command specified.")
        if rkwargs.pop("_viewer", False) is True:
            output = Viewer.from_command(
                self,
                *args,
                ctx=self.ctx,
                runclass=self.runclass,
                **(rkwargs | kwargs),
            )
        else:
            rkwargs = {k[1:]: v for k, v in rkwargs.items()}
            output = self.runclass(self.ctx).run(cstring, **rkwargs)
        # disowned case
        if output is None:
            return
        # need the runner/result to actually create a thread to watch the
        # done callback. we also never want to actually return a Promise
        # object because it tends to behave badly.
        if ("runner" in dir(output)) and (not isinstance(output, Viewer)):
            output = output.runner
        if dcallback is not None:
            _submit_callback(dcallback, output)
        output.command = cstring
        return output

    def __str__(self):
        if self.command is None:
            return f"{self.__class__.__name__} (no curried command)"
        return f"{self.__class__.__name__} ({self.cstring()})"

    def __repr__(self):
        return self.__str__()


class Viewer:
    """
    encapsulates an instance of a `RunCommand` subclass or other process
    abstraction. performs a variety of automated output handling, process
    initialization, and metadata tracking operations, and also prevents the
    abstraction from throwing errors or unexpectedly blocking in REPL
    environments. `Viewer.from_command()` is the preferred constructor for
    most purposes.

    `Viewer` pretends to inherit most of the attributes of its encapsulated
    process abstraction, so in addition to attributes explicitly defined on
    `Viewer`, you can access attributes like `.done` and call methods like
    `.kill()`.
    """

    def __init__(
        self,
        cbuffer: CBuffer,
        runner: Optional[type(invoke.Runner)] = None,
        metadata: Optional[Mapping] = None,
    ):
        self.runner, self.cbuffer = runner, cbuffer
        self.metadata = {} if metadata is None else metadata
        self.out = cbuffer.caches["out"]
        self.err = cbuffer.caches["err"]

    # TODO: untangle this a bit
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
        cmdlines = self.command.split("\n")
        cmdstring = f"{cmdlines[0]}..." if len(cmdlines) > 1 else cmdlines[0]
        base = f"Viewer for {runstring} process {cmdstring}"
        try:
            base += f", PID {self.pid}"
        except AttributeError:
            # TODO: fetch remote PIDs with shell tricks
            pass
        outlist = self.out[-20:]
        if len(self.out) > 20:
            outlist = ["..."] + outlist
        return base + "".join([f"\n{line}" for line in outlist])

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def host(self):
        return self.runner.context.host

    def wait_for_output(
        self,
        stream: Literal["out", "err", "any"] = "any",
        poll: float = 0.05,
        timeout: float = 10,
    ):
        """
        Block until the Viewer receives output on the specified stream(s) or
            its process exits.

        Args:
            stream: "out" to wait for output on stdout, "err" to wait for
                output on stderr, "any" for either.
            poll: poll rate (seconds)
            timeout: how long to wait between successive outputs before
                raising a TimeoutError (seconds)
        """
        if self.done:
            return
        streams = {
            "out": (self.out,),
            "err": (self.err,),
            "any": (self.out, self.err),
        }[stream]
        waiting, _ = timeout_factory(timeout=timeout)
        starting = [len(s) for s in streams]
        while (
            all(len(s) == l for s, l in zip(streams, starting))
            and self.running
        ):
            time.sleep(poll)
            waiting()
        return

    @classmethod
    def from_command(
        cls,
        command: Union[str, RunCommand],
        *args: Any,
        ctx: Optional[invoke.context.Context] = None,
        runclass: Optional[invoke.Runner] = None,
        cbuffer: Optional[CBuffer] = None,
        **kwargs: Any,
    ) -> "Viewer":
        """
        Construct a `Viewer` from a command. This is the most convenient
        constructor for `Viewer` and should generally be preferred to
        `Viewer.__init__`.

        Args:
            command: Either a shell command as a string, or an existing
                `RunCommand` object.
            args: additional arguments for the executed shell command. See
                `RunCommand.__call__()` for a detailed description of behavior.
            ctx: optional Invoke `Context` for Viewer. Just creates a new one
                if not specified.
            runclass: underlying Invoke `Runner` class for this `Viewer`. if
                not specified, defaults to the default runclass of `command`,
                if it has one, and the default `runclass` of `RunCommand` if it
                does not.
            cbuffer: context buffer for `Viewer`. Creates a new `CBuffer` if
                not specified.
            kwargs: additional keyword arguments for the executed shell
                command. See `RunCommand.__call__()` for a detailed
                description of behavior.

        Returns:
            a `Viewer` constructed from `command` .
        """
        if cbuffer is None:
            cbuffer = CBuffer()
        if not isinstance(command, RunCommand):
            command = RunCommand(command, ctx, runclass)
        # note that Viewers _only_ run commands asynchronously. use the wait
        # or wait_for_output methods if you want to block.
        base_kwargs = {
            "_bg": True,
            "_out": cbuffer.buffers["out"],
            "_err": cbuffer.buffers["err"],
        }
        viewer = object.__new__(cls)
        if "_done" in kwargs:
            kwargs["_done"] = cbuffer.make_callback(kwargs["_done"], "done")
        viewer.__init__(cbuffer)
        viewer.runner = command(*args, **kwargs | base_kwargs, _viewer=False)
        return viewer

    # TODO: implement children property

    done = property(_is_done)
    running = property(_is_running)
    initialized = False
    _pid_records = None


def defer(func: Callable, *args: Any, **kwargs: Any) -> Callable[[], Any]:
    """
    Defer a function call.

    Args:
        func: function whose call to defer
        args: positional arguments to deferred call
        kwargs: keyword arguments to deferred call

    Returns:
        A niladic function that, when called, executes `func(*args, **kwargs)`.
    """

    def deferred():
        return func(*args, **kwargs)

    return deferred


def deferinto(
    func: Callable, *args: Any, _target: MutableSequence, **kwargs: Any
) -> Callable[[], None]:
    """
    Defer a function call and redirect its result.

    Args:
        func: function whose call to defer
        *args: positional arguments for the deferred call
        _target: object to append the call's return value to (must have an
            .append() method; a list is suitable)
        **kwargs: keyword arguments for the deferred call

    Returns:
        A niladic function that, when called, executes func(*args, **kwargs),
            but appends its return value to _target rather than returning it.
    """

    def deferred_into():
        _target.append(func(*args, **kwargs))

    return deferred_into


def make_piped_callback(func: Callable) -> tuple[Pipe, Callable]:
    """
    Turn a function into a callback. The decorated function sends its result
    to a Pipe rather than returning it, allowing its caller to receive output
    from it even if it is executed in a subprocess.

    Note that this is not usable with the @ decorator syntax because it
    returns a tuple.

    Args:
        func: function to turn into a callback.

    Returns:
        * a Pipe the decorated function will send its output to if called
        * the decorated function
    """
    here, there = Pipe()

    def sendback(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as ex:
            result = ex
        return there.send(result)

    return here, sendback


def piped(func: Callable, block: bool = True) -> Callable:
    """
    the youngest sibling of the piped() / make_call_redirect() /
    watched_process() family.

    decorator that modifies a function so that, when called, it executes in
    a subprocess rather than the calling interpreter's process.

    Args:
        func: function to decorate
        block: if True, the decorated function blocks until completion when
            called (like a regular function). If False, calling the decorated
            function returns a Process object rather than its normal return
            value. In this case, the caller is responsible for polling the
            Process if it wishes to receive a return value from the function.
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


def make_call_redirect(
    func: Callable, fork: bool = False
) -> tuple[Callable, dict[str, Pipe]]:
    """
    the middle sibling of the piped() / make_call_redirect() /
    watched_process() family.

    Modify a function so that, when called, it runs in a subprocess,
    directing its output into a dict of pipes. Intended for longer-term,
    speculative, or callback-focused functions than piped(). The calling
    process is always responsible for polling the pipes; watched_process()
    provides a more automated alternative.

    This mutates the return signature of the modified function so that it
    always returns None. It redirects its return value to the 'result' Pipe.

    Note that this function cannot be used with the `@` decorator syntax
    because it returns a `tuple`.

    Args:
        func: function to be modified.
        fork: if True, execute `func` in a double-forked, mostly-daemonized
            process when called.

    Returns:
        redirected_func: the modified function
        pipes: a dict of `Pipe` objects `redirected_func` will redirect its
            output to. keys are "out" (stdout), "err" (stderr), and "result"
            (return value).
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
            redirect_stderr(Aliased(e_there, ("write",), "send")),
        ):
            try:
                result = func(*args, **kwargs)
            except Exception as ex:
                result = ex
            return r_there.send(result)

    proximal = {"result": r_here, "out": o_here, "err": e_here}
    if fork is True:
        proximal["pids"] = p_here
    return run_redirected, proximal


def make_watch_caches() -> dict[str, list]:
    """
    construct an empty 'caches' structure for watched_process()

    Returns:
        dictionary of lists suitable for capturing output of a watched process
    """
    return {"result": [], "out": [], "err": []}


@curry
def watched_process(
    func: Callable,
    *,
    caches: MutableMapping[str, MutableSequence],
    fork: bool = False,
) -> Callable:
    """
    the eldest sibling of the piped() / make_call_redirect() /
    watched_process() family.

    decorate a function so that calling it will execute it in a subprocess
    rather than the calling interpreter's process, redirecting its stdout,
    stderr, and any return value to 'caches'.

    This mutates the decorated function's call and return signatures.
    It adds the kwargs _blocking (default True) and _poll (default 0.05),
    which set auto-join/poll and poll interval (s) respectively.

    If _blocking is True, calling the decorated function blocks until the
    subprocess exits and returns a tuple whose first element is its normal
    return value and whose second argument is the caches dict. This should
    generally be used in a thread, unless you have some unusual reason to
    execute a subprocessed function serially.

    If _blocking is False, the decorated function returns a tuple whose
    first value is a Process object and whose second value is a dict of pipes.
    Note that in this case, the calling process is responsible for
    polling the pipes if it wishes to receive output.

    Note that caches is a mandatory argument with no default value. To use
    this function with the @ decorator syntax, do something like:
    ```
    IMAGE_OUTPUT = make_watch_caches()

    @watched_process(caches=IMAGE_OUTPUT)
    def process_image( ...
    ```

    Args:
        func: function to run in subprocess
        caches: dict of lists for func's output. 'result' will contain
            any return value; 'out', its stdout; 'err', its stderr.
            make_watch_caches() constructs a suitable set of empty caches.
        fork: if True, double-fork the subprocess so it will not terminate
            if the calling process exits.

    Returns:
        function with mutated signature that, when called, executes in a
            subprocess.
    """
    assert len(intersection(caches.keys(), {"result", "out", "err"})) == 3
    target, proximal = make_call_redirect(func, fork)

    @wraps(func)
    def run_and_watch(*args, _blocking=True, _poll=0.05, **kwargs):
        process = Process(target=target, args=args, kwargs=kwargs)
        process.start()
        caches["pids"] = [process.pid]
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


# convenience aliases


def runv(*args, **kwargs):
    """run a command in a Viewer"""
    return Viewer.from_command(*args, **kwargs)


def run(*args, **kwargs):
    """run a command not in a Viewer"""
    return RunCommand(*args, **kwargs)().stdout


Processlike = Union[Viewer, invoke.runners.Runner, invoke.runners.Result]
"""union of expected types for interfaces to processes."""
