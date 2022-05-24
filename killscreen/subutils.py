"""
helper functions for interacting with subprocesses, including the sh library
"""
import re
import time
import warnings
from functools import partial
from multiprocessing import Pipe
from pathlib import Path
from typing import Sequence, Union

import sh
from cytoolz import juxt, valfilter
from dustgoggles.func import zero


def append_write(path, text):
    with open(path, "a+") as file:
        file.write(text)


def target_to_method(stream_target):
    if isinstance(stream_target, Path):
        return partial(append_write, stream_target)
    t_dir = dir(stream_target)
    for method_name in ("write", "append", "__call__", "print"):
        if method_name in t_dir:
            return getattr(stream_target, method_name)


def make_stream_handler(targets):
    if targets is None:
        return None
    return juxt(tuple(map(target_to_method, targets)))


def console_stream_handlers(out_targets=None, err_targets=None):
    """
    create a pair of stdout and stderr handler functions to pass to a
    `sh` subprocess call.
    """
    out_actions = make_stream_handler(out_targets)
    err_actions = make_stream_handler(err_targets)

    def handle_out(message):
        out_actions(message)

    def handle_err(message):
        err_actions(message)

    return {"_out": handle_out, "_err": handle_err}


def piped(func):
    here, there = Pipe()

    def sendback(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except Exception as ex:
            result = ex
        return there.send(result)

    return here, sendback


class Viewer:
    """
    encapsulates a sh.RunningCommand object (or conceivably another process
    abstraction, with some extra work). prevents its __str__ and __repr__
    methods from blocking in REPL environments. may be called as a constructor
    with handler functions to allow access to streams from a command, again
    without blocking.
    """
    def __init__(self, command=None, _host=None):
        self.process = command
        if isinstance(command, sh.RunningCommand):
            self._populate_from_running_command()
        elif isinstance(command, partial):
            if isinstance(command.func, sh.RunningCommand):
                self._populate_from_running_command()
        self.host = _host

    process = None
    pid = None
    cmd = None
    kill = zero
    terminate = zero
    wait = zero
    is_alive = zero
    out = None
    err = None
    remote_pid = None
    _initialized = False
    _children = None
    _get_children = False
    _pid_records = None

    def _populate_from_running_command(self):
        self._initialized = True
        self.pid = self.process.pid
        self.cmd = " ".join([token.decode() for token in self.process.cmd])
        self.kill = self.process.kill
        self.terminate = self.process.terminate
        self.wait = self.process.wait
        self.is_alive = self.process.is_alive

    def wait_for_output(self, polling_interval=0.05, timeout=10):
        if self.is_alive() in (False, None):
            return
        if isinstance(self.out, Sequence):
            starting_length, start_time = len(self.out), time.time()
            while (
                ((time.time() - start_time) < timeout)
                and (len(self.out) == starting_length)
            ):
                time.sleep(polling_interval)
            if len(self.out) == starting_length:
                raise TimeoutError
            return
        self.process.next()

    def run(self):
        if self.process is None:
            raise TypeError("Nothing to run.")
        if isinstance(self.process, sh.RunningCommand):
            raise TypeError("Already running.")
        self.process = self.process()
        self._populate_from_running_command()

    @property
    def children(self):
        if self._get_children is False:
            return None
        if (self._children not in ([], None)) and not self.process.is_alive():
            return self._children
        if (self._pid_records is None) and (self._get_children is True):
            raise ValueError(
                "This object has not been initialized correctly; cannot find "
                "spawned child processes."
            )
        if self.remote_pid is None:
            raise ValueError(
                "The remote process does not appear to have correctly "
                "returned a process identifier."
            )
        ps_records = ps_to_records(self._pid_records)
        try:
            ppids = [self.remote_pid]
            children = list(filter(lambda p: p['pid'] in ppids, ps_records))
            generation = tuple(
                filter(lambda p: p['ppid'] in ppids, ps_records)
            )
            while len(generation) > 0:
                children += generation
                ppids = [p['pid'] for p in generation]
                generation = tuple(
                    filter(lambda p: p['ppid'] in ppids, ps_records)
                )
            self._children = children
        except (StopIteration, KeyError):
            warnings.warn("couldn't identify child processes.")
            self._children = []
        return self._children

    def __str__(self):
        if self._initialized is False:
            return f"Viewer for unexecuted process {self.process}"
        if self.process.is_alive():
            base = f"running command {self.cmd}, PID {self.pid}"
        else:
            base = self.process.__str__()
        return base + "".join([f"\n{line}" for line in self.out])

    def __repr__(self):
        if self._initialized is False:
            return self.__str__()
        if self.process.is_alive():
            return self.__str__()
        if (
            (len(self.process.stdout) == 0)
            and (len(self.out) > 0)
        ):
            return self.__str__()
        return self.process.__repr__()

    @classmethod
    def from_command(
        cls,
        *command_args,
        _handlers=True,
        _host=None,
        _get_children=False,
        _defer=False,
        _quit=False,
        **command_kwargs
    ):
        viewer = object.__new__(cls)
        out, err, pid_records = [], [], None
        if _get_children is True:
            if _handlers is not True:
                raise ValueError(
                    "can't do managed child process id (_get_children=True) "
                    "with no stream handlers (_handlers=False)."
                )

        if _handlers is True:
            if _get_children is True:
                pid_records = []
                recording_ps_output = False

                def handle_out(message):
                    nonlocal recording_ps_output
                    if message.startswith("##PARENT PID"):
                        viewer.remote_pid = re.search(
                            r"PID (\d+)", message
                        ).group(1)
                    elif message.startswith("##BEGIN PROCESS DUMP##"):
                        recording_ps_output = True
                    elif message.startswith("##END PROCESS DUMP##"):
                        recording_ps_output = False
                    elif recording_ps_output is True:
                        pid_records.append(message)
                    else:
                        out.append(message)

                command_kwargs |= console_stream_handlers((handle_out,), (err,))
                command_args = list(command_args)
                connector = re.match(r".*(;|&|\|)+ *$", command_args[-1])
                if connector is not None:
                    command_args[-1] = command_args[-1][:connector.span(1)[0]]
                command_args[-1] += (
                    r' & (echo \#\#PARENT PID $$\#\# ; '
                    r'echo \#\#BEGIN PROCESS DUMP\#\# ; '
                    r'ps -f ; echo \#\#END PROCESS DUMP\#\#)'
                )
            else:
                command_kwargs |= console_stream_handlers((out,), (err,))
        if isinstance(command_args[0], sh.Command):
            command = command_args[0]
        else:
            command = sh.Command(command_args[0])
        if _quit is True:
            command_args[-1] += " & exit"
        viewer.__init__(
            partial(command, *command_args[1:], **command_kwargs), _host=_host
        )
        (
            viewer.out, viewer.err, viewer._pid_records, viewer._get_children
        ) = out, err, pid_records, _get_children
        if _defer is False:
            viewer.run()
        return viewer


def clean_process_records(
    records, block=True, block_threshold=1, poll_delay=0.05
):
    i_am_actively_blocking = True
    while i_am_actively_blocking is True:
        process_records = valfilter(lambda v: "process" in v.keys(), records)
        alive_count = 0
        for record in process_records.values():
            if not record["process"]._closed:
                if record["process"].is_alive():
                    alive_count += 1
                    continue
            if "result" not in record.keys():
                record["result"] = record["pipe"].recv()
                record["pipe"].close()
            if not record["process"]._closed:
                record["process"].close()
        i_am_actively_blocking = (alive_count >= block_threshold) and block
        if i_am_actively_blocking is True:
            time.sleep(poll_delay)


def run(cmd: str, shell="bash", _viewer=False, **kwargs):
    """
    run the literal text of cmd in the specified shell using `sh`.
    """
    if _viewer is False:
        return getattr(sh, shell)(sh.echo(cmd), **kwargs)
    out, err = [], []
    handlers = console_stream_handlers((out,), (err,))
    kwargs |= handlers
    viewer = Viewer(
        getattr(sh, shell)(sh.echo(cmd), **kwargs)
    )
    viewer.out, viewer.err = out, err
    return viewer


def split_sh_stream(cmd: sh.RunningCommand, which="stdout"):
    lines = getattr(cmd, which).decode().split("\n")
    return [line.strip() for line in lines]


def ps_to_records(ps_command):
    if isinstance(ps_command, (list, tuple)):
        lines = [line.strip() for line in ps_command]
    else:
        lines = split_sh_stream(ps_command)
    split = [
        tuple(filter(None, line.split(" ")))
        for line in lines
        if line != ""
    ]
    fields = tuple(map(str.lower, split[0]))
    return [
        {field: value for field, value in zip(fields, line)}
        for line in split[1:]
    ]


Processlike = Union[sh.RunningCommand, Viewer]
Commandlike = Union[sh.Command, str]
