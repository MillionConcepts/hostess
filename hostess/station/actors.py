"""
implementations of Actor and Sensor base classes, along with some helper
functions.
"""

from __future__ import annotations

import datetime as dt
import random
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Optional, Hashable, MutableMapping

from google.protobuf.message import Message

import hostess.station.nodes as nodes
from hostess.station.station import Station
import hostess.station.proto.station_pb2 as pro
from hostess.station.bases import Sensor, Actor, NoMatch, BaseNode
from hostess.station.handlers import (
    make_function_call,
    actiondict,
    tail_file,
    watch_dir,
)
from hostess.station.messages import unpack_obj
from hostess.subutils import RunCommand

# keys a dict must have to count as a valid "actiondict" for inclusion in
# a Node's actions list
NODE_ACTION_FIELDS = frozenset({"id", "start", "stop", "status", "result"})


def init_execution(
    node: BaseNode,
    instruction: Message,
    key: Hashable,
    noid: bool
):
    """'invariant' start of a 'do' execution"""
    if instruction.HasField("action"):
        action = instruction.action  # for brevity in execute() methods
    else:
        action = instruction  # pure description cases
    if key is None:
        key = random.randint(0, int(1e7))
    node.actions[key] = actiondict(action)
    if noid is False:
        node.actions[key]["instruction_id"] = key
    return action, node.actions[key], key


def conclude_execution(
    result: Any,
    status: Optional[str] = None,
    report: Optional[MutableMapping] = None
):
    """
    common cleanup steps for a 'do' execution. individual Actors, even if they
    use the @reported decorator that includes this function, may also define
    additional cleanup steps.
    """
    report = {} if report is None else report
    if isinstance(result, Exception):
        # Actors that run commands in subprocesses may insert their own
        # 'streaming' results, which we do not want to overwrite with the
        # exception.
        report["status"] = "crash"
        report['exception'] = result
    else:
        # individual actors may have unique failure criteria
        report["status"] = "success" if status is None else status
        report['result'] = result
    # in some cases could check stderr but would have to be careful
    # due to the many processes that communicate on stderr on purpose
    report["end"] = dt.datetime.utcnow()
    report["duration"] = report["end"] - report["start"]


def reported(executor: Callable) -> Callable:
    """
    decorator for bound execute() methods of Actors that will handle
    Instructions from a Station
    """
    def with_reportage(
        self,
        node: BaseNode,
        instruction: Message,
        key=None,
        noid=False,
        **kwargs
    ):
        action, report, key = init_execution(node, instruction, key, noid)
        results = executor(self, node, action, key, **kwargs)
        conclude_execution(*results, report=report)

    return with_reportage


class PipeActorPlaceholder(Actor):
    """
    pipeline execution actor. resubmits individual steps of pipelines as
    Instructions to the calling node. we haven't implemented this, so it
    doesn't do anything.
    """

    def match(self, instruction: Any, **_):
        if instruction.WhichOneof("task") == "pipe":
            return True
        raise NoMatch("not a pipeline instruction")

    name = "pipeline"


class FileWriter(Actor):
    """basic actor to write to a file."""
    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.name != "filewrite":
            raise NoMatch("not a file write instruction")
        if instruction.action.WhichOneof("call") != "localcall":
            raise NoMatch("Not a properly-formatted local call")
        return True

    @reported
    def execute(
        self,
        node: "nodes.Node",
        instruction: Message,
        key=None,
        noid=False,
        **_,
    ):
        with open(self.file, self.mode) as stream:
            stream.write(unpack_obj(instruction.action.localcall))

    def _get_mode(self) -> str:
        return self._mode

    def _set_mode(self, mode: str):
        self._mode = mode

    def _get_file(self) -> Path:
        return self._file

    def _set_file(self, path: Path):
        self._file = path

    _file = None
    _mode = "a"
    file = property(_get_file, _set_file)
    mode = property(_get_mode, _set_mode)
    interface = ("file", "mode")
    actortype = "action"
    name = "filewrite"


class FuncCaller(Actor):
    """
    Actor to execute Instructions that ask a Node to call a Python
    function from the node's execution environment. see
    handlers.make_function_call for serious implementation details.
    """

    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("call") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    @reported
    def execute(
        self,
        node: "nodes.Node",
        action: Message,
        key=None,
        noid=False,
        **_,
    ) -> Any:
        caches, call = make_function_call(action.functioncall)
        node.actions[key] |= caches
        call()
        if len(node.actions[key]['result']) != 0:
            return node.actions[key]["result"]
        else:
            return None

    name = "funccaller"
    actortype = "action"


class SysCaller(Actor):
    """
    Actor to execute Instructions that ask a Node to run a command
    in an OS-level interpreter.
    """

    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("call") != "systemcall":
            raise NoMatch("not a system call instruction")
        return True

    @reported
    def execute(
        self,
        node: "nodes.Node",
        action: Message,
        key=None,
        noid=False,
        **_,
    ) -> tuple[dict[str, list[str]], str]:
        # TODO:
        #  - handle environment variables. workaround is of course to just
        #    set them in the command
        #  - switch interpreter on command
        #  - handle compression
        #  - handle fork requests (in 'context' field of SystemCall message)
        kwargs = {}
        # feed a binary blob to process stdin if present
        if action.systemcall.payload is not None:
            kwargs['_in_stream'] = BytesIO(action.systemcall.payload)
        viewer = RunCommand(action.systemcall.command, _viewer=True)()
        # slightly different convention than FunctionCaller because all we have
        # is out and err
        node.actions[key] |= {'result': {'out': viewer.out, 'err': viewer.err}}
        viewer.wait()
        # we don't want to report the action as failed for having stuff in
        # stderr because of how many applications randomly print to stderr.
        # the requesting object will have to handle that, or a subclass
        # could call this method from super and postfilter the results.
        # even raising the exception based on exit code is maybe questionable!
        node.actions[key]['exit_code'] = viewer.returncode()
        status = 'crash' if viewer.returncode() != 0 else 'success'
        return {'out': viewer.out, 'err': viewer.err}, status

    name = "syscaller"
    actortype = "action"


class LineLogger(Actor):
    """
    logs all strings passed to it. intended to be attached to a Sensor as a
    supplement to a Node's primary logging.
    """

    def match(self, line, **_):
        if isinstance(line, str):
            return True
        raise NoMatch("not a string.")

    def execute(self, node: "nodes.Node", line: str, *, path=None, **_):
        if path is None:
            return
        with path.open("a") as stream:
            stream.write(f"{line}\n")

    name = "linelog"
    actortype = "log"


class ReportStringMatch(Actor):
    """
    checks whether a string matches any of a sequence of regex patterns.
    executing it inserts the string into the associated node's list of
    actionable events, optionally annotated with a path denoting the string's
    source.
    """

    def match(self, line, *, patterns=(), **_):
        if not isinstance(line, str):
            raise NoMatch("is not a string")
        for pattern in patterns:
            if re.search(pattern, line):
                return True
        raise NoMatch("does not match patterns")

    def execute(
        self,
        node: "nodes.Node",
        line: str,
        patterns=(),
        *,
        path=None,
        **_
    ):
        node.actionable_events.append(
            {
                "path": str(path),
                "content": line,
                "match": [p for p in patterns if re.search(p, line)]
            }
        )
    name = "grepreport"
    actortype = "action"


class InstructionFromInfo(Actor):
    """
    skeleton info Actor for Stations. check, based on configurable criteria,
    whether a piece of info received in an Update indicates that we should
    assign a task to some handler Node, and if it does, create an Instruction
    from that info based on an instruction-making function.
    """

    # note: fields doesn't do anything atm
    def match(
        self, note, *, fields=None, criteria: list[Callable] = None, **_
    ) -> bool:
        if criteria is None:
            raise NoMatch("no criteria to match against")
        for criterion in criteria:
            if criterion(note):
                return True
        raise NoMatch("note did not match criteria")

    def execute(
        self,
        station: "Station",
        note,
        *,
        instruction_maker: Optional[Callable[[Any], pro.Instruction]] = None,
        node_picker: Optional[Callable[[Any], str]] = None,
        **_,
    ):
        if instruction_maker is None:
            raise TypeError("Must have an instruction maker.")
        if node_picker is None:
            node_picker = station.next_handler
        station.outboxes[node_picker(note)].append(instruction_maker(note))

    def _set_criteria(self, criteria):
        self.config['match']["criteria"] = criteria

    def _get_criteria(self):
        return self.config['match']['criteria']

    def _set_instruction_maker(self, instruction_maker):
        self.config['exec']['instruction_maker'] = instruction_maker

    def _get_instruction_maker(self):
        return self.config['exec']['instruction_maker']

    name: str
    actortype = "info"
    interface = ("instruction_maker", "criteria")
    instruction_maker = property(
        _get_instruction_maker, _set_instruction_maker
    )
    criteria = property(_get_criteria, _set_criteria)


class FileSystemWatch(Sensor):
    """
    simple Sensor for watching contents of a filesystem. offers an
    interface for changing target path and regex match patterns. base class
    tails a file. see DirWatch below for an inheritor that diffs a directory.
    """

    def __init__(self, checker=tail_file):
        self.checker = checker
        super().__init__()

    def _set_target(self, path):
        self._watched = Path(path)
        self.config["check"]["path"] = Path(path)
        self.config["grepreport"]["exec"]["path"] = Path(path)

    def _get_target(self):
        return self._watched

    def _get_logfile(self):
        return self._logfile

    def _set_logfile(self, path):
        self._logfile = path
        self.config["linelog"]["exec"]["path"] = Path(path)

    def _get_patterns(self):
        return self._patterns

    def _set_patterns(self, patterns):
        self._patterns = patterns
        self.config["grepreport"]["match"]["patterns"] = patterns
        self.config["grepreport"]["exec"]["patterns"] = patterns

    actions = (ReportStringMatch,)
    loggers = (LineLogger,)
    name = "filewatch"
    target = property(_get_target, _set_target)
    logfile = property(_get_logfile, _set_logfile)
    patterns = property(_get_patterns, _set_patterns)
    _watched = None
    _logfile = None
    _patterns = ()
    interface = ("logfile", "target", "patterns")


class DirWatch(FileSystemWatch):
    def __init__(self):
        super().__init__(checker=watch_dir)

    name = "dirwatch"
