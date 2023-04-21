"""
implementations of Actor and Sensor base classes, along with some helper
functions.
"""

from __future__ import annotations

import datetime as dt
import random
import re
from pathlib import Path
from typing import Any, Union, Callable, Optional

from google.protobuf.message import Message

import hostess.station.nodes as nodes
import hostess.station.proto.station_pb2 as pro
from hostess.station.bases import Sensor, Actor, NoMatch
from hostess.station.handlers import (
    make_function_call,
    actiondict,
    tail_file,
    watch_dir,
    flatten_into_json,
)


# keys a dict must have to count as a valid "actiondict" for inclusion in
# a Node's actions list
NODE_ACTION_FIELDS = frozenset({"id", "start", "stop", "status", "result"})


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


class FunctionCall(Actor):
    """
    Actor to execute Instructions that ask a Node to call a Python
    function from the node's execution environment. see
    handlers.make_function_call for serious implementation details.
    """

    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("command") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    def execute(
        self,
        node: "nodes.Node",
        instruction: Message,
        key=None,
        noid=False,
        **_,
    ) -> Any:
        if instruction.HasField("action"):
            action = instruction.action
        else:
            action = instruction
        caches, call = make_function_call(action.functioncall)
        if key is None:
            key = random.randint(0, int(1e7))
        node.actions[key] = actiondict(action) | caches
        report = node.actions[key]  # just shorthand
        if noid is False:
            report["instruction_id"] = key
        call()
        if len(caches["result"]) != 0:
            caches["result"] = caches["result"][0]
        else:
            caches["result"] = None
        if isinstance(caches["result"], Exception):
            report["status"] = "crash"
        else:
            report["status"] = "success"
        # in some cases could check stderr but would have to be careful
        # due to the many processes that communicate on stderr on purpose
        report["end"] = dt.datetime.utcnow()
        report["duration"] = report["end"] - report["start"]

    name = "functioncall"
    actortype = "action"


class BaseNodeLog(Actor):
    """
    default logging actor: log completed actions and incoming/outgoing messages
    """

    def match(self, event, **_) -> bool:
        if isinstance(event, Message):
            return True
        elif isinstance(event, dict):
            if NODE_ACTION_FIELDS.issubset(event.keys()):
                return True
        raise NoMatch("Not a running/completed action or a received Message")

    def execute(
        self, node: "nodes.Node", event: Union[dict, Message], **_
    ) -> Any:
        node._log_event(flatten_into_json(event, maxsize=64))

    name = "base_log_actor"
    actortype = "log"


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

    def execute(self, node: "nodes.Node", line: str, *, path=None, **_):
        node.actionable_events.append({"path": str(path), "content": line})

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
        station: "nodes.Station",
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
        station.outbox[node_picker(note)].append(instruction_maker(note))

    name: str
    actortype = "info"


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
