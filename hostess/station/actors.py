"""
concrete implementations of Actor and Sensor base classes, along with some
helper functions.
"""
from __future__ import annotations

import datetime as dt
from io import BytesIO
from pathlib import Path
import random
import re
from typing import Any, Callable, Hashable, MutableMapping, Optional, Sequence

from dustgoggles.structures import listify
from google.protobuf.message import Message

from hostess.station.bases import (
    Actor, DispatchActor, Node, NoMatch, Sensor
)
import hostess.station.delegates as delegates
from hostess.station.handlers import (
    make_actiondict,
    make_function_call,
    tail_file,
    watch_dir,
)
from hostess.station.messages import unpack_obj
import hostess.station.proto.station_pb2 as pro
from hostess.station.station import Station
from hostess.subutils import RunCommand


NODE_ACTION_FIELDS = frozenset({"id", "start", "stop", "status", "result"})
"""
keys a dict must have to count as a valid "actiondict" in a Node's actions list
"""


def init_execution(
    node: Node,
    instruction: Message,
    key: Optional[Hashable],
    noid: bool
) -> tuple[pro.Action, dict, Hashable]:
    """
    perform setup for a "regular" 'do'-type Instruction. Intended primarily as
    a component function of the `reported()` decorator.

    Args:
        node: Actor's parent Node (usually a Delegate)
        instruction: 'do'-type Instruction
        key: name of / identifier for Instruction's Action. if not specified,
            generates a random integer.
        noid: don't insert the Instruction's id into the generated actiondict.

    Returns:
        action: Action from action field of Instruction, or full Instruction
            if it contains only a description of the action
        actiondict: data/metadata dict for the action that this function
            generated function and inserted into the parent Node's `actions`
        key: identifier for action
    """
    if instruction.HasField("action"):
        action = instruction.action  # for brevity in execute() methods
    else:
        action = instruction  # pure description cases
    if key is None:
        key = random.randint(0, int(1e7))
    node.actions[key] = make_actiondict(action)
    if noid is False:
        node.actions[key]["instruction_id"] = key
    return action, node.actions[key], key


def conclude_execution(
    result: Any,
    status: Optional[str] = None,
    actiondict: Optional[MutableMapping[str, Any]] = None
):
    """
    conclude a "regular" 'do'-type action, inserting relevant data into its
    associated actiondict. Intended primarily as a component function of the
    `reported()` decorator.

    Note that individual Actors, even if they use `reported()`, often define
    additional cleanup steps.

    Args:
        result: return value of, or Exception raised by, an Actor's execute()
            method.
        status: optional status code. if not specified, status will always be
            "success" unless `result` is an Exception. Primarily intended to
            allow Actors to describe gracefully-handled failures.
        actiondict: element of parent Node's `actions` attribute. if not
            specified, creates an empty dict -- in this case, this function
            is basically a no-op.
    """
    actiondict = {} if actiondict is None else actiondict
    if isinstance(result, Exception):
        # Actors that run commands in subprocesses may insert their own
        # 'streaming' results, which we do not want to overwrite with the
        # exception.
        actiondict["status"] = "crash"
        actiondict['exception'] = result
    else:
        # individual actors may have unique failure criteria
        actiondict["status"] = "success" if status is None else status
        actiondict['result'] = result
    # in some cases could check stderr but would have to be careful
    # due to the many processes that communicate on stderr on purpose
    actiondict["end"] = dt.datetime.utcnow()
    actiondict["duration"] = actiondict["end"] - actiondict["start"]


def reported(executor: Callable) -> Callable:
    """
    decorator for bound execute() methods of Actors that handle 'do'-type
    Instructions in a "normal" fashion. Provides standardized setup and
    conclusion behaviors.

    Args:
        executor: bound execute() method of associated Actor.

    Returns:
        version of execute() method with added setup and conclusion steps.
    """
    def with_reportage(
        self,
        node: Node,
        instruction: Message,
        key=None,
        noid=False,
        **kwargs
    ):
        action, report, key = init_execution(node, instruction, key, noid)
        try:
            results = executor(self, node, action, key, **kwargs)
        except Exception as ex:
            results = (ex,)
        conclude_execution(*listify(results), actiondict=report)

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
    """Simple Actor that writes to a file."""
    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.name != "filewrite":
            raise NoMatch("not a file write instruction")
        if instruction.action.WhichOneof("call") != "localcall":
            raise NoMatch("Not a properly-formatted local call")
        return True

    @reported
    def execute(
        self,
        node: "Node",
        action: Message,
        key=None,
        noid=False,
        **_,
    ):
        with open(self.file, self.mode) as stream:
            stream.write(unpack_obj(action.localcall))

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
    """file this Actor writes to"""
    mode = property(_get_mode, _set_mode)
    """mode this Actor writes in -- one of 'w', 'wb', 'a', or 'ab'."""
    interface = ("file", "mode")
    actortype = "action"
    name = "filewrite"


class FuncCaller(Actor):
    """
    Versatile Actor that handles Instructions to call Python functions.
    A Station typically makes these using `handlers.make_function_call()`.
    """

    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("call") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    @reported
    def execute(
        self,
        node: "Node",
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
    Versatile Actor that handles Instructions to run OS-level shell commands.
    """

    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("call") != "systemcall":
            raise NoMatch("not a system call instruction")
        return True

    @reported
    def execute(
        self,
        node: "Node",
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
    Simple Actor that logs all strings passed to it. intended to be attached
    to Sensors that need to generate their own logs rather than writing to
    their parent Delegate's primary log.
    """

    def match(self, line, **_):
        """match only strings"""
        if isinstance(line, str):
            return True
        raise NoMatch("not a string.")

    def execute(self, node: "delegates.Delegate", line: str, *, path=None, **_):
        if path is None:
            return
        with path.open("a") as stream:
            stream.write(f"{line}\n")

    name = "linelog"
    actortype = "log"


class ReportStringMatch(Actor):
    """
    Actor that checks whether a string matches any of a sequence of regex
    patterns. Intended to be used by Sensors that work by tailing a file or
    other data stream.

    This Actor's `execute()` inserts the string into the parent Delegate's
    list of actionable events, annotated with a list of all patterns that
    matched the string, and, optionally, with a string denoting the
    string's source. the Delegate will use this to construct an Info Message
    it will include in an Update to its Station.
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
        node: "delegates.Delegate",
        line: str,
        patterns=(),
        *,
        path=None,
        **_
    ):
        node.add_actionable_event(
            {
                "path": str(path),
                "content": line,
                "match": [p for p in patterns if re.search(p, line)]
            },
            self.owner
        )
    name = "grepreport"
    actortype = "action"


class InstructionFromInfo(DispatchActor):
    """
    skeleton Info-handling Actor for Stations. Checks, based on configurable
    criteria, whether an object unpacked from an Info message included in an
    Update indicates that the Station should assign a task to some handler
    Delegate, and, if it does, create an Instruction from that object based on
    an instruction-making function.

    Note that this Actor is basically abstract by default; its `criteria` and
    `instruction_maker` properties must be assigned to make it do anything.
    """

    def match(self, note, **_) -> bool:
        if self.criteria is None:
            raise NoMatch("no criteria to match against")
        for criterion in self.criteria:
            if criterion(note):
                return True
        raise NoMatch("note did not match criteria")

    def execute(self, node: "Station", note, **_,):
        if self.instruction_maker is None:
            raise TypeError("Must have an instruction maker.")
        delegatename = self.pick(node, note)
        node.queue_task(delegatename, self.instruction_maker(note))

    interface = ("instruction_maker", "criteria")
    name: str
    actortype = "info"
    instruction_maker: Optional[Callable[[Any], pro.Instruction]] = None
    """function that generates an Instruction from an object"""
    criteria: Optional[Sequence[Callable[[Any], bool]]] = None
    """
    predicate functions that define what objects this Actor can handle. if
    any of these functions return True when passed an object, the Actor 
    matches that object.
    """


class FileSystemWatch(Sensor):
    """
    simple Sensor for watching contents of a filesystem. offers an
    interface for changing target path and regex match patterns. this base
    class tails a file. see DirWatch for a subclass that diffs a directory.
    """

    def __init__(self, checker=tail_file):
        super().__init__()
        self.checker = checker

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
    """what file does this Sensor watch?"""
    logfile = property(_get_logfile, _set_logfile)
    """where does this Sensor log its matches?"""
    patterns = property(_get_patterns, _set_patterns)
    """what patterns does this Sensor look for in the file?"""
    _watched = None
    _logfile = None
    _patterns = ()
    interface = ("logfile", "target", "patterns")


class DirWatch(FileSystemWatch):
    """
    like FileSystemWatch, but its `target` property should be a folder, not a
    file, and its `patterns` property matches newly-appearing filenames.
    """
    def __init__(self):
        super().__init__(checker=watch_dir)

    name = "dirwatch"
