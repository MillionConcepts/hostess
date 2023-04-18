from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
from abc import ABC
from functools import wraps
from itertools import chain
from pathlib import Path
from types import MappingProxyType as MPt
from typing import Any, Mapping, Union, Callable

from cytoolz import valmap
from dustgoggles.func import gmap
from dustgoggles.structures import unnest
from google.protobuf.message import Message

import hostess.station.nodes as nodes
from hostess.station.handlers import make_function_call, actiondict
from hostess.station.proto_utils import m2d


def inc_name(cls, config):
    name = cls.name
    if name in config:
        matches = filter(lambda k: re.match(name, k), config)
        name = f"{name}_{len(tuple(matches)) + 1}"
    return name




class DoNotUnderstand(ValueError):
    """the node does not know how to interpret this instruction."""


class NoActorForEvent(DoNotUnderstand):
    """no actor matches this instruction."""


class NoTaskError(DoNotUnderstand):
    """control node issued a 'do' instruction with no attached task."""


class NoInstructionType(DoNotUnderstand):
    """control node issued an instruction with no type."""


class NoMatch(Exception):
    """actor does not match instruction. used for control flow."""


def validate_instruction(instruction):
    """first-pass instruction validation"""
    if not instruction.HasField("type"):
        raise NoInstructionType
    if (instruction.type == "do") and not instruction.HasField("task"):
        raise NoTaskError


def configured(func, config):
    @wraps(func)
    def with_configuration(*args, **kwargs):
        return func(*args, **kwargs, **config)

    return with_configuration


class Actor(ABC):
    """base class for conditional responses to events."""
    def __init__(self):
        self.config = {'match': {}, 'exec': {}}
        self.params = {'match': self.match_params, 'exec': self.exec_params}
        self.match = configured(self.execute, self.config['match'])
        self.execute = configured(self.execute, self.config['exec'])

    def match(self, event: Any, **_) -> bool:
        """
        match is expected to return True if an event matches and raise
        NoMatch if it does not.
        """
        raise NotImplementedError

    def execute(self, node: "nodes.Node", event: Any, **kwargs) -> Any:
        raise NotImplementedError

    name: str

    config: Mapping
    match_params = ()
    exec_params = ()
    interface = ()
    actortype: str


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
    actor for do instructions that directly call a python function within the
    node's execution environment.
    """
    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("command") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    def execute(
            self, node: "nodes.Node", instruction: Message, key=None, noid=False, **_
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
            report['instruction_id'] = key
        call()
        if len(caches['result']) != 0:
            caches['result'] = caches['result'][0]
        else:
            caches['result'] = None
        if isinstance(caches['result'], Exception):
            report['status'] = 'crash'
        else:
            report['status'] = 'success'
        # in some cases could check stderr but would have to be careful
        # due to the many processes that communicate on stderr on purpose
        report['end'] = dt.datetime.utcnow()
        report['duration'] = report['end'] - report['start']

    name = "functioncall"

    actortype = "action"


NODE_ACTION_FIELDS = frozenset({"id", "start", "stop", "status", "result"})


# required keys for an in-memory action entry in node.actions
def associate_actor(cls, config, params, actors, props):
    name = inc_name(cls, config)
    actors[name] = cls()
    config[name] = actors[name].config
    params[name] = {
        'match': actors[name].match_params,
        'exec': actors[name].exec_params
    }
    for prop in actors[name].interface:
        props.append((f"{name}_{prop}", getattr(actors[name], prop)))
    return config, params, actors, props


def flatten_into_json(event, maxsize: int = 64):
    # TODO: if this ends up being unperformant with huge messages, do something
    if isinstance(event, Message):
        event = m2d(event)
    # TODO: figure out how to not make this infinity json objects
    return json.dumps(gmap(lambda v: v[:maxsize], valmap(str, unnest(event))))


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

    def execute(self, node: "nodes.Node", event: Union[dict, Message], **_) -> Any:
        node.log_event(flatten_into_json(event, maxsize=64))

    name = "base_log_actor"
    actortype = "log"


def increment_suffix(name, keys):
    matches = filter(lambda k: re.match(name, k), keys)
    numbers = []
    for m in matches:
        if not (number := re.search(r"_(\d+)", m)):
            continue
        numbers.append(number.group(1))
    if len(numbers) == 0:
        return 0
    return max(numbers)


class Matcher(ABC):
    """base class for things that can match sequences of actors."""

    def match(self, event: Any, category=None, **kwargs) -> list[Actor]:
        matching_actors = []
        actors = self.matching_actors(category)
        for actor in actors:
            try:
                actor.match(event, **kwargs)
                matching_actors.append(actor)
            except (NoMatch, AttributeError, KeyError, ValueError):
                continue
            return matching_actors
        raise NoActorForEvent

    # TODO: maybe redundant
    def explain_match(self, event: Any, category=None, **kwargs) -> dict[str]:
        reasons = {}
        actors = self.matching_actors(category)
        for actor in actors:
            try:
                reasons[actor.name] = actor.match(event, **kwargs)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                reasons[actor.name] = f"{type(err)}: {err}"
        return reasons

    def matching_actors(self, actortype):
        if actortype is None:
            return self.actors
        return [r for r in self.actors.values() if r.actortype == actortype]

    actors: dict[str, Actor]


def tail_file(position, *, path: Path, **_):
    if path is None:
        return position, []
    if os.stat(path).st_size == position:
        return position, []
    with path.open() as stream:
        stream.seek(position)
        lines = stream.readlines()
        position = stream.tell()
        return position, lines


class LineLogger(Actor):
    """logs all lines passed to it, separately from node's main logging."""

    def match(self, line, **_):
        if isinstance(line, str):
            return True
        raise NoMatch("not a string.")

    def execute(self, node: "nodes.Node", line: str, *, path=None, **_):
        if path is None:
            raise IOError("can't log this line without a path to log to.")
        with path.open("a") as stream:
            stream.write(f"{line}\n")

    exec_params = ("path",)
    name = "linelog"
    actortype = "log"


class ReportStringMatch(Actor):
    def match(self, line, *, patterns=(), **_):
        if not isinstance(line, str):
            raise NoMatch("is not a string")
        for pattern in patterns:
            if re.search(pattern, line):
                return True
        raise NoMatch("does not match patterns")

    def execute(self, node: "nodes.Node", line: str, *, path=None, **_):
        node.actionable_events.append({'path': str(path), 'content': line})

    name = "grepreport"
    match_params = ('patterns',)
    exec_params = ('path',)
    actortype = "action"


class Sensor(Matcher, ABC):
    """bass class for watching an input source."""
    # TODO: config here has to be initialized, like in Definition.
    #  and perhaps should be propagated up in a flattened way
    def __init__(self):
        if "sources" in dir(self):
            raise TypeError("bad configuration: can't nest Sources.")
        config, actors, props = {'check': {}}, {}, []
        params = {'check': self.check_params}
        for cls in chain(self.actions, self.loggers):
            config, params, actors, props = associate_actor(
                cls, config, params, actors, props
            )
        self.config, self.params, self.actors= config, params, actors
        for prop in props:
            setattr(self, *prop)
        self.check = configured(self.check, self.config['check'])
        self.memory = None

    def check(self, node, **kwargs):
        self.memory, events = self.checker(self.memory, **kwargs)
        for event in events:
            try:
                actors = self.match(event)
                for actor in actors:
                    actor.execute(self, node, **kwargs)
            except NoActorForEvent:
                continue

    base_config = MPt({})
    checker: Callable
    actions: tuple[type[Actor]] = ()
    loggers: tuple[type[Actor]] = ()
    check_params = ()
    name: str
    interface = ()


class WatchFile(Sensor):

    def _set_target(self, path):
        self._watched = path
        self.config['check']['path'] = path
        self.config['matchreport']['exec'] = path

    def _get_target(self):
        return self._watched

    def _get_logfile(self):
        return self._logfile

    def _set_logfile(self, path):
        self._logfile = path
        self.config['linelog']['exec']['path'] = path

    def _get_patterns(self):
        return self._patterns

    def _set_patterns(self, patterns):
        self._patterns = patterns
        self.config['grepreport']['match']['patterns'] = patterns

    actions = (ReportStringMatch,)
    loggers = (LineLogger,)
    checker = tail_file
    name = "filewatch"
    check_params = ("path",)
    target = property(_get_target, _set_target)
    logfile = property(_get_logfile, _set_logfile)
    patterns = property(_get_patterns, _set_patterns)
    _watched = None
    _logfile = None
    _patterns = ()
    interface = ("logfile", "target", "patterns")
