from __future__ import annotations

import os
from abc import ABC
import datetime as dt
from functools import wraps
from itertools import chain
import json
import random
import re
from pathlib import Path
from types import MappingProxyType as MPt
from typing import Any, Mapping, Union, Optional, Iterable, Callable

from cytoolz import valmap
from dustgoggles.func import gmap
from dustgoggles.structures import unnest
from google.protobuf.message import Message

from hostess.station.proto_utils import m2d
from hostess.station.handlers import make_function_call, actiondict


class DoNotUnderstand(ValueError):
    """the node does not know how to interpret this instruction."""


class NoRuleForEvent(DoNotUnderstand):
    """no rule matches this instruction."""


class NoTaskError(DoNotUnderstand):
    """control node issued a 'do' instruction with no attached task."""


class NoInstructionType(DoNotUnderstand):
    """control node issued an instruction with no type."""


class NoMatch(Exception):
    """rule does not match instruction. used for control flow."""


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

    return with_configuration()


class Rule(ABC):
    """base class for conditional responses to events."""

    def __init__(self, config=None):
        config = config if config is not None else {}
        self.config = self.base_config | config
        self.execute = configured(self.execute, self.config)

    def match(self, event: Any, **match_kwargs) -> bool:
        """
        match is expected to return True if an event matches and raise
        NoMatch if it does not.
        """
        raise NotImplementedError

    def execute(self, node: "Node", event: Any, **kwargs) -> Any:
        raise NotImplementedError

    name: str
    base_config: Mapping = MPt({})


class PipeRulePlaceholder(Rule):
    """
    pipeline execution rule. resubmits individual steps of pipelines as
    Instructions to the calling node. we haven't implemented this, so it
    doesn't do anything.
    """

    def match(self, instruction: Any, **_):
        if instruction.WhichOneof("task") == "pipe":
            return True
        raise NoMatch("not a pipeline instruction")

    name = "pipeline"


class FunctionCall(Rule):
    """
    rule for do instructions that directly call a python function within the
    node's execution environment.
    """

    def match(self, instruction: Any, **_) -> bool:
        if instruction.action.WhichOneof("command") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    def execute(
        self, node: "Node", instruction: Message, key=None, noid=False, **_
    ) -> Any:
        if instruction.HasField("action"):
            action = instruction.action
        else:
            action = instruction
        caches, call = make_function_call(action.functioncall)
        if key is None:
            key = random.randint(0, int(1e7))
        (report := node.actions[key]) = actiondict(action) | caches
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


NODE_ACTION_FIELDS = frozenset({"id", "start", "stop", "status", "result"})


# required keys for an in-memory action entry in node.actions
def csv_sanitize(thing):
    """overly simplistic csv sanitizer"""
    return re.sub(r"[\n,]", "_", str(thing)).strip()


# TODO: less simplistic
def flatten_into_json(event, maxsize: int = 64):
    # TODO: if this ends up being unperformant with huge messages, do something
    if isinstance(event, Message):
        event = m2d(event)
    # TODO: figure out how to not make this infinity json objects
    return json.dumps(gmap(lambda v: v[:maxsize], valmap(str, unnest(event))))


class BaseLogRule(Rule):
    """
    default logging rule: log completed actions and incoming/outgoing messages
    """
    def match(self, event, **_) -> bool:
        if isinstance(event, Message):
            return True
        elif isinstance(event, dict):
            if NODE_ACTION_FIELDS.issubset(event.keys()):
                return True
        raise NoMatch("Not a running/completed action or a received Message")

    def execute(self, node: "Node", event: Union[dict, Message], **_) -> Any:
        node.log_event(flatten_into_json(event, maxsize=64))

    name = "base_log_rule"


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
    """base class for things that can match sequences of rules."""

    def match(
        self, instruction: Any, category=None, **match_kwargs
    ) -> list[Rule]:
        matching_rules = []
        if category is None:
            rules = tuple(chain(self.actions, self.loggers, self.sources))
        else:
            rules = {
                'log': self.loggers,
                'action': self.actions,
                'source': self.sources
            }
        for rule in rules:
            try:
                rule.match(instruction, **match_kwargs)
                matching_rules.append(rule)
            except (NoMatch, AttributeError, KeyError, ValueError):
                continue
            return matching_rules
        raise NoRuleForEvent

    def explain_match(self, instruction: Any) -> dict[str]:
        reasons = {}
        for rule in self.rules[instruction.type]:
            try:
                reasons[rule.name] = rule.match(instruction)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                reasons[rule.name] = f"{type(err)}: {err}"
        return reasons

    actions: tuple[Rule]
    loggers: tuple[Rule]
    sources: tuple[Rule]


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


class LineLogger(Rule):

    def match(self, line, **_):
        if isinstance(line, str):
            return True
        raise NoMatch("not a string.")

    def execute(self, node: "Node", line: str, *, path=None, **_):
        if path is None:
            raise IOError("can't log this line without a path to log to.")
        with path.open("a") as stream:
            stream.write(f"{line}\n")

    name = "log_all_lines"


class ReportStringMatch(Rule):
    def match(self, line, *, patterns=(), **_):
        if not isinstance(line, str):
            raise NoMatch("is not a string")
        for pattern in patterns:
            if re.search(pattern, line):
                return True
        raise NoMatch("does not match patterns")

    def execute(self, node: "Node", line: str, *, path=None, **_):
        node.actionable_events.append({'path': str(path), 'content': line})

    name = "report_string_match"


class Source(ABC, Matcher):
    """bass class for watching an input source."""

    def __init__(self, config=None):
        config = config if config is not None else {}
        self.config = self.base_config | config
        self.check = configured(self.check, self.config)
        self.match = configured(self.match, self.config)
        self.memory = None

    def check(self, node, **kwargs):
        self.memory, events = self.checker(self.memory, **kwargs)
        for event in events:
            try:
                rules = self.match(event)
                for rule in rules:
                    rule.execute(self, node, **kwargs)
            except NoRuleForEvent:
                continue

    checker: Callable
    rules: tuple[Rule]


class WatchFile(Source):
    rules = (LineLogger, ReportStringMatch)
    checker = tail_file
    name = "watch_file"


class Definition(Matcher, ABC):

    def __init__(self):
        self.config = {}
        # TODO: too complicated?
        for obj in chain(self.actions, self.sources, self.loggers):
            name = obj.name
            nameconfig = self.base_config.get(name, {})
            if name in self.config:
                matches = filter(lambda k: re.match(name, k), self.config)
                name = f"{name}_{len(tuple(matches))}"
            self.config[name] = nameconfig
        self.config = dict(self.base_config)

    base_config: Optional[Mapping]
    actions: tuple[Rule]
    sources: tuple[Source]
    loggers: tuple[Rule]
    name: str


class DefaultRules(Definition):
    rules = (FunctionCall,)

    name = "default ruleset"


class FileWatcher(Definition):
    sources = (WatchFile,)
    name = "file watcher"


