from __future__ import annotations

import time
from abc import ABC
import datetime as dt
from types import MappingProxyType as MPt
from typing import Any, Mapping, Union

from cytoolz import valmap, valfilter
from dustgoggles.structures import unnest
from google.protobuf.message import Message

from hostess.station.handlers import make_function_call, actiondict
from hostess.station.proto_utils import m2d


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


class Rule(ABC):
    """base class for conditional responses to events."""

    def match(self, event: Any) -> bool:
        """
        match is expected to return True if an event matches and raise
        NoMatch if it does not.
        """
        raise NotImplementedError

    def execute(self, node: "Node", event: Any, **kwargs) -> Any:
        raise NotImplementedError

    name: str


class PipeRulePlaceholder(Rule):
    """
    pipeline execution rule. resubmits individual steps of pipelines as
    Instructions to the calling node. we haven't implemented this, so it
    doesn't do anything.
    """

    def match(self, instruction: Any):
        if instruction.WhichOneof("task") == "pipe":
            return True
        raise NoMatch("not a pipeline instruction")

    name = "pipeline"


class FunctionCall(Rule):
    """
    rule for do instructions that directly call a python function within the
    node's execution environment.
    """

    def match(self, instruction: Any) -> bool:
        if instruction.action.WhichOneof("command") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    def execute(self, node: "Node", instruction: Message, **_) -> Any:
        if instruction.HasField("action"):
            action = instruction.action
        else:
            action = instruction
        caches, call = make_function_call(action.functioncall)
        node.actions[action.id] = actiondict(action) | caches
        call()
        if len(caches['result']) != 0:
            caches['result'] = caches['result'][0]
        else:
            caches['result'] = None
        if isinstance(caches['result'], Exception):
            node.actions[action.id]['status'] = 'crash'
        else:
            node.actions[action.id]['status'] = 'success'
        # in some cases could check stderr but would have to be careful
        # due to the many processes that communicate on stderr on purpose
        node.actions[action.id]['end'] = dt.datetime.utcnow()

    name = "functioncall"


# required keys for an in-memory action entry in node.actions
NODE_ACTION_FIELDS = frozenset({"id", "start", "stop", "status", "result"})


def flatten_for_csv(event, maxsize: int = 64):
    # TODO: if this ends up being unperformant with huge messages, do something
    if isinstance(event, Message):
        event = m2d(event)
    return valfilter(lambda v: len(v) < maxsize, valmap(str, unnest(event)))


class BaseLogRule(Rule):
    """
    default logging rule: log completed actions and incoming/outgoing messages
    """
    def match(self, event) -> bool:
        if isinstance(event, Message):
            return True
        elif isinstance(event, dict):
            if NODE_ACTION_FIELDS.issubset(event.keys()):
                return True
        raise NoMatch("Not a running/completed action or a received Message")

    def execute(self, node: "Node", event: Union[dict, Message], **_) -> Any:
        node.log(flatten_for_csv(event, maxsize=64))

    name = "base_log_rule"


class Ruleset(ABC):
    rules: Mapping[str, tuple[Rule]]

    def __getitem__(self, item):
        return self.rules[item]

    def match(self, instruction: Any) -> Rule:
        for rule in self.rules[instruction.type]:
            try:
                rule.match(instruction)
                return rule
            except (NoMatch, AttributeError, KeyError, ValueError):
                continue
        raise NoRuleForEvent

    def explain_match(self, instruction: Any) -> dict[str]:
        reasons = {}
        for rule in self.rules[instruction.type]:
            try:
                reasons[rule.name] = rule.match(instruction)
            except (NoMatch, AttributeError, KeyError, ValueError) as err:
                reasons[rule.name] = f"{type(err)}: {err}"
        return reasons

    name: str


class DefaultRules(Ruleset):
    rules = MPt(
        {'do': [FunctionCall]}
    )

    name = "default ruleset"
