from __future__ import annotations

from abc import ABC
from typing import Any, Mapping

from google.protobuf.message import Message

from hostess.station.handlers import make_function_call, actiondict


class DoNotUnderstand(ValueError):
    """the node does not know how to interpret this instruction."""


class NoRuleForInstruction(DoNotUnderstand):
    """no rule matches this instruction."""


class NoTaskError(DoNotUnderstand):
    """control node issued a 'do' instruction with no attached task."""


class NoInstructionType(DoNotUnderstand):
    """control node issued an instruction with no type."""


class NoMatch(Exception):
    """rule does not match instruction. used for control flow."""


class InstructionRule(ABC):
    """
    class for defining conditional responses to control node instructions for
    handler/listener nodes.
    """

    def match(self, instruction: Message) -> bool:
        """
        match is expected to return True if an instruction matches and raise
        NoMatch if it does not.
        """
        raise NotImplementedError

    def execute(self, node: "Node", instruction: Message) -> Any:
        raise NotImplementedError

    name: str


class PipeRulePlaceholder(InstructionRule):
    """ "
    pipeline execution rule. resubmits individual steps of pipelines as
    Instructions to the calling node. we haven't implemented this, so it
    doesn't do anything.
    """

    def match(self, instruction: Message):
        if instruction.WhichOneof("task") == "pipe":
            return True
        raise NoMatch("not a pipeline instruction")

    name = "pipeline"


class FunctionCall(InstructionRule):
    """
    rule for do instructions that directly call a python function within the
    node's execution environment.
    """

    def match(self, instruction: Message):
        if instruction.action.WhichOneof("command") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    def execute(self, node: "Node", instruction: Message) -> Any:
        if instruction.HasField("action"):
            action = instruction.action
        else:
            action = instruction
        caches, call = make_function_call(action.functioncall)
        node.running_actions.append(actiondict(action) | caches)
        call()

    name = "functioncall"


class InstructionRuleset(ABC):
    rules: Mapping[str, tuple[InstructionRule]]

    def __getitem__(self, item):
        return self.rules[item]

    def match(self, instruction: Message):
        for rule in self.rules[instruction.type]:
            try:
                rule.match(instruction)
                return rule
            except (NoMatch, AttributeError, KeyError, ValueError):
                continue
        raise NoRuleForInstruction

    def explain_match(self, instruction: Message):
        reasons = {}
        for rule in self.rules[instruction.type]:
            try:
                reasons[rule.name] = rule.match(instruction)
            except (NoMatch, AttributeError, KeyError, ValueError) as err:
                reasons[rule.name] = f"{type(err)}: {err}"


class DefaultRules(InstructionRuleset):
    pass
