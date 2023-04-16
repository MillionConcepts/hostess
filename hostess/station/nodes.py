from __future__ import annotations
from typing import Literal

from dustgoggles.func import filtern
from google.protobuf.message import Message

from hostess.station.rules import (
    NoRuleForInstruction,
    NoTaskError,
    NoInstructionType,
    InstructionRuleset,
    DefaultRules,
)

NodeType: Literal["handler", "listener"]


# noinspection PyTypeChecker
class Node:
    def __init__(self, station: "Station", nodetype: NodeType):
        self.rules = self.rule_class()
        self.nodetype = NodeType
        self.station = station

    def interpret_instruction(self, instruction: Message):
        self._validate_instruction(instruction)
        try:
            rule = filtern(
                lambda r: r.match(instruction), self.rules[instruction.type]
            )
        except StopIteration:
            raise NoRuleForInstruction
        return rule.execute(instruction)

    @staticmethod
    def _validate_instruction(instruction):
        """first-pass instruction validation"""
        if not instruction.HasField("type"):
            raise NoInstructionType
        if (instruction.type == "do") and not instruction.HasField("task"):
            raise NoTaskError

    rule_class: InstructionRuleset = DefaultRules
