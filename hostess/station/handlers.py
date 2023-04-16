"""default action handlers"""
from __future__ import annotations
import datetime as dt
import json
import struct
import sys
from abc import ABC
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import Mapping, Any, Literal

import dill
from dustgoggles.func import intersection, filtern
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from hostess.subutils import make_watch_caches, defer, watched_process, \
    deferinto


def tbd(*_, **__):
    raise NotImplementedError


def get_module(module_name: str):
    if module_name in sys.modules:
        return sys.modules[module_name]
    if Path(module_name).stem in sys.modules:
        return sys.modules[module_name]
    try:
        return import_module(module_name)
    except ModuleNotFoundError:
        pass
    spec = spec_from_file_location(Path(module_name).stem, module_name)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[Path(module_name).stem] = module
    return module


def unpack_callargs(arguments):
    kwargs = {}
    for arg in arguments:
        if len(intersection({"name", "value"}, arg.keys())) != 2:
            raise ValueError("need both value and argument name")
        if arg.get("compression") is not None:
            raise NotImplementedError
        if arg.get("serialization") == "json":
            value = json.loads(arg["value"])
        elif arg.get("serialization") == "pickle":
            value = dill.loads(arg["value"])
        elif arg.get("scanf") is not None:
            value = struct.unpack(arg["scanf"], arg["value"])
        else:
            value = arg["value"]
        kwargs[arg["argname"]] = value
    return kwargs


def make_function_call(func=None, module=None, arguments=None, context=None):
    if func is None:
        raise TypeError("Can't actually do this without a function.")
    if module is not None:
        func = getattr(get_module(module), func)
    else:
        func = getattr("__builtins__", func)
    kwargs = unpack_callargs(arguments)
    if context in ("thread", "unknowncontext"):
        result = {"result": []}
        return deferinto(func, _target=result['result'], **kwargs)
    elif context in ("process", "detached"):
        fork, caches = context == "detached", make_watch_caches()
        call = defer(watched_process(func, caches=caches, fork=fork), **kwargs)
        return caches, call
    else:
        raise ValueError(f"unknown context {context}")


#
# def dispatch_task(task: Message):
#     func = {'action': dispatch_action, 'pipe': tbd}[task.WhichOneof("task")]
#     return func(task)


def actiondict(action: Message):
    """standardized dict for recording running action"""
    return {
        "name": action.name,
        "id": action.id,
        "start": dt.datetime.utcnow(),
        "stop": None
    }


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
        if instruction.action.WhichOneOf("command") != "functioncall":
            raise NoMatch("not a function call instruction")
        return True

    def execute(self, node: "Node", instruction: Message) -> Any:
        if instruction.hasfield("action"):
            action = instruction.action
        else:
            action = instruction
        caches, call = make_function_call(MessageToDict(**action.command))
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


NodeType: Literal["handler", "listener"]


# noinspection PyTypeChecker
class Node:
    def __init__(self, station: "Station", nodetype: NodeType):
        self.rules = self.rule_class()

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
