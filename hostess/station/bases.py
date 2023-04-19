from __future__ import annotations

import re
from abc import ABC
from functools import wraps
from itertools import chain
from types import MappingProxyType as MPt
from typing import Any, Callable, Mapping

from hostess.station import nodes as nodes
from hostess.station.proto_utils import enum


def configured(func, config):
    @wraps(func)
    def with_configuration(*args, **kwargs):
        return func(*args, **kwargs, **config)

    return with_configuration


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
            return list(self.actors.values())
        return [r for r in self.actors.values() if r.actortype == actortype]

    actors: dict[str, Actor]


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
        self.config, self.params, self.actors = config, params, actors
        for prop in props:
            setattr(self, *prop)
        self.check = configured(self.check, self.config['check'])
        self.memory = None

    def check(self, node, **check_kwargs):
        self.memory, events = self.checker(self.memory, **check_kwargs)
        for event in events:
            try:
                actors = self.match(event)
                for actor in actors:
                    # kwargs propagate to individual actors via `self.config`
                    actor.execute(node, event)
            except NoActorForEvent:
                continue

    base_config = MPt({})
    checker: Callable
    actions: tuple[type[Actor]] = ()
    loggers: tuple[type[Actor]] = ()
    check_params = ()
    name: str
    interface = ()


class Actor(ABC):
    """base class for conditional responses to events."""

    def __init__(self):
        self.config = {'match': {}, 'exec': {}}
        self.params = {'match': self.match_params, 'exec': self.exec_params}
        self.match = configured(self.match, self.config['match'])
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


def inc_name(cls, config):
    name = cls.name
    if name in config:
        matches = filter(lambda k: re.match(name, k), config)
        name = f"{name}_{len(tuple(matches)) + 1}"
    return name


def validate_instruction(instruction):
    """first-pass instruction validation"""
    if enum(instruction, "type") == "unknowninst":
        raise NoInstructionType
    if (instruction.type == "do") and not instruction.HasField("task"):
        raise NoTaskError
