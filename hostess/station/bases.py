"""base classes and helpers for Nodes, Stations, Sensors, and Actors."""
from __future__ import annotations

from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from inspect import getmembers_static
from itertools import chain
import os
import re
import socket
import threading
from types import MappingProxyType as MPt
from typing import Any, Callable, Mapping, Union, Optional

from dustgoggles.func import filtern

from hostess.station.proto_utils import enum
from hostess.station.talkie import TCPTalk
from hostess.utilities import filestamp


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
        "match": actors[name].match_params,
        "exec": actors[name].exec_params,
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
        config, actors, props = {"check": {}}, {}, []
        params = {"check": self.check_params}
        for cls in chain(self.actions, self.loggers):
            config, params, actors, props = associate_actor(
                cls, config, params, actors, props
            )
        self.config, self.params, self.actors = config, params, actors
        for prop in props:
            setattr(self, *prop)
        self.check = configured(self.check, self.config["check"])
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
        self.config = {"match": {}, "exec": {}}
        self.params = {"match": self.match_params, "exec": self.exec_params}
        self.match = configured(self.match, self.config["match"])
        self.execute = configured(self.execute, self.config["exec"])

    def match(self, event: Any, **_) -> bool:
        """
        match is expected to return True if an event matches and raise
        NoMatch if it does not.
        """
        raise NotImplementedError

    def execute(self, node: BaseNode, event: Any, **kwargs) -> Any:
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


class PropConsumer:
    def __init__(self):
        self.proprefs = {}

    def consume_property(self, obj, attr, newname=None):
        prop = filtern(lambda kv: kv[0] == attr, getmembers_static(type(obj)))[
            1
        ]
        if not isinstance(prop, property):
            raise TypeError(f"{attr} of {type(obj).__name__} not a property")
        newname = attr if newname is None else newname
        self.proprefs[newname] = (obj, attr)

    def __getattr__(self, attr):
        ref = self.proprefs.get(attr)
        if ref is None:
            raise AttributeError
        return getattr(ref[0], ref[1])

    def __setattr__(self, attr, value):
        if attr == "proprefs":
            return super().__setattr__(attr, value)
        if (ref := self.proprefs.get(attr)) is not None:
            return setattr(ref[0], ref[1], value)
        return super().__setattr__(attr, value)

    def __dir__(self):
        return super().__dir__() + list(self.proprefs.keys())


class BaseNode(Matcher, PropConsumer, ABC):
    def __init__(
        self,
        name: str,
        n_threads: int = 6,
        elements: tuple[Union[type[Sensor], type[Actor]]] = (),
        start: bool = False,
        can_receive=False,
        host: Optional[str] = None,
        port: Optional[int] = None,
        poll: float = 0.08,
        timeout: int = 10,
    ):
        super().__init__()
        self.host, self.port = host, port
        self.interface, self.params = [], {}
        self.name = name
        self.config = {}
        self._lock = threading.Lock()
        # TODO: do this better
        os.makedirs("logs", exist_ok=True)
        self.logfile = f"logs/{self.name}_{filestamp()}.csv"
        for element in elements:
            self.add_element(element)
        self.exec = ThreadPoolExecutor(n_threads)
        self.can_receive = can_receive
        self.poll, self.timeout, self.signals = poll, timeout, {}
        if start is True:
            self.exec.submit(self.start)

    def add_element(self, cls: Union[type[Actor], type[Sensor]]):
        name = inc_name(cls, self.config)
        element = cls()
        if issubclass(cls, Actor):
            self.actors[name] = element
        elif issubclass(cls, Sensor):
            if len(self.sensors) > self.n_threads - 2:
                raise EnvironmentError("Not enough threads to add sensor.")
            self.sensors[name] = element
        else:
            raise TypeError(f"{cls} is not a valid subelement for this class.")
        self.config[name], self.params[name] = element.config, element.params
        for prop in element.interface:
            self.consume_property(element, prop, f"{name}_{prop}")
        self.threads = {}

    def restart_server(self):
        if self.server is not None:
            self.server['kill']()
        if self.can_receive is False:
            if (self.host is not None) or (self.port is not None):
                raise TypeError(
                    "cannot provide host/port for non-receiving node."
                )
        elif (self.host is None) or (self.port is None):
            raise TypeError("must provide host and port for receiving node.")
        elif self.can_receive is True:
            self.server = TCPTalk(
                self.host,
                self.port,
                ackcheck=self.ackcheck,
                executor=self.exec
            )
            self.threads |= self.server.threads
            self.inbox = self.server.data
            for ix, sig in self.server.signals.items():
                self.signals[f"server_{ix}"] = sig
        else:
            self.server, self.server_events, self.inbox = None, None, None

    def start(self):
        if self.__started is True:
            raise EnvironmentError("Node already started.")
        self.restart_server()
        self.threads["main"] = self.exec.submit(self._start)
        self.__started = True

    def nodeid(self):
        return {
            "name": self.name,
            "pid": os.getpid(),
            "host": socket.gethostname(),
        }

    def busy(self):
        # or maybe explicitly check threads? do we want a free one?
        # idk
        if self.exec._work_queue.qsize() > 0:
            return True
        return False

    def _start(self):
        raise NotImplementedError

    def _is_locked(self):
        return self._lock.locked()

    def _set_locked(self, state: bool):
        if state is True:
            self._lock.acquire(blocking=False)
        elif state is False:
            self._lock.release()
        else:
            raise TypeError

    locked = property(_is_locked, _set_locked)
    __started = False
    threads = None
    server = None
    server_events = None
    inbox = None
