"""base classes and helpers for Nodes, Stations, Sensors, and Actors."""
from __future__ import annotations

import inspect
import json
import os
import re
import socket
import threading
from abc import ABC
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from types import MappingProxyType as MPt
from typing import Any, Callable, Mapping, Union, Optional, Sequence

import yaml
from cytoolz import valmap
# noinspection PyProtectedMember
from google.protobuf.pyext._message import Message

from hostess.station.handlers import flatten_for_json, json_sanitize
from hostess.station.proto_utils import enum
from hostess.station.talkie import TCPTalk
from hostess.utilities import configured, logstamp, yprint


def associate_actor(
    cls, cdict, params, actors: Sequence[Actor], props, name=None
):
    """utility function for associating an actor with a Sensor."""
    name = inc_name(cls.name if name is None else name, cdict)
    actors[name] = cls()
    cdict[name] = actors[name].config
    params[name] = {
        "match": actors[name].params["match"],
        "exec": actors[name].params["exec"],
    }
    for prop in actors[name].interface:
        props.append((f"{name}_{prop}", getattr(actors[name], prop)))
    return cdict, params, actors, props


class AttrConsumer:
    """
    implements functionality for "consuming" the attributes of associated
    objects. designed to permit Nodes and similar objects to promote the
    interface properties of attached elements into their own interfaces as
    pseudo-attributes.
    """

    def __init__(self):
        self.attrefs = {}

    def consume_property(self, obj, attr, newname=None):
        """
        promote the `attr` attribute of `obj` into this object's interface.
        if `newname` is not None, assign it to the `newname` key of this
        object's attribute reference dict (`attrefs`); otherwise, use the
        original name.
        """
        newname = attr if newname is None else newname
        self.attrefs[newname] = (obj, attr)

    def __getattr__(self, attr):
        """
        if normal attribute lookup fails, attempt to refer it to a property
        of an associated object.
        """
        ref = self.attrefs.get(attr)
        if ref is None:
            raise AttributeError
        return getattr(ref[0], ref[1])

    def __setattr__(self, attr, value):
        """
        refer assignments to pseudo-attributes defined in self.proprefs to the
        properties of the underlying objects.
        """
        if attr == "attrefs":
            return super().__setattr__(attr, value)
        if (ref := self.attrefs.get(attr)) is not None:
            return setattr(ref[0], ref[1], value)
        return super().__setattr__(attr, value)

    def __dir__(self):
        """
        add the pseudo-attributes in attrefs to this object's directory so
        that they look real.
        """
        # noinspection PyUnresolvedReferences
        return super().__dir__() + list(self.attrefs.keys())

    def _get_interface(self):
        return [k for k in self.attrefs.keys()]

    def _set_interface(self, _):
        raise TypeError("interface does not support assignment")

    interface = property(_get_interface, _set_interface)


class Matcher(AttrConsumer, ABC):
    """base class for things that can match sequences of actors."""

    def match(self, event: Any, category=None, **kwargs) -> list[Actor]:
        matching_actors = []
        actors = self.filter_actors_by_category(category)
        for actor in actors:
            try:
                actor.match(event, **kwargs)
                matching_actors.append(actor)
            except (NoMatch, AttributeError, KeyError, ValueError, TypeError):
                continue
        if len(matching_actors) == 0:
            raise NoActorForEvent
        return matching_actors

    def explain_match(self, event: Any, category=None, **kwargs) -> dict[str]:
        reasons = {}
        actors = self.filter_actors_by_category(category)
        for actor in actors:
            try:
                reasons[actor.name] = actor.match(event, **kwargs)
            except KeyboardInterrupt:
                raise
            except Exception as err:
                reasons[actor.name] = f"{type(err)}: {err}"
        return reasons

    def filter_actors_by_category(self, actortype):
        if actortype is None:
            return list(self.actors.values())
        return [r for r in self.actors.values() if r.actortype == actortype]

    def add_element(self, cls: Union[type[Actor], type[Sensor]], name=None):
        """
        associate an Actor or Sensor with this object, consuming its
        interface properties and making it available for matching or sensor
        looping.
        """
        name = inc_name(cls.name if name is None else name, self.cdict)
        element = cls()
        element.name = name
        if issubclass(cls, Actor):
            self.actors[name] = element
        elif issubclass(cls, Sensor):
            self.sensors[name] = element
        else:
            raise TypeError(f"{cls} is not a valid subelement for this class.")
        self.cdict[name], self.params[name] = element.config, element.params
        for prop in element.interface:
            self.consume_property(element, prop, f"{name}_{prop}")

    actors: dict[str, Actor]
    params: dict[str, Any]
    sensors: dict[str, "Sensor"]


class Sensor(Matcher, ABC):
    """bass class for watching an input source."""

    def __init__(self):
        super().__init__()
        if "sources" in dir(self):
            raise TypeError("bad configuration: can't nest Sources.")
        props, actors = [], {}
        checkp = inspect.getfullargspec(self.check).kwonlyargs
        self.config = {"check": {k: None for k in checkp}}
        self.check_params = tuple(checkp)
        params = {"check": self.check_params}
        for cls in chain(self.actions, self.loggers):
            self.config, params, actors, props = associate_actor(
                cls, self.config, params, actors, props
            )
        self.params, self.actors = params, actors
        for prop in props:
            setattr(self, *prop)
        self.check = configured(self.check, self.config["check"])
        self.memory = None

    def add_element(self, cls: type[Actor], name=None):
        if issubclass(cls, Sensor):
            raise TypeError("cannot add Sensors to a Sensor.")
        super().add_element(cls, name)

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

    def __str__(self):
        pstring = f"{type(self).__name__} ({self.name})\n"
        pstring += f"interface:\n"
        for attr in self.interface:
            pstring += f"    {attr}: {getattr(self, attr)}\n"
        pstring += f"actors: {[a for a in self.actors]}\n"
        pstring += f"config: {yaml.dump(self.config).replace('null', 'None')}"
        return pstring

    def __repr__(self):
        return self.__str__()

    base_config = MPt({})
    checker: Callable
    actions: tuple[type[Actor]] = ()
    loggers: tuple[type[Actor]] = ()
    name: str
    interface = ()


class Actor(ABC):
    """base class for conditional responses to events."""

    def __init__(self):
        matchp = inspect.getfullargspec(self.match).kwonlyargs
        execp = inspect.getfullargspec(self.execute).kwonlyargs
        self.config = {
            "match": {k: None for k in matchp},
            "exec": {k: None for k in execp},
        }
        self.params = {"match": tuple(matchp), "exec": tuple(execp)}
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
    interface = ()
    actortype: str


class DoNotUnderstand(ValueError):
    """the node does not know how to interpret this instruction."""


class NoActorForEvent(DoNotUnderstand):
    """no actor matches this instruction."""


class NoTaskError(DoNotUnderstand):
    """control node issued a 'do' instruction with no attached task."""


class NoConfigError(DoNotUnderstand):
    """control node issued a 'configure' instruction with no config."""


class NoInstructionType(DoNotUnderstand):
    """control node issued an instruction with no type."""


class NoMatch(Exception):
    """actor does not match instruction. used for control flow."""


def inc_name(name, config):
    if name in config:
        matches = filter(lambda k: re.match(name, k), config)
        name = f"{name}_{len(tuple(matches)) + 1}"
    return name


def validate_instruction(instruction):
    """first-pass instruction validation"""
    if enum(instruction, "type") == "unknowninst":
        raise NoInstructionType
    if enum(instruction, "type") == "configure" and instruction.config is None:
        raise NoConfigError
    if enum(instruction, "type") == "do" and not instruction.HasField("task"):
        raise NoTaskError


class BaseNode(Matcher, ABC):
    """base class for Nodes and Stations."""

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
        self.params, self.name = {}, name
        self.cdict, self.threads, self.actors, self.sensors = {}, {}, {}, {}
        self._lock = threading.Lock()
        # TODO: do this better
        os.makedirs("logs", exist_ok=True)
        # self.logfile = f"logs/{self.name}_{filestamp()}.csv"
        for element in elements:
            self.add_element(element)
        self.exec = ThreadPoolExecutor(n_threads)
        self.can_receive = can_receive
        self.poll, self.timeout, self.signals = poll, timeout, {}
        if start is True:
            self.start()

    def restart_server(self):
        """
        (re)start the node's TCPTalk server (if it is supposed to have one).
        """
        if self.server is not None:
            self.server["kill"]()
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
                ackcheck=self._ackcheck,
                executor=self.exec,
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
        self.state = "running"

    def nodeid(self):
        """basic identifying information for node."""
        return {
            "name": self.name,
            "pid": os.getpid(),
            "host": socket.gethostname(),
        }

    def busy(self):
        """are we too busy to do new stuff?"""
        # TODO: or maybe explicitly check threads? do we want a free one?
        #  idk
        if self.exec._work_queue.qsize() > 0:
            return True
        return False

    def _main_loop(self):
        raise NotImplementedError

    def _shutdown(self, exception: Optional[Exception]):
        raise NotImplementedError

    def shutdown(self):
        self.locked = True
        self.signals['main'] = 1

    def _start(self):
        """
        private method to start the node. should only be executed by the
        public start() method.
        """
        for name, sensor in self.sensors.items():
            self.threads[name] = self.exec.submit(self._sensor_loop, sensor)
        exception = None
        try:
            self._main_loop()
        except Exception as ex:
            exception = ex
        self._shutdown(exception)
        return exception

    def _is_locked(self):
        return self._lock.locked()

    def _set_locked(self, state: bool):
        if state is True:
            self._lock.acquire(blocking=False)
        elif state is False:
            if self._lock.locked():
                self._lock.release()
        else:
            raise TypeError

    def __str__(self):
        pstring = f"{type(self).__name__} ({self.name})"
        pstring += f"\nthreads:\n"
        pstring += yprint({k: str(t) for k, t in self.threads.items()}, 2)
        pstring += f"\nactors: {[a for a in self.actors]}"
        pstring += f"\nsensors: {[s for s in self.sensors]}"
        pstring += f"\nconfig:\n{yprint(self.config, 2)}"
        return pstring

    def __repr__(self):
        return self.__str__()

    def _log(self, event, **extra_fields):
        logdict = valmap(json_sanitize, {"time": logstamp()} | extra_fields)
        if isinstance(event, (dict, Message)):
            # TODO, maybe: still want an event key?
            logdict |= flatten_for_json(event)
        else:
            logdict['event'] = json_sanitize(event)
        with self.logfile.open("a") as stream:
            json.dump(logdict, stream, indent=2)
            stream.write(",\n")

    def _get_config(self):
        props, params = {}, defaultdict(dict)
        for prop in self.interface:
            props[prop] = getattr(self, prop)
        for name, actor_cdict in self.params.items():
            for k, v in filter(lambda kv: kv[1] != (), actor_cdict.items()):
                params[name][k] = self.cdict[name].get(k)
        return {'interface': props, 'cdict': dict(params)}

    inbox = None
    config = property(_get_config)
    locked = property(_is_locked, _set_locked)
    __started = False
    threads = None
    server = None
    server_events = None
    state = "stopped"
    _ackcheck: Optional[Callable] = None


