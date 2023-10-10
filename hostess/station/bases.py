"""base classes and helpers for Delegates, Stations, Sensors, and Actors."""
from __future__ import annotations

import atexit
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
from pathlib import Path
from random import shuffle
from types import MappingProxyType as MPt
from typing import Any, Callable, Mapping, Union, Optional, Literal, Collection

import yaml
from cytoolz import valmap
from dustgoggles.dynamic import exc_report

# noinspection PyProtectedMember
from google.protobuf.pyext._message import Message

from hostess.station.handlers import flatten_for_json, json_sanitize
from hostess.station.proto_utils import enum
from hostess.station.talkie import TCPTalk
from hostess.utilities import configured, logstamp, yprint, filestamp


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
        filtered = []
        for r in self.actors.values():
            if r.actortype == actortype:
                filtered.append(r)
            elif isinstance(r.actortype, tuple) and actortype in r.actortype:
                filtered.append(r)
        return filtered

    def add_element(self, cls: Union[type[Actor], type[Sensor]], name=None):
        """
        associate an Actor or Sensor with this object, consuming its
        interface properties and making it available for matching or sensor
        looping.
        """
        name = inc_name(cls.name if name is None else name, self.cdict)
        element = cls()
        element.name, element.owner = name, self
        if issubclass(cls, Actor):
            self.actors[name] = element
        elif issubclass(cls, Sensor):
            self.sensors[name] = element
            element.set_poll_nonsticky(self.poll)
        else:
            raise TypeError(f"{cls} is not a valid subelement for this class.")
        self.cdict[name], self.params[name] = element.config, element.params
        for prop in element.interface:
            self.consume_property(element, prop, f"{name}_{prop}")
        return name

    actors: dict[str, Actor]
    params: dict[str, Any]
    sensors: dict[str, "Sensor"]


class Sensor(Matcher, ABC):
    """base class for watching an input source."""

    def __init__(self):
        super().__init__()
        self.interface = self.interface + self.class_interface
        if "sources" in dir(self):
            raise TypeError("bad configuration: can't nest Sources.")
        props, self.actors = [], {}
        checkp = inspect.getfullargspec(self.check).kwonlyargs
        self.config = {"check": {k: None for k in checkp}}
        self.check_params = tuple(checkp)
        self.params = {"check": self.check_params}
        for cls in chain(self.actions, self.loggers):
            self.add_element(cls, cls.name)
        for prop in props:
            setattr(self, *prop)
        self.check = configured(self.check, self.config["check"])
        self.memory = None

    # TODO: perhaps make this less redundant with superclass add_element
    def associate_actor(self, cls, name=None):
        """associate an actor with this Sensor."""
        name = inc_name(cls.name if name is None else name, self.actors)
        self.actors[name] = cls()
        self.actors[name].owner, self.actors[name].name = self, name
        self.config[name] = self.actors[name].config
        self.params[name] = {
            "match": self.actors[name].params["match"],
            "exec": self.actors[name].params["exec"],
        }
        for prop in self.actors[name].interface:
            self.props.append(
                (f"{name}_{prop}", getattr(self.actors[name], prop))
            )

    def add_element(self, cls: type[Actor], name=None):
        if issubclass(cls, Sensor):
            raise TypeError("cannot add Sensors to a Sensor.")
        self.associate_actor(cls, name)

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

    def _get_poll(self) -> Optional[float]:
        return self._poll

    def _set_poll(self, pollrate: float):
        self._poll = pollrate
        self.has_individual_pollrate = True

    def set_poll_nonsticky(self, pollrate: float):
        self._poll = pollrate

    def close(self):
        pass

    poll = property(_get_poll, _set_poll)
    _poll = None
    has_individual_pollrate = False
    base_config = MPt({})
    checker: Callable
    actions: tuple[type[Actor]] = ()
    loggers: tuple[type[Actor]] = ()
    name: str
    class_interface = ("poll",)
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

    def execute(self, node: Node, event: Any, **kwargs) -> Any:
        raise NotImplementedError

    name: str
    config: Mapping
    class_interface = ()
    interface = ()
    actortype: str
    owner = None


class DispatchActor(Actor, ABC):
    """
    abstract subclass for actors intended to dispatch instructions from
    Stations to Nodes.
    """

    def __init__(self):
        super().__init__()
        self.interface = self.interface + (
            "target_name",
            "target_actor",
            "target_picker",
        )

    def pick(self, station: "Station", message: Message, **_):
        if all(
            t is None
            for t in (self.target_name, self.target_actor, self.target_picker)
        ):
            raise TypeError("Must have a delegate name, actor name, or picker")
        targets = station.delegates
        if self.target_name is not None:
            targets = [n for n in targets if n["name"] == self.target_name]
        if self.target_actor is not None:
            targets = [n for n in targets if self.target_actor in n["actors"]]
        if self.target_picker is not None:
            targets = [n for n in targets if self.target_picker(n, message)]
        not_busy = [n for n in targets if n.get("busy") is False]
        if len(targets) == 0:
            raise NoMatchingDelegate
        if len(not_busy) == 0:
            shuffle(targets)
            return targets[0]["name"]
        return not_busy[0]["name"]

    target_name: Optional[str] = None
    target_actor: Optional[str] = None
    target_picker: Optional[Callable[[dict, Message], str]] = None


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


class NoMatchingDelegate(Exception):
    """no nodes are available that match this action."""


class AllBusy(Exception):
    """all nodes we could dispatch this instruction to are busy."""


def inc_name(name: str, config: Mapping[str]) -> str:
    """add an incrementing numerical suffix to a prospective duplicate key"""
    if name in config:
        matches = filter(lambda k: re.match(name, k), config)
        name = f"{name}_{len(tuple(matches)) + 1}"
    return name


def element_dict(elements: Collection[Actor | Sensor]) -> dict[str, str]:
    """sensor title formatter for identify_elements or similar tasks"""
    return {
        k: f"{v.__class__.__module__}.{v.__class__.__name__}"
        for k, v in elements
    }


def validate_instruction(instruction):
    """first-pass instruction validation"""
    if enum(instruction, "type") == "unknowninst":
        raise NoInstructionType
    if enum(instruction, "type") == "configure" and instruction.config is None:
        raise NoConfigError
    if enum(instruction, "type") == "do" and not instruction.HasField("task"):
        raise NoTaskError


class Node(Matcher, ABC):
    """base class for Delegates and Stations."""

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
        _is_process_owner=False,
        **extra_attrs
    ):
        super().__init__()
        # attributes unique to subclass but necessary for this step of
        # initialization, mostly related to logging
        for k, v in extra_attrs.items():
            setattr(self, k, v)
        self.host, self.port = host, port
        self.params, self.name = {}, name
        self.init_time = filestamp()
        self._set_logfile()
        try:
            self.logfile.parent.mkdir(exist_ok=True, parents=True)
            self._log("initializing", category="system")
            self.cdict, self.actors, self.sensors = {}, {}, {}
            self.threads, self._lock = {}, threading.Lock()
            for element in elements:
                self.add_element(element)
            self.n_threads = n_threads
            self.exc = ThreadPoolExecutor(n_threads)
            self.can_receive = can_receive
            self.poll, self.timeout, self.signals = poll, timeout, {}
            self.__is_process_owner = _is_process_owner
            self.is_shut_down = False
            self.exception = None
            atexit.register(
                self.exc.shutdown, wait=False, cancel_futures=True
            )
            if start is True:
                self.start()
        except Exception as ex:
            self._log(
                "initialization failed", exception=ex, category="system"
            )



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
                executor=self.exc,
            )
            self.threads |= self.server.threads
            self.inbox = self.server.data
            for ix, sig in self.server.signals.items():
                self.signals[f"server_{ix}"] = sig
        else:
            self.server, self.server_events, self.inbox = None, None, None

    def _set_logfile(self):
        raise NotImplementedError

    def start(self):
        if self.__started is True:
            raise EnvironmentError("Node already started.")
        self._log("starting", category="system")
        self.restart_server()
        self.threads["main"] = self.exc.submit(self._start)
        self.__started = True
        self.state = "nominal"
        self._log("completed start", category="system")

    def nodeid(self):
        """basic identifying information for node."""
        return {
            "name": self.name,
            "pid": os.getpid(),
            "host": socket.gethostname(),
        }

    def add_element(self, cls: Union[type[Actor], type[Sensor]], name=None):
        logname = name if name is not None else cls.name
        self._log(
            f"adding element", cls=str(cls), name=logname, category="system"
        )
        super().add_element(cls, name)
        self._log(
            f"added element", cls=str(cls), name=logname, category="system"
        )

    def busy(self):
        """are we too busy to do new stuff?"""
        # TODO: or maybe explicitly check threads? do we want a free one?
        #  idk
        if self.exc._work_queue.qsize() > 0:
            return True
        return False

    def _main_loop(self):
        """you need to define a main loop when you use this base class"""
        raise NotImplementedError

    def _shutdown(self, exception: Optional[Exception] = None):
        """subclasses must define a way to shut down"""
        raise NotImplementedError

    def shutdown(self, exception=None, instruction=None):
        self.locked = True
        self.state = "shutdown" if exception is None else "crashed"
        self._log(
            'beginning shutdown',
            category='system',
            state=self.state,
            exception=exception
        )
        self.signals["main"] = 1
        try:
            self._shutdown(exception=exception)
        except Exception as ex:
            self._log(
                "shutdown exception", exception=ex, category='system'
            )
        self.is_shut_down = True
        self._log("shutdown complete", category='system')

    def _start(self):
        """
        private method to start the node. should only be executed by the
        public start() method.
        """
        for name, sensor in self.sensors.items():
            self.threads[name] = self.exc.submit(self._sensor_loop, sensor)
        exception = None
        try:
            self._main_loop()
        except Exception as ex:
            exception = ex
        # don't do a double shutdown during a graceful termination
        if self.state not in ("shutdown", "crashed"):
            self.shutdown(exception)
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

    def identify_elements(
        self, element_type: Optional[Literal["actors", "sensors"]] = None
    ) -> dict[str, str]:
        """
        return a dict with names and classes of node's attached elements.
        intended primarily to produce a lightweight version of identifying
        information for transmission.
        """
        if element_type is None:
            elements = chain(self.actors.items(), self.sensors.items())
        else:
            elements = getattr(self, element_type).items()
        return element_dict(elements)

    def __str__(self):
        pstring = f"{type(self).__name__} ({self.name})"
        pstring += f"\nthreads:\n"
        pstring += yprint({k: str(t) for k, t in self.threads.items()}, 2)
        pstring += f"\nactors: {list(self.actors)}"
        pstring += f"\nsensors: {list(self.sensors)}"
        pstring += f"\nconfig:\n{yprint(self.config, 2)}"
        return pstring

    def __repr__(self):
        return self.__str__()

    def _log(self, event, **extra_fields):
        exkeys = [
            k for k, v in extra_fields.items() if isinstance(v, Exception)
        ]
        for k in exkeys:
            extra_fields |= exc_report(extra_fields.pop(k))
        logdict = valmap(json_sanitize, {"time": logstamp(3)} | extra_fields)
        if isinstance(event, (dict, Message)):
            # TODO, maybe: still want an event key?
            logdict |= flatten_for_json(event)
        else:
            logdict["event"] = json_sanitize(event)
        with self.logfile.open("a") as stream:
            json.dump(logdict, stream, indent=2)
            stream.write(",\n")

    def _get_config(self):
        props, params = {}, defaultdict(dict)
        for prop in self.interface:
            try:
                props[prop] = getattr(self, prop)
            except AttributeError:
                props[prop] = "UNINITIALIZED PROPERTY"
        for name, actor_cdict in self.params.items():
            for k, v in filter(lambda kv: kv[1] != (), actor_cdict.items()):
                params[name][k] = self.cdict[name].get(k)
        return {"interface": props, "cdict": dict(params)}

    def _get_n_threads(self):
        return self._n_threads

    def _set_n_threads(self, n_threads):
        self._n_threads = n_threads
        if self.exc is None:
            return
        self.exc._max_workers = n_threads

    exc = None
    inbox = None
    config = property(_get_config)
    locked = property(_is_locked, _set_locked)
    __started = False
    threads = None
    server = None
    server_events = None
    state = "stopped"
    _ackcheck: Optional[Callable] = None
    logfile: Path
