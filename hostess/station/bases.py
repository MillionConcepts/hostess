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
from functools import partial
from itertools import chain
from pathlib import Path
from random import shuffle
from types import MappingProxyType as MPt
from typing import Any, Callable, Mapping, Union, Optional, Literal, Collection

from cytoolz import valmap
from dustgoggles.dynamic import exc_report
import yaml

# noinspection PyProtectedMember
from google.protobuf.pyext._message import Message

from hostess.station.handlers import flatten_for_json, json_sanitize
from hostess.station.proto_utils import enum
from hostess.station.talkie import TCPTalk
from hostess.utilities import configured, logstamp, yprint, filestamp


class ConsumedAttributeError(AttributeError):
    """An AttrConsumer's attempt to set a consumed attribute failed."""
    pass


class AttrConsumer:
    """
    Mix-in class that provides functionality for "consuming" attributes of
    other objects. Designed to permit Nodes and similar objects to promote
    interface properties of attached elements into their own interfaces as
    pseudo-attributes.
    """

    def __init__(self):
        self.attrefs = {}

    def consume_property(
        self, obj: Any, attr: str, newname: Optional[str] = None
    ):
        """
        consume an attribute of another object into this object's interface.

        Args:
            obj: object from which to consume attribute
            attr: name of attribute to consume
            newname: optional name of referencing attribute of `self`;
                if not specified, just use `attr`.
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
            try:
                return setattr(ref[0], ref[1], value)
            except AttributeError as ae:
                raise ConsumedAttributeError(str(ae))
        return super().__setattr__(attr, value)

    def __dir__(self):
        """
        add the pseudo-attributes in attrefs to this object's directory so
        that they look real.
        """
        # noinspection PyUnresolvedReferences
        return super().__dir__() + list(self.attrefs.keys())

    def _get_interface(self) -> list[str]:
        return [k for k in self.attrefs.keys()]

    def _set_interface(self, _):
        raise TypeError("interface does not support assignment")

    interface = property(_get_interface, _set_interface)


class Matcher(AttrConsumer, ABC):
    """
    Abstract mix-in class for Node and Sensor. Provides functionality for
    matching objects against Actors.
    """

    def match(self, event: Any, category=None, **kwargs) -> list[Actor]:
        """
        Check the Matcher's Actors to see which, if any, can handle an event.

        Args:
            event: object to match Actors against
            category: optional category of Actor to check; if specified,
                attempt to match `event` only against Actors whose `category`
                 attribute is equal to `category`
            **kwargs: kwargs to pass to Actor.match

        Returns:
            list of all Actors that matched `event`.

        Raises:
            NoActorForEvent if no Actors matched `event`.
        """
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

    def explain_match(
        self, event: Any, category=None, **kwargs
    ) -> dict[str, Union[str, bool]]:
        """
        Introspection function for matching process.

        Args:
            event: object to match Actors against
            category: optional category of Actors to match `event` against
            **kwargs: kwargs for `Actor.match()`

        Returns:
              dict whose keys are the names of Actors and whose values are
                the output of each actor's `match()` method, or a stringified
                version of the Exception it raised.

        """
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

    def filter_actors_by_category(
        self, actortype: Optional[str]
    ) -> list[Actor]:
        """
        Args:
            actortype: optional string denoting a category of Actor

        Returns:
            A list containing all of this object's Actors whose `category`
                attribute is equal to `category`, or, if `category` is None,
                simply all of this objcet's Actors.
        """
        if actortype is None:
            return list(self.actors.values())
        filtered = []
        for r in self.actors.values():
            if r.actortype == actortype:
                filtered.append(r)
            elif isinstance(r.actortype, tuple) and actortype in r.actortype:
                filtered.append(r)
        return filtered

    def add_element(
        self, cls: Union[type[Actor], type[Sensor]], name: Optional[str] = None
    ):
        """
        instantiate an Actor or Sensor and associate it with this object,
        consuming its interface properties and making it available for
        matching or sensor looping.

        Args:
            cls: Actor or Sensor type to instantiate and associate
            name: optional custom name for element that will be used to
                identify it in this object's configuration dictionary
                and properties interface. If not specified, uses the name of
                the element's class, suffixing incrementing numbers if that
                name would collide with an already-associated element.
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
    """
    abstract base class for Node elements that 'watch' some data source.
    semi-autonomous; runs asynchronously from its parent Node and uses its
    own Actors to watch the data source and decide what to bother the Node
    about.

    Sensors should generally only be instantiated by methods of a parent Node.
    """

    def __init__(self):
        super().__init__()
        self.interface = self.interface + self.class_interface
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
    def associate_actor(self, cls: type[Actor], name: Optional[str] = None):
        """
        instantiate an Actor and associate it with this Sensor.

        Args:
            cls: type of Actor to instantiate and associate
            name: optional name for Actor; used to identify it in this Sensor's
                interface and config. If not specified, defaults to the class
                name, suffixed with incrementing numbers if it would collide
                with the name of an already-attached Actor
        """
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

    def add_element(self, cls: type[Actor], name: Optional[str] = None):
        """
        use special Sensor association behavior, and don't permit Sensor mise
        en abyme.
        """
        if issubclass(cls, Sensor):
            raise TypeError("cannot add Sensors to a Sensor.")
        self.associate_actor(cls, name)

    def _log(self, *args, **kwargs):
        """
        call owning Node's log function. automatically include the fact that
        this Sensor generated the log entry.
        """
        self.owner._log(
            *args, category="sensor", sensor=self.name, **kwargs
        )

    def check(self, node: Node, **check_kwargs):
        """
        Main pointy-end function for Sensor.

        Use this Sensor's `checker()` method to look for new events and, if
        there are any, match them against this Sensor's Actors

        Args:
            node: Node to inform about any matching events. In normal
                operation, this argument will never be explicitly passed: it
                will always be this Sensor's owning Node, and will always be
                partially evaluated into this method during `Sensor.__init__`.
            **check_kwargs: kwargs to pass to `self.checker()`. Will also
                never be explicitly passed in normal operation; if there are
                any, they will be automatically taken from the "check" item of
                this Sensor's `config` dict.
        """
        step = "check"  # for error logging
        try:
            self.memory, events = self.checker(self.memory, **check_kwargs)
            for event in events:
                try:
                    step = "match"
                    actors = self.match(event)
                except NoActorForEvent:
                    continue
                for actor in actors:
                    step = f"execute {actor.name}"
                    # kwargs propagate to individual actors via `self.config`
                    actor.execute(node, event)
        except Exception as ex:
            self._log("check failure", step=step, exception=ex)

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
    """
    when this `Sensor` is running in a Node's `sensor_loop()` function, this
    sets interval in seconds between subsequent calls to `self.check()`. If
    not set, it defaults to the poll rate of the parent Node. 
    """
    _poll = None
    has_individual_pollrate = False
    base_config = MPt({})
    checker: Callable
    """
    data-fetching function called by `self.check()`. Must be defined in 
    implementations of this class.
    """
    actions: tuple[type[Actor]] = ()
    """
    default Actors associated with this class. `Sensor.__init__()` 
    instantiates and attaches an Actor of each specified type.
    """
    loggers: tuple[type[Actor]] = ()
    """same, but for logging-only Actors."""
    name: str
    class_interface = ("poll",)
    interface = ()


class Actor(ABC):
    """
    abstract base class enabling conditional responses to events. Actors
    should generally only be instantiated from methods of a parent Matcher.
    """

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
        Determine if this Actor can / should handle a given event.
        Must be implemented in concrete subclasses of Actor.

        Args:
            event: event to match.
            **_: placeholder for kwargs defined in a subclass.

        Returns:
            True if this Actor can handle `event`.

        Raises:
            NoMatch if this Actor cannot handle `event`.
        """
        raise NotImplementedError

    def execute(self, node: Node, event: Any, **kwargs) -> Any:
        """
        This method defines what an Actor does with objects it matches. Must
        be implemented in concrete subclasses of Actor.

        Args:
            node: parent Node of this Actor. In normal operation, this
                argument will never be explicitly passed, but instead
                partially evaluated into this method in `Actor.__init__()`.
            event: object to do something with.
            **kwargs: placeholder for kwargs defined in a subclass.
        """
        raise NotImplementedError

    name: str
    config: Mapping
    class_interface = ()
    interface = ()
    actortype: str
    owner = None


class DispatchActor(Actor, ABC):
    """
    abstract class for Actors intended to dispatch Instructions from
    Stations to Nodes.
    """

    def __init__(self):
        super().__init__()
        self.interface = self.interface + (
            "target_name",
            "target_actor",
            "target_picker",
        )

    def pick(self, station: "Station", instruction: Message, **_):
        """
        Pick which of a Station's Delegates to send an Instruction to.

        Args:
            station: Parent Station. in normal operation, will never be
                explicitly passed.
            instruction: Instruction Message to dispatch.
            **_: placeholder for kwargs defined in concrete subclasses.

        Returns:
            Name of selected Delegate.

        Raises:
            `NoMatchingDelegate` if no Delegate matches rules defined by this
                Actor's target_name, target_picker, or target_actor
                attributes, including if the Station has no Delegates.
            `TypeError` if this Actor's target_name, target_picker, and
                target_actor attributes are all None.
        """
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
            targets = [
                n for n in targets if self.target_picker(n, instruction)
            ]
        not_busy = [n for n in targets if n.get("busy") is False]
        if len(targets) == 0:
            raise NoMatchingDelegate
        if len(not_busy) == 0:
            shuffle(targets)
            return targets[0]["name"]
        return not_busy[0]["name"]

    target_name: Optional[str] = None
    """if set, dispatch only to Delegates named exactly target_name."""
    target_actor: Optional[str] = None
    """
    if set, dispatch only to Delegates that have an Actor named target_actor.
    """
    target_picker: Optional[Callable[[dict, Message], str]] = None
    """
    function that can be used to define more complex Delegate selection 
    behaviors.
    """


class DoNotUnderstand(ValueError):
    """This Delegate does not know how to interpret this Instruction."""


class NoActorForEvent(DoNotUnderstand):
    """This Matcher has no Actor that matches this Event."""


class NoTaskError(DoNotUnderstand):
    """
    This Delegate received a 'do' Instruction, but the Instruction included
    neither an Action Message or a description of a task to perform.
    """


class NoConfigError(DoNotUnderstand):
    """
    This Delegate received a 'configure' Instruction, but the Instruction
    did not specify what to configure.
    """


class NoInstructionType(DoNotUnderstand):
    """
    This Delegate received an Instruction that did not specify what type of
    Instruction it was.
    """


class NoMatch(Exception):
    """
    This Actor does not match an event passed to its `check()` method. This
    Exception is used primarily for control flow.
    """


class NoMatchingDelegate(Exception):
    """
    A Station, possibly via one of its Actors, attempted to dispatch an
    Instruction to one of its Delegates, but found no appropriate Delegate.
    """


class AllBusy(Exception):
    """
    A Station, possibly via one of its Actors, attempted to dispatch an
    Instruction to one of its Delegates, but all appropriate Delegates for the
    Instruction were busy.
    """


def inc_name(name: str, config: Mapping[str]) -> str:
    """
    If a string would duplicate an existing key of a dictionary, add a
    numerical suffix to it to prevent collisions.

    Args:
        name: key caller would like to add to `config`
        config: mapping caller would like to use `name` as a key in

    Returns:
        `name` if it does not duplicate any key of `config`; `name` with a
            unique-within-keys-of-config numerical suffix if it does.
    """
    if name in config:
        matches = filter(lambda k: re.match(name, k), config)
        name = f"{name}_{len(tuple(matches)) + 1}"
    return name


def element_dict(elements: Collection[Union[Actor, Sensor]]) -> dict[str, str]:
    """
    Actor/Sensor title formatter for `identify_elements()` or similar
    introspection methods.
    """
    return {
        k: f"{v.__class__.__module__}.{v.__class__.__name__}"
        for k, v in elements
    }


def validate_instruction(instruction: Message):
    """
    First-pass Instruction validation function. Called by Delegates on
        Instruction receipt; can also be used as an independent validator.

    Args:
        instruction: Instruction to validate.

    Raises:
        NoInstructionType if `instruction` does not have a defined type.
        NoConfigError if a 'configure' Instruction does not specify a config.
        NoTaskError if a 'do' instruction does not specify a task to perform.
    """
    if enum(instruction, "type") == "unknowninst":
        raise NoInstructionType
    if enum(instruction, "type") == "configure" and instruction.config is None:
        raise NoConfigError
    if enum(instruction, "type") == "do" and not instruction.HasField("task"):
        raise NoTaskError


class Node(Matcher, ABC):
    """
    Abstract base class for Delegates and Stations. Defines core behavior like
    running Sensors, spooling events to Actors, managing a TCPTalk server,
    constructing an interface, starting up, shutting down, and logging.
    """

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
            if self.__is_process_owner is True:
                atexit.register(
                    partial(self._log, "shutdown complete", category="system")
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
        """
        concrete subclasses must define rules for constructing log filenames.
        """
        raise NotImplementedError

    def start(self):
        """
        Start the Node's main loop and, if it is supposed to have one, its
        TCPTalk server.
        """
        if self.__started is True:
            raise EnvironmentError("Node already started.")
        self._log("starting", category="system")
        self.restart_server()
        self.threads["main"] = self.exc.submit(self._start)
        self.__started = True
        self.state = "nominal"
        self._log("completed start", category="system")

    def nodeid(self) -> dict[str, Union[str, int]]:
        """
        get basic identifying information for this Node.

        Returns:
            dict whose keys are "name", "pid", and "host".
        """
        return {
            "name": self.name,
            "pid": os.getpid(),
            "host": socket.gethostname(),
        }

    def add_element(
        self, cls: Union[type[Actor], type[Sensor]], name: Optional[str] = None
    ):
        """
        Instantiate an Actor or Sensor and associate it with this Node.

        Args:
            cls: type of Actor or Sensor to instantiate and associate.
            name: optional name for Actor or Sensor used to identify it in
                this Node's interface/config. name of its class will be used
                if not specified, plus a numerical suffix if it would collide
                with the name of an already-attached element.
        """
        logname = name if name is not None else cls.name
        self._log(
            f"adding element", cls=str(cls), name=logname, category="system"
        )
        super().add_element(cls, name)
        self._log(
            f"added element", cls=str(cls), name=logname, category="system"
        )

    def busy(self) -> bool:
        """are we too busy to do new stuff?"""
        # TODO: or maybe explicitly check threads? do we want a free one?
        #  idk
        if self.exc._work_queue.qsize() > 0:
            return True
        return False

    def _main_loop(self):
        """
        Implementations of Node must define what they actually do when they're
        running. Should only be executed in a thread, and only by
        `Node._start()`.
        """
        raise NotImplementedError

    def _shutdown(self, exception: Optional[Exception] = None):
        """Implementations of Node must define specific shutdown behavior."""
        raise NotImplementedError

    def shutdown(self, exception: Optional[Exception] = None):
        """
        Shut down the Node.

        Args:
            exception: Unhandled Exception that stopped the Node's main loop,
                if any.
        """
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
            self._log("completed shutdown")
        except Exception as ex:
            self._log(
                "shutdown exception", exception=ex, category='system'
            )
        self.is_shut_down = True

    def _start(self) -> Optional[Exception]:
        """
        private method to start the Node. should only be executed by
        `Node.start()`.

        Returns:
            Exception that stopped the Node's main loop, if any. None on
                intentional shutdown.
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
