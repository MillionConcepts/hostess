from __future__ import annotations

import selectors
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from inspect import getmembers_static
import json
import os
import random
import socket
import threading
import time
from typing import Literal, Union

from dustgoggles.func import filtern
from google.protobuf.json_format import MessageToDict, Parse
from google.protobuf.message import Message

from hostess.station import bases
from hostess.station.messages import obj2msg, completed_task_msg
from hostess.station.proto_utils import make_timestamp, dict2msg, enum
import hostess.station.proto.station_pb2 as pro
from hostess.station.talkie import (
    stsend,
    read_comm,
    timeout_factory,
    stlisten,
    HOSTESS_ACK, make_comm,
)
from hostess.utilities import filestamp


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


# noinspection PyTypeChecker
class Node(bases.Matcher, PropConsumer):
    def __init__(
        self,
        station: tuple[str, int],
        name: str,
        elements: tuple[Union[type[bases.Sensor], type[bases.Actor]]] = (),
        n_threads=6,
        poll=0.08,
        timeout=10,
        update_interval=10,
        start=True,
    ):
        """
        station: (hostname, port) of supervising Station
        name: identifying name for node
        n_threads: max threads in executor
        poll: delay, in seconds, for polling loops
        timeout: timeout, in s, for inter-node communications
        update: interval, in s, for check-in Updates to supervising Station
        """
        super().__init__()
        self.update_interval = update_interval
        self.poll, self.timeout, self.signals = poll, timeout, {}
        self.actionable_events = []
        self.actors, self.sensors, self.config = {}, {}, {}
        self.interface, self.params = [], {}
        self.name, self.station = name, station
        self.actions, self.threads, self._lock = {}, {}, threading.Lock()
        self.n_threads = n_threads
        # TODO: do this better
        os.makedirs("logs", exist_ok=True)
        self.logfile = f"logs/{self.name}_{filestamp()}.csv"
        for element in elements:
            self.add_element(element)
        self.exec, self.instruction_queue = ThreadPoolExecutor(n_threads), []
        self.update_timer, self.reset_update_timer = timeout_factory(False)
        if start is True:
            self.exec.submit(self.start)

    def add_element(self, cls: Union[type[bases.Actor], type[bases.Sensor]]):
        name = bases.inc_name(cls, self.config)
        element = cls()
        if issubclass(cls, bases.Actor):
            self.actors[name] = element
        elif issubclass(cls, bases.Sensor):
            if len(self.sensors) > self.n_threads - 2:
                raise EnvironmentError("Not enough threads to add sensor.")
            self.sensors[name] = element
        else:
            raise TypeError(f"{cls} is not a valid element for Node.")
        self.config[name], self.params[name] = element.config, element.params
        for prop in element.interface:
            self.consume_property(element, prop, f"{name}_{prop}")

    def sensor_loop(self, sensor: bases.Sensor):
        exception = None
        try:
            while self.signals.get(sensor.name) is None:
                if not self.locked:
                    sensor.check(self)
                time.sleep(self.poll)
        except Exception as ex:
            exception = ex
        finally:
            return {
                "name": sensor.name,
                "signal": self.signals.get(sensor.name),
                "exception": exception,
            }

    def check_running(self):
        to_clean = []
        for instruction_id, action in self.actions.items():
            # TODO: multistep case
            if action["status"] != "running":
                self.log(action)
                # TODO: reset update timer in here
                self.report_on(action)
                # TODO: if report was successful...
                to_clean.append(instruction_id)
        for target in to_clean:
            self.actions.pop(target)

    def update_from_events(self):
        mdict = self._base_message()
        mdict["reason"] = "info"
        message = Parse(json.dumps(mdict), pro.Update())
        # TODO: this might want to be more sophisticated
        info = pro.Update(info=[obj2msg(e) for e in self.actionable_events])
        message.MergeFrom(info)
        self.send_to_station(message)
        self.actionable_events = []

    def start(self):
        if self.__started is True:
            raise EnvironmentError("Node already started.")
        self.threads["main"] = self.exec.submit(self._start)
        self.__started = True

    def _start(self):
        for name, sensor in self.sensors.items():
            print("hi")
            self.threads[name] = self.exec.submit(self.sensor_loop, sensor)
        exception = None
        try:
            while True:
                # TODO: lockouts might be overly strict. we'll see
                if len(self.actionable_events) > 0:
                    self.locked = True
                    self.update_from_events()
                    self.locked = False
                if len(self.instruction_queue) > 0:
                    self.locked = True
                    self.interpret_instruction(self.instruction_queue.pop())
                    self.locked = False
                if self.update_timer() >= self.update_interval:
                    if "check_in" not in self.threads:
                        self.check_in()
                self.check_running()
                # TODO: clean up finished threads
                time.sleep(self.poll)
        except Exception as ex:
            exception = ex
        finally:
            self.locked = True
            # TODO: other cleanup tasks
            self.send_exit_report(exception)

    def send_exit_report(self, exception=None):
        mdict = self._base_message()
        status = "crashed" if exception is not None else "shutdown"
        mdict["state"]["status"] = status
        message = Parse(json.dumps(mdict), pro.Update())
        if exception is not None:
            info = pro.Update(info=[obj2msg(exception, "exception")])
            message.MergeFrom(info)
        self.send_to_station(message)

    def report_on(self, action):
        mdict = self._base_message()
        # TODO; multi-step case
        message = Parse(json.dumps(mdict), pro.Update())
        report = completed_task_msg(action)
        message.MergeFrom(pro.Update(completed=report))
        self.send_to_station(message)

    def check_in(self):
        mdict = self._base_message()
        mdict["reason"] = "scheduled"
        # TODO: multi-step case
        action_reports = []
        for id_, action in self.actions.items():
            action_reports.append(dict2msg(action, pro.ActionReport))
        message = Parse(json.dumps(mdict), pro.Update())
        message.MergeFrom(pro.Update(running=action_reports))
        self.send_to_station(message)
        self.reset_update_timer()

    def nodeid(self):
        return {
            "name": self.name,
            "pid": os.getpid(),
            "host": socket.gethostname(),
        }

    # TODO: figure out how to not make this infinity json objects
    def log_event(self, obj):
        with open(self.logfile, "a") as stream:
            stream.write(f"\n###\n{obj}\n###\n")

    def busy(self):
        # or maybe explicitly check threads? do we want a free one?
        # idk
        if self.exec._work_queue.qsize() > 0:
            return True
        return False

    def log(self, event, **extra_fields):
        try:
            loggers = self.match(event, "log")
        except bases.NoActorForEvent:
            # if we don't have a logger for something, that's fine
            return
        for logger in loggers:
            logger.execute(self, event, **extra_fields)

    def match_instruction(self, event) -> bases.Actor:
        try:
            return self.match(event, "action")
        except StopIteration:
            raise bases.NoActorForEvent

    def do_actions(self, actions, instruction, key, noid):
        for action in actions:
            action.execute(self, instruction, key=key, noid=noid)

    def interpret_instruction(self, instruction: Message):
        actions = None
        try:
            bases.validate_instruction(instruction)
            actions = self.match_instruction(instruction)
            status = "wilco"
        except bases.DoNotUnderstand:
            # TODO: maybe add some more failure info
            rule, status = None, "bad_request"
        self.reply_to_instruction(instruction, status)
        self.log(instruction, direction="received", status=status)
        if actions is None:
            return
        if instruction.id is None:
            key, noid, noid_infix = random.randint(0, int(1e7)), True, "noid_"
        else:
            key, noid, noid_infix = instruction.id, False, ""
        threadname = f"Instruction_{noid_infix}{key}"
        # TODO: this could get sticky for the multi-step case
        self.threads[threadname] = self.exec.submit(
            self.do_actions, actions, instruction, key, noid
        )

    def send_to_station(self, message):
        message = self.insert_config(message)
        self.log(message, direction="sent")
        response, _ = stsend(message, *self.station)
        if response == "timeout":
            # TODO: do something else
            self.log("timeout", direction="received")
            return
        decoded = read_comm(response)
        if isinstance(decoded, dict):
            decoded = decoded["body"]
        self.log(decoded, direction="received")
        if isinstance(decoded, pro.Instruction):
            self.instruction_queue.append(decoded)

    def _base_message(self):
        return {
            "nodeid": self.nodeid(),
            "time": MessageToDict(make_timestamp()),
            "state": {
                # TODO: check state
                "status": "nominal",
                # TODO: loc assignment
                "loc": "primary",
                "can_receive": False,
                "busy": self.busy(),
                "threads": {k: v._state for k, v in self.threads.items()},
            },
        }

    def insert_config(self, message: Message):
        if not message.HasField("state"):
            return
        message.state.MergeFrom(
            pro.NodeState(config=obj2msg(self.config))
        )
        return message

    def reply_to_instruction(self, instruction, status: str):
        mdict = self._base_message()
        mdict["reason"], mdict["instruction_id"] = status, instruction.id
        self.send_to_station(Parse(json.dumps(mdict), pro.Update()))

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


class HeadlessNode(Node):
    def __init__(self, *args, **kwargs):
        station = ("", -1)
        super().__init__(station, *args, **kwargs)
        self.message_log = []

    def send_to_station(self, message):
        self.message_log.append(message)


class Station:
    def __init__(self, host, port):
        self.exec = ThreadPoolExecutor(8)
        self.server, self.server_events, self.inbox = stlisten(
            host, port, ack=self.ack, executor=self.exec
        )
        self.received = []
        self.threads = self.server["threads"].copy()
        self.nodes, self.instruction_queue = [], defaultdict(list)

    # TODO: loop and relaunch threads

    def check_inbox(self):
        for comm in self.inbox:
            # check against rules
        pass

    def ack(
        self, sel: selectors.DefaultSelector, comm: dict, conn: socket.socket
    ):
        response = None
        try:
            sel.unregister(conn)
        except KeyError as ke:
            # someone else got here first
            return None, "ack attempt", f"{ke}"
        # in lieu of logging here, we periodically dump the contents of
        # self.received
        if comm["err"]:
            # TODO: send did-not-understand
            conn.send(HOSTESS_ACK)
            return None, "sent decode err", None
        message = comm["body"]
        try:
            nodename = message.nodeid.name
        except (AttributeError, ValueError):
            # TODO: send not-enough-info message
            conn.send(HOSTESS_ACK)
            return None, "sent not-enough-info", None
        if not enum(message.state, "status") in ("shutdown", "crashed"):
            # TODO: internal information about crashed and shut down nodes
            queue = self.instruction_queue[nodename]
            if len(queue) > 0:
                response = queue.pop()
        if response is None:
            response, record = HOSTESS_ACK, "sent ack"
        else:
            record = f"sent instruction {response.id}"
        try:
            conn.send(make_comm(response))
            return None, record, None
        except (KeyError, ValueError) as kve:
            # someone else got here first
            return None, "ack attempt", f"{kve}"

    def log(self, *args, **kwargs):
        pass
