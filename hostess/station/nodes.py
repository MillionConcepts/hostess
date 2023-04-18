from __future__ import annotations

import json
import os
import random
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from inspect import getmembers_static
from typing import Literal, Union

from dustgoggles.func import filtern
from google.protobuf.json_format import MessageToDict, Parse
from google.protobuf.message import Message

import hostess.station.proto.station_pb2 as pro
from hostess.station import actors
from hostess.station.messages import pack_arg
from hostess.station.proto_utils import make_timestamp, dict2msg, enum
from hostess.station.talkie import stsend, read_comm, timeout_factory
from hostess.utilities import filestamp

NodeType: Literal["handler", "listener"]


class PropConsumer:

    def __init__(self):
        self.proprefs = {}

    def consume_property(self, obj, attr, newname=None):
        prop = filtern(
            lambda kv: kv[0] == attr, getmembers_static(type(obj))
        )[1]
        if not isinstance(prop, property):
            raise TypeError(f'{attr} of {type(obj).__name__} not a property')
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
class Node(actors.Matcher, PropConsumer):
    def __init__(
        self,
        station: tuple[str, int],
        nodetype: NodeType,
        name: str,
        elements: tuple[Union[type[actors.Sensor], type[actors.Actor]]] = (),
        n_threads=6,
        poll=0.08,
        timeout=10,
        update_interval=10,
        start=True
    ):
        """
        station: (hostname, port) of supervising Station
        nodetype: nominal category of node
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
        self.name, self.nodetype, self.station = name, nodetype, station
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

    def add_element(self, cls: Union[type[actors.Actor], type[actors.Sensor]]):
        name = actors.inc_name(cls, self.config)
        element = cls()
        if issubclass(cls, actors.Actor):
            self.actors[name] = element
        elif issubclass(cls, actors.Sensor):
            if len(self.sensors) > self.n_threads - 2:
                raise EnvironmentError("Not enough threads to add sensor.")
            self.sensors[name] = element
        else:
            raise TypeError(f"{cls} is not a valid element for Node.")
        self.config[name], self.params[name] = element.config, element.params
        for prop in element.interface:
            self.consume_property(element, prop, f"{name}_{prop}")

    def sensor_loop(self, sensor: actors.Sensor):
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
                'name': sensor.name,
                'signal': self.signals.get(sensor.name),
                'exception': exception
            }

    def check_running(self):
        for instruction_id, action in self.actions.values():
            # TODO: multistep case
            if action['status'] != 'running':
                self.log(action)
                # TODO: reset update timer in here
                self.report_on(action)

    def update_from_event(self, event):
        mdict = self._base_message()

        mdict['reason'] = "info"
        message = Parse(json.dumps(mdict), pro.Update())
        # TODO: this wants to be more sophisticated
        info = pro.Update(info=[pack_arg("event", event)])
        message.MergeFrom(info)
        self.send_to_station(message)

    def start(self):
        if self.__started is True:
            raise EnvironmentError("Node already started.")
        self.threads['main'] = self.exec.submit(self._start)
        self.__started = True

    def _start(self):
        for name, sensor in self.sensors.items():
            print('hi')
            self.threads[name] = self.exec.submit(self.sensor_loop, sensor)
        exception = None
        try:
            while True:
                # TODO: lockouts might be overly strict. we'll see
                if len(self.actionable_events) > 0:
                    self.locked = True
                    self.update_from_event(self.actionable_events.pop())
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
            # TODO: other cleanup tasks
            self.send_exit_report(exception)

    def send_exit_report(self, exception=None):
        mdict = self._base_message()
        status = "crashed" if exception is not None else "shutdown"
        mdict['state'] = {'status': status}
        message = Parse(json.dumps(mdict), pro.Update())
        if exception is not None:
            info = pro.Update(info=[pack_arg("exception", exception)])
            message.MergeFrom(info)
        self.send_to_station(message)

    def report_on(self, action):
        report = self._base_message()
        # TODO; multi-step case
        report.action = dict2msg(action, pro.ActionReport)
        report.ok = action['status'] == 'success'
        self.send_to_station(report)

    def check_in(self):
        update = self._base_message()
        update.reason = 'scheduled'
        # TODO: multi-step case
        action_reports = []
        for id_, action in self.actions.items():
            action_reports.append(dict2msg(action, pro.ActionReport))
        update.action_rules = action_reports
        self.send_to_station(update)
        self.reset_update_timer()

    def describe(self):
        return {
            'name': self.name,
            'type': self.nodetype,
            'pid': os.getpid(),
            'host': socket.gethostname()
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
        except actors.NoActorForEvent:
            # if we don't have a logger for something, that's fine
            return
        for logger in loggers:
            logger.execute(self, event, **extra_fields)

    def match_instruction(self, event) -> actors.Actor:
        try:
            return self.match(event, "action")
        except StopIteration:
            raise actors.NoActorForEvent

    def do_actions(self, actions, instruction, key, noid):
        for action in actions:
            action.execute(self, instruction, key=key, noid=noid)

    def interpret_instruction(self, instruction: Message):
        actions = None
        try:
            actors.validate_instruction(instruction)
            actions = self.match_instruction(instruction)
            status = "wilco"
        except actors.DoNotUnderstand:
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
        self.log(message, direction="sent")
        response, _ = stsend(message, *self.station)
        decoded = read_comm(response)
        if isinstance(decoded, dict):
            decoded = decoded['body']
        self.log(decoded, direction="received")
        if isinstance(decoded, pro.Instruction):
            self.instruction_queue.append(decoded)

    def _base_message(self):
        return {
            'nodeid': self.describe(), 'time': MessageToDict(make_timestamp())
        }

    def reply_to_instruction(self, instruction, status: str):
        mdict = self._base_message()
        mdict['reason'], mdict['instruction_id'] = status, instruction.id
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