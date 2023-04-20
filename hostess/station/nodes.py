from __future__ import annotations

import json
import random
import selectors
import socket
import time
from collections import defaultdict
from typing import Union

from dustgoggles.func import gmap
from google.protobuf.json_format import MessageToDict, Parse
from google.protobuf.message import Message

import hostess.station.proto.station_pb2 as pro
from hostess.station import bases
from hostess.station.messages import obj2msg, completed_task_msg, unpack_obj
from hostess.station.proto_utils import make_timestamp, dict2msg, enum
from hostess.station.talkie import (
    stsend,
    read_comm,
    timeout_factory,
    HOSTESS_ACK,
    make_comm,
)


class Node(bases.BaseNode):
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
        super().__init__(
            name=name,
            n_threads=n_threads,
            elements=elements,
            start=start,
            poll=poll,
            timeout=timeout
        )
        self.update_interval = update_interval
        self.actionable_events = []
        self.station = station
        self.actions = {}
        self.n_threads = n_threads
        self.instruction_queue = []
        self.update_timer, self.reset_update_timer = timeout_factory(False)

    def sensor_loop(self, sensor: bases.Sensor):
        exception = None
        try:
            while self.signals.get(sensor.name) is None:
                # noinspection PyPropertyAccess
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

    # TODO: figure out how to not make this infinity json objects
    def log_event(self, obj):
        with open(self.logfile, "a") as stream:
            stream.write(f"\n###\n{obj}\n###\n")

    def log(self, event, **extra_fields):
        try:
            loggers = self.match(event, "log")
        except bases.NoActorForEvent:
            # if we don't have a logger for something, that's fine
            return
        for logger in loggers:
            logger.execute(self, event, **extra_fields)

    def match_instruction(self, event) -> list[bases.Actor]:
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
        # noinspection PyTypeChecker
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
        # noinspection PyProtectedMember
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
        message.state.MergeFrom(pro.NodeState(config=obj2msg(self.config)))
        return message

    def reply_to_instruction(self, instruction, status: str):
        mdict = self._base_message()
        mdict["reason"], mdict["instruction_id"] = status, instruction.id
        self.send_to_station(Parse(json.dumps(mdict), pro.Update()))


class HeadlessNode(Node):
    def __init__(self, *args, **kwargs):
        station = ("", -1)
        super().__init__(station, *args, **kwargs)
        self.message_log = []

    def send_to_station(self, message):
        self.message_log.append(message)


class Station(bases.BaseNode):
    def __init__(self, host, port, name="station", n_threads=8, max_inbox=100):
        super().__init__(
            host=host,
            port=port,
            name=name,
            n_threads=n_threads,
            can_receive=True
        )
        self.max_inbox = max_inbox
        self.received = []
        self.events = []
        self.nodes, self.instruction_queue = [], defaultdict(list)
        self.tendtime, self.reset_tend = timeout_factory(False)
    #
    # def check_inbox(self):
    #     n_comms = len(self.inbox)
    #     for ix in range(n_comms):
    #         comm = self.inbox.pop()
    #         self.received.append(comm)
    #         self.match_comm(comm)

    def handle_info(self, message):
        notes = gmap(unpack_obj, message.info)
        for note in notes:
            try:
                actions = self.match(note, "info")
            except bases.NoActorForEvent:
                continue
            for action in actions:
                action.execute(self, note)

    def handle_incoming_message(self, message: pro.Update):
        # TODO: internal information about crashed and shut down nodes
        try:
            return self.handle_info(message)
        except (AttributeError, KeyError):
            pass
        # TODO: handle action report

    def _start(self):
        while self.signals.get('start') is None:
            if self.tendtime() > self.poll * 30:
                crashed_threads = self.server.tend()
                if len(self.inbox) > self.max_inbox:
                    self.inbox = self.inbox[-self.max_inbox:]
                self.reset_tend()
                if len(crashed_threads) > 0:
                    self.log({'server_errors': crashed_threads})

            time.sleep(self.poll)

    def ackcheck(self, _conn: socket.socket, comm: dict):
        # TODO: lockout might be too strict
        self.locked = True
        try:
            # in lieu of logging here, we periodically dump the contents of
            # self.received
            if comm["err"]:
                # TODO: send did-not-understand
                return HOSTESS_ACK, "notified sender of error"
            message = comm["body"]
            self.received.append(message)
            try:
                nodename = message.nodeid.name
            except (AttributeError, ValueError):
                # TODO: send not-enough-info message
                return HOSTESS_ACK, "notified sender not enough info"
            self.handle_incoming_message(comm["body"])
            if enum(message.state, "status") in ("shutdown", "crashed"):
                return None, "decline to send ack to terminated node"
            queue = self.instruction_queue[nodename]
            if len(queue) == 0:
                return HOSTESS_ACK, "sent ack"
            response = queue.pop()
            status = f"sent instruction {response.id}"
            return make_comm(response), status
        finally:
            self.locked = False

    def log(self, *args, **kwargs):
        pass
