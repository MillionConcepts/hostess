from __future__ import annotations

import os
import random
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

import hostess.station.proto.station_pb2 as pro
from dustgoggles.func import filtern
from google.protobuf.message import Message
from hostess.station import rules
from hostess.station.messages import pack_arg
from hostess.station.proto_utils import make_timestamp, dict2msg
from hostess.station.talkie import stsend, read_comm, timeout_factory
from hostess.utilities import filestamp

NodeType: Literal["handler", "listener"]


# noinspection PyTypeChecker
class Node:
    def __init__(
        self,
        station: tuple[str, int],
        nodetype: NodeType,
        name: str,
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
        self.update_interval = update_interval
        self.poll, self.timeout, self.signals = poll, timeout, {}
        self.actionable_events = []
        self.definition = self.definition_class()
        self.name, self.nodetype, self.station = name, nodetype, station
        self.actions, self.threads, self._lock = {}, [], threading.Lock()
        # TODO: do this better
        os.makedirs("logs", exist_ok=True)
        self.logfile = f"logs/{self.name}_{filestamp()}.csv"
        if len(self.definition.sources) > n_threads - 1:
            raise EnvironmentError(
                "Not enough threads to run this node. Set n_threads higher or "
                "reduce watch rules."
            )
        self.exec, self.instruction_queue = ThreadPoolExecutor(n_threads), []
        self.update_timer, self.reset_update_timer = timeout_factory(False)
        if start is True:
            self.start()

    def launch_watch_loop(self, source: rules.Source):
        exception = None
        try:
            while self.signals.get(source.name) is None:
                if not self.locked:
                    source.check(self)
                time.sleep(self.poll)
        except Exception as ex:
            exception = ex
        finally:
            return {
                'name': source.name,
                'signal': self.signals.get(source.name),
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
        update = self._base_message()
        # TODO: this wants to be more sophisticated
        update.info = [pack_arg('info', event)]
        update.reason = "info"
        self.send_to_station(update)

    def start(self):
        for source in self.definition.sources:
            self.threads[source.name] = self.launch_watch_loop(source)
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
        update = self._base_message()
        update.status = "crashed" if exception is not None else "shutdown"
        update.reason = "exiting"
        self.send_to_station(update)

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
        update.actions = action_reports
        self.send_to_station(update)
        self.reset_update_timer()

    def describe(self):
        return pro.NodeId(
            name=self.name,
            type=self.nodetype,
            pid=os.getpid(),
            host=socket.gethostname()
        )

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
            loggers = self.match_event(event, "log")
        except rules.NoRuleForEvent:
            # if we don't have a logger for something, that's fine
            return
        for logger in loggers:
            logger.execute(self, event, **extra_fields)

    def match_event(self, event, category):
        return self.definition.match(event, category)

    def match_instruction(self, event) -> rules.Rule:
        try:
            return self.match_event(event, "action")
        except StopIteration:
            raise rules.NoRuleForEvent

    def do_actions(self, actions, instruction, key, noid):
        for action in actions:
            action.execute(self, instruction, key=key, noid=noid)

    def interpret_instruction(self, instruction: Message):
        actions = None
        try:
            rules.validate_instruction(instruction)
            actions = self.match_instruction(instruction)
            status = "wilco"
        except rules.DoNotUnderstand:
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
        return pro.Update(nodeid=self.describe(), time=make_timestamp())

    def reply_to_instruction(self, instruction, status: str):
        message = self._base_message()
        message.reason, message.instruction_id = status, instruction.id
        self.send_to_station(message)

    def configure(self, rule, key, value):
        self.definition.config[rule][key] = value

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
    definition_class: rules.Definition = rules.DefaultRules
