from __future__ import annotations

import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import socket
import time
from typing import Literal

from dustgoggles.func import filtern
from google.protobuf.message import Message

from hostess.station import rules
import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import make_timestamp
from hostess.station.talkie import stsend, read_comm, timeout_factory

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
        start = True
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
        self.rules = self.rule_class()
        self.name, self.nodetype, self.station = name, nodetype, station
        self.actions, self.threads, self._lock = {}, [], threading.Lock()
        if len(self.rules['watch']) > n_threads - 1:
            raise EnvironmentError(
                "Not enough threads to run this node. Set n_threads higher or "
                "reduce watch rules."
            )
        self.exec, self.instruction_queue = ThreadPoolExecutor(n_threads), []
        self.update_timer, self.reset_update_timer = timeout_factory(False)
        if start is True:
            self.start()

    def launch_watch_loop(self, source):
        exception = None
        try:
            while self.signals.get(source.name) is None:
                if not self.locked:
                    source.func(self)
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
        for action in self.actions.values():
            # TODO: multistep case
            if action['status'] != 'running':
                self.log(action)
                # TODO: reset update timer in here
                self.report_on(action)

    def start(self):
        for source in self.rules['sources']:
            self.threads[source.name] = self.launch_watch_loop(source)

        exception = None
        try:
            while True:
                if len(self.instruction_queue) > 0:
                    # TODO: this lockout might be overly strict. we'll see
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

    def describe(self):
        return pro.NodeId(
            name=self.name,
            type=self.nodetype,
            pid=os.getpid(),
            host=socket.gethostname()
        )

    def busy(self):
        # or maybe explicitly check threads? do we want a free one?
        # idk
        if self.exec._work_queue.qsize() > 0:
            return True
        return False

    def log(self, event, **extra_fields):
        try:
            logrule = self.match_event(event, "log")
        except rules.NoRuleForEvent:
            # if we don't have a logger for something, that's fine
            return
        logrule.execute(self, event, **extra_fields)

    def match_event(self, event, event_type) -> rules.Rule:
        try:
            return filtern(lambda r: r.match(event), self.rules[event_type])
        except StopIteration:
            raise rules.NoRuleForEvent

    def interpret_instruction(self, instruction: Message):
        try:
            rules.validate_instruction(instruction)
            rule = self.match_event(instruction, instruction.type)
            status = "wilco"
        except rules.DoNotUnderstand:
            # TODO: maybe add some more failure info
            rule, status = None, "bad_request"
        self.respond_to_instruction(instruction, status)
        self.log(instruction, direction="received", status=status)
        if rule is None:
            return
        if instruction.id is None:
            threadname = f"Instruction_noid_{random.randint(0, int(1e7))}"
        else:
            threadname = f"Instruction_{instruction.id}"
        self.threads[threadname] = self.exec.submit(
            rule.execute, self, instruction
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

    def _base_update(self):
        return pro.Update(nodeid=self.describe(), time=make_timestamp())

    def respond_to_instruction(self, instruction, status: str):
        message = self._base_update()
        message.reason, message.instruction_id = status, instruction.id
        self.send_to_station(message)

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
    rule_class: rules.Ruleset = rules.DefaultRules
