from __future__ import annotations

from collections import defaultdict
import json
import random
import socket
import time
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
        start=False,
    ):
        """
        configurable remote processor for hostess network. can gather data
        and/or execute actions based on the elements attached to it.

        station: (hostname, port) of supervising Station
        name: identifying name for node
        n_threads: max threads in executor
        elements: Sensors or Actors to add to node at creation.
        poll: delay, in seconds, for polling loops
        timeout: timeout, in s, for intra-hostess communications
        update: interval, in s, for check-in Updates to supervising Station
        """
        super().__init__(
            name=name,
            n_threads=n_threads,
            elements=elements,
            start=start,
            poll=poll,
            timeout=timeout,
        )
        self.update_interval = update_interval
        self.actionable_events = []
        self.station = station
        self.actions = {}
        self.n_threads = n_threads
        self.instruction_queue = []
        self.update_timer, self.reset_update_timer = timeout_factory(False)

    def _sensor_loop(self, sensor: bases.Sensor):
        """
        continuously check a Sensor. must be launched in a separate thread or
        it will block and be useless. should probably only be called by the
        main loop in _start().
        """
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

    def _check_actions(self):
        """
        check running actions (threads launched as part of a 'do'
        instruction). if any have crashed or completed, log them and report
        them to the Station, then remove them from the thread cache.
        """
        to_clean = []
        for instruction_id, action in self.actions.items():
            # TODO: multistep "pipeline" case
            if action["status"] != "running":
                self._log(action)
                # TODO: determine if we should reset update timer here
                self._report_on_action(action)
                # TODO: if report was successful, record the response
                to_clean.append(instruction_id)
        for target in to_clean:
            self.actions.pop(target)

    def _send_info(self):
        """
        construct an Update based on everything in the actionable_events
        cache and send it to the Station, then clear actionable_events.
        """
        mdict = self._base_message()
        mdict["reason"] = "info"
        message = Parse(json.dumps(mdict), pro.Update())
        # TODO: this might want to be more sophisticated
        info = pro.Update(info=[obj2msg(e) for e in self.actionable_events])
        message.MergeFrom(info)
        self.send_to_station(message)
        self.actionable_events[:] = []

    def _start(self):
        """
        private method to start the node. should only be called by the public
        .start method inherited from BaseNode.
        """
        for name, sensor in self.sensors.items():
            self.threads[name] = self.exec.submit(self.sensor_loop, sensor)
        exception = None
        try:
            while True:
                # TODO: lockouts might be overly strict. we'll see
                # report actionable events (appended to actionable_events by
                # Sensors) to Station
                if len(self.actionable_events) > 0:
                    self.locked = True
                    self._send_info()
                    self.locked = False
                # act on any Instructions received from Station
                if len(self.instruction_queue) > 0:
                    self.locked = True
                    self._handle_instruction(self.instruction_queue.pop())
                    self.locked = False
                # periodically check in with Station
                if self.update_timer() >= self.update_interval:
                    if "check_in" not in self.threads:
                        self._check_in()
                # clean up and report on completed / crashed actions
                self._check_actions()
                # TODO: launch sensors that were dynamically added; relaunch
                #  failed sensor threads
                time.sleep(self.poll)
        except Exception as ex:
            exception = ex
        finally:
            self.locked = True
            # TODO: other cleanup tasks, like signaling or killing threads --
            #  not sure exactly how hard we want to do this!
            self._send_exit_report(exception)

    def _send_exit_report(self, exception=None):
        """
        send Update to Station informing it that the node is exiting, and why.
        """
        mdict = self._base_message()
        status = "crashed" if exception is not None else "shutdown"
        mdict["state"]["status"] = status
        message = Parse(json.dumps(mdict), pro.Update())
        if exception is not None:
            info = pro.Update(info=[obj2msg(exception, "exception")])
            message.MergeFrom(info)
        self.send_to_station(message)

    def _report_on_action(self, action: dict):
        """report to Station on completed/failed action."""
        mdict = self._base_message()
        # TODO; multi-step case
        message = Parse(json.dumps(mdict), pro.Update())
        report = completed_task_msg(action)
        message.MergeFrom(pro.Update(completed=report))
        self.send_to_station(message)

    def _check_in(self):
        """send check-in Update to the STation."""
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

    # TODO: figure out how to not make this infinity json objects.
    #  this is basically a hook for logger Actors.
    def _log_event(self, obj):
        with open(self.logfile, "a") as stream:
            stream.write(f"\n###\n{obj}\n###\n")

    def _log(self, event, **extra_fields):
        """see if an event is loggable, then log it (this may just in."""
        try:
            loggers = self.match(event, "log")
        except bases.NoActorForEvent:
            # if we don't have a logger for something, that's fine
            return
        for logger in loggers:
            logger.execute(self, event, **extra_fields)

    def _match_task_instruction(self, event) -> list[bases.Actor]:
        """
        wrapper for self.match that specifically checks for actors that can
        execute a task described in an Instruction.
        """
        try:
            return self.match(event, "action")
        except StopIteration:
            raise bases.NoActorForEvent

    def _do_actions(self, actions, instruction, key, noid):
        for action in actions:
            action.execute(self, instruction, key=key, noid=noid)

    def _handle_instruction(self, instruction: Message):
        """interpret, reply to, and execute (if relevant) an Instruction."""
        actions = None
        try:
            bases.validate_instruction(instruction)
            # TODO: Actors for default config/shutdown handling. or maybe
            #  those should be directly attached to this class?
            actions = self._match_instruction(instruction)
            status = "wilco"
        except bases.DoNotUnderstand:
            # TODO: maybe add some more failure info
            rule, status = None, "bad_request"
        # noinspection PyTypeChecker
        self._reply_to_instruction(instruction, status)
        self._log(instruction, direction="received", status=status)
        if actions is None:
            return
        if instruction.id is None:
            key, noid, noid_infix = random.randint(0, int(1e7)), True, "noid_"
        else:
            key, noid, noid_infix = instruction.id, False, ""
        threadname = f"Instruction_{noid_infix}{key}"
        # TODO: this could get sticky for the multi-step case
        self.threads[threadname] = self.exec.submit(
            self._do_actions, actions, instruction, key, noid
        )

    def send_to_station(self, message):
        """send a Message to the Station."""
        message = self._insert_config(message)
        self._log(message, direction="sent")
        response, _ = stsend(message, *self.station)
        if response == "timeout":
            # TODO: do something else
            self._log("timeout", direction="received")
            return
        decoded = read_comm(response)
        if isinstance(decoded, dict):
            decoded = decoded["body"]
        self._log(decoded, direction="received")
        if isinstance(decoded, pro.Instruction):
            self.instruction_queue.append(decoded)

    def _base_message(self):
        """
        construct a dict with the basic components of an Update message --
        time, the node's ID, etc.
        """
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

    def _insert_config(self, message: Message):
        """
        insert the Node's current configuration details into a Message.
        TODO: this is distinct from _base_message() because it requires some
            fiddly Message construction that doesn't work well with the
            protobuf.json_format functions -- we may want to improve our
            message-to-dict functions to help this kind of case at some point.
        """
        if not message.HasField("state"):
            return
        message.state.MergeFrom(pro.NodeState(config=obj2msg(self.config)))
        return message

    def _reply_to_instruction(self, instruction, status: str):
        """
        send a reply Update to an Instruction informing the Station that we
        will or won't do the thing.
        """
        mdict = self._base_message()
        mdict["reason"], mdict["instruction_id"] = status, instruction.id
        self.send_to_station(Parse(json.dumps(mdict), pro.Update()))


class HeadlessNode(Node):
    """
    simple Node implementation that just does stuff on its own. right now
    mostly for testing/prototyping but could easily be useful.
    """

    def __init__(self, *args, **kwargs):
        station = ("", -1)
        super().__init__(station, *args, **kwargs)
        self.message_log = []

    def send_to_station(self, message):
        self.message_log.append(message)


class Station(bases.BaseNode):
    """
    central control node for hostess network. can receive Updates from and
    send Instructions to Nodes.
    """

    def __init__(
        self,
        host: str,
        port: int,
        name: str = "station",
        n_threads: int = 8,
        max_inbox=100,
    ):
        super().__init__(
            host=host,
            port=port,
            name=name,
            n_threads=n_threads,
            can_receive=True,
        )
        self.max_inbox = max_inbox
        self.events = []
        self.nodes, self.outbox = [], defaultdict(list)
        self.tendtime, self.reset_tend = timeout_factory(False)
        self.last_handler = None

    def _handle_info(self, message: Message):
        """
        check info received in a Message against the Station's 'info' Actors,
        and execute any relevant actions (most likely constructing
        Instructions).
        """
        notes = gmap(unpack_obj, message.info)
        for note in notes:
            try:
                actions = self.match(note, "info")
            except bases.NoActorForEvent:
                continue
            for action in actions:
                action.execute(self, note)

    def _handle_incoming_message(self, message: pro.Update):
        """
        handle an incoming message. right now just wraps _handle_info() but
        will eventually also deal with node state tracking, logging, etc.
        """
        # TODO: internal state tracking for crashed and shutdown nodes
        try:
            return self._handle_info(message)
        except (AttributeError, KeyError):
            pass
        # TODO: log action report
        # TODO: log other stuff

    def _start(self):
        """
        main loop for Station. should only be executed by the start() method
        inherited from BaseNode.
        """
        while self.signals.get("start") is None:
            if self.tendtime() > self.poll * 30:
                crashed_threads = self.server.tend()
                if len(self.inbox) > self.max_inbox:
                    self.inbox = self.inbox[-self.max_inbox :]
                self.reset_tend()
                if len(crashed_threads) > 0:
                    self.log({"server_errors": crashed_threads})
            time.sleep(self.poll)

    def _ackcheck(self, _conn: socket.socket, comm: dict):
        """
        callback for interpreting comms and responding as appropriate.
        should only be called inline of the ack() method of the Station's
        server attribute (a talkie.TCPTalk object).
        """
        # TODO: lockout might be too strict
        self.locked = True
        try:
            # TODO: choose whether to log here or to log when we dump the
            #  inbox + at exit.
            if comm["err"]:
                # TODO: send did-not-understand
                return HOSTESS_ACK, "notified sender of error"
            message = comm["body"]
            try:
                nodename = message.nodeid.name
            except (AttributeError, ValueError):
                # TODO: send not-enough-info message
                return HOSTESS_ACK, "notified sender not enough info"
            # interpret the comm here in case we want to immediately send a
            # response based on its contents (e.g., in a gPhoton 2-like
            # pipeline that's mostly coordinating execution of a big list of
            # non-serial processes, we would want to immediately send
            # another task to any Node that tells us it's finished one)
            self._handle_incoming_message(comm["body"])
            if enum(message.state, "status") in ("shutdown", "crashed"):
                return None, "decline to send ack to terminated node"
            # if we have any Instructions for the Node -- including ones that
            # might have been added to the outbox in the
            # _handle_incoming_message() workflow -- send them
            # TODO, probably: send more than one Instruction when available.
            #  we might want a special control code for that.
            queue = self.outbox[nodename]
            if len(queue) == 0:
                return HOSTESS_ACK, "sent ack"
            response = queue.pop()
            status = f"sent instruction {response.id}"
            return make_comm(response), status
        finally:
            self.locked = False

    def handlers(self) -> list[dict]:
        """
        list the nodes we've internally designated as handlers. this probably
        eventually wants to be more sophisticated.
        """
        return [n for n in self.nodes if "handler" in n["roles"]]

    def next_handler(self, _note):
        """
        pick the 'next' handler node. for distributing tasks between multiple
        available handler nodes.
        """
        if len(self.handlers()) == 0:
            raise StopIteration("no handler nodes available.")
        best = [n for n in self.handlers() if n["name"] != self.last_handler]
        if len(best) == 0:
            return self.handlers()[0]["name"]
        return best[0]["name"]

    def log(self, *args, **kwargs):
        pass
