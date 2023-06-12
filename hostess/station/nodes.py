from __future__ import annotations

import json
import random
import sys
import time
from itertools import count
from pathlib import Path
from types import ModuleType
from typing import Union, Literal, Optional, Type

from dustgoggles.dynamic import exc_report
from dustgoggles.structures import rmerge
from google.protobuf.json_format import MessageToDict, Parse
from google.protobuf.message import Message

import hostess.station.proto.station_pb2 as pro
from hostess.station import bases
from hostess.station.messages import pack_obj, completed_task_msg, unpack_obj
from hostess.station.proto_utils import make_timestamp, enum
from hostess.station.talkie import stsend, read_comm, timeout_factory

ConfigParamType = Literal["config_property", "config_dict"]


class Node(bases.BaseNode):
    def __init__(
        self,
        station_address: tuple[str, int],
        name: str,
        elements: tuple[Union[type[bases.Sensor], type[bases.Actor]]] = (),
        n_threads=6,
        poll=0.08,
        timeout=10,
        update_interval=10,
        start=False,
        # TODO: something better
        logdir=Path(__file__).parent / ".nodelogs",
        _is_process_owner=False,
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
        self.station = station_address
        self.actions = {}
        self.n_threads = n_threads
        self.instruction_queue = []
        self.update_timer, self.reset_update_timer = timeout_factory(False)
        self.logdir = logdir
        self.logdir.mkdir(exist_ok=True)
        # TODO: hacky temp log thing, do it better
        self.logid = f"{str(random.randint(0, 10000)).zfill(5)}"
        # TODO: add local hostname of node
        self.logfile = Path(
            self.logdir,
            f"{self.station[0]}_{self.station[1]}_{self.name}_"
            f"{self.logid}.log",
        )
        self.__is_process_owner = _is_process_owner

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

    def check_on_action(self, instruction_id: int):
        try:
            result = self.threads[f"Instruction_{instruction_id}"].result(0)
        except TimeoutError:
            return None, True
        except Exception as ex:
            # action crashed without setting its status as such
            self.actions[instruction_id]["status"] = "crash"
            return ex, False
        return result, False

    def _check_actions(self):
        """
        check running actions (threads launched as part of a 'do'
        instruction). if any have crashed or completed, log them and report
        them to the Station, then remove them from the thread cache.
        """
        to_clean = []
        for instruction_id, action in self.actions.items():
            # TODO: multistep "pipeline" case
            result, running = self.check_on_action(instruction_id)
            if running is True:
                continue
            # TODO: accomplish this with a wrapper
            if isinstance(result, Exception):
                self._log(action | exc_report(result, 0))
                action["result"] = result
            else:
                self._log(action, result=result)
                action["result"] = result
            # TODO: determine if we should reset update timer here
            # TODO: error handling
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
        info = pro.Update(info=[pack_obj(e) for e in self.actionable_events])
        message.MergeFrom(info)
        self.talk_to_station(message)
        self.actionable_events[:] = []

    def _main_loop(self):
        while self.signals.get("main") is None:
            # TODO: lockouts might be overly strict. we'll see
            # report actionable events (appended to actionable_events by
            # Sensors) to Station
            if (len(self.actionable_events) > 0) and (not self.locked):
                self.locked = True
                self._send_info()
                self.locked = False
            # clean up and report on completed / crashed actions
            if not self.locked:
                self._check_actions()
            # periodically check in with Station
            if self.update_timer() >= self.update_interval:
                if ("check_in" not in self.threads) and (not self.locked):
                    self._check_in()
            # TODO: launch sensors that were dynamically added; relaunch
            #  failed sensor threads
            # act on any Instructions received from Station
            if (len(self.instruction_queue) > 0) and (not self.locked):
                self.locked = True
                self._handle_instruction(self.instruction_queue.pop())
                self.locked = False
            time.sleep(self.poll)

    def _shutdown(self, exception: Optional[Exception] = None):
        """shut down the node"""
        self._log("beginning shutdown", category="exit")
        # divorce oneself from actors and acts, from events and instructions
        self.actions, self.actionable_events = {}, []
        # TODO, maybe: try to kill child processes (can't in general kill
        #  threads but sys.exit should handle it)
        # signal sensors to shut down
        for k in self.threads.keys():
            self.signals[k] = 1
        # goodbye to all that
        self.instruction_queue, self.actors, self.sensors = [], {}, {}
        if exception is not None:
            self.exec.submit(self._send_exit_report, exception)
            self._log(
                exc_report(exception, 0), status="crashed", category="exit"
            )
        else:
            self.exec.submit(self._send_exit_report)
            self._log("exiting", status="graceful", category="exit")
        self.state = "stopped"
        if self.__is_process_owner is True:
            sys.exit()

    def _send_exit_report(self, exception=None):
        """
        send Update to Station informing it that the node is exiting, and why.
        """
        mdict = self._base_message()
        mdict["reason"] = "exiting"
        status = "crashed" if exception is not None else "shutdown"
        mdict["state"]["status"] = status
        message = Parse(json.dumps(mdict), pro.Update())
        if exception is not None:
            info = pro.Update(
                info=[pack_obj(exc_report(exception, 0), "exception")]
            )
            message.MergeFrom(info)
        self.talk_to_station(message)

    def _report_on_action(self, action: dict):
        """report to Station on completed/failed action."""
        mdict = self._base_message()
        # TODO; multi-step case
        message = Parse(json.dumps(mdict), pro.Update())
        report = completed_task_msg(action)
        message.MergeFrom(pro.Update(completed=report, reason="completion"))
        self.talk_to_station(message)

    def _check_in(self):
        """send check-in Update to the Station."""
        mdict = self._base_message()
        mdict["reason"] = "scheduled"
        # TODO: multi-step case
        # action_reports = []
        # for id_, action in self.actions.items():
        #     action_reports.append(dict2msg(action, pro.ActionReport))
        message = Parse(json.dumps(mdict), pro.Update())
        # message.MergeFrom(pro.Update(running=action_reports))
        self.talk_to_station(message)
        self.reset_update_timer()

    def _match_task_instruction(self, event) -> list[bases.Actor]:
        """
        wrapper for self.match that specifically checks for actors that can
        execute a task described in an Instruction.
        """
        try:
            return self.match(event, "action")
        except StopIteration:
            raise bases.NoActorForEvent(
                str(self.explain_match(event, "action"))
            )

    def _do_actions(self, actions, instruction, key, noid):
        for action in actions:
            action.execute(self, instruction, key=key, noid=noid)

    def _configure_from_instruction(self, instruction: Message):
        for param in instruction.config:
            if enum(param, "paramtype") == "config_property":
                try:
                    setattr(self, param.value.name, unpack_obj(param.value))
                except AttributeError:
                    raise bases.DoNotUnderstand(
                        f"no property {param.value.name}"
                    )
            elif enum(param, "paramtype") == "config_dict":
                self.cdict = rmerge(self.cdict, unpack_obj(param.value))
            else:
                raise bases.DoNotUnderstand("unknown ConfigParamType")

    def _handle_instruction(self, instruction: Message):
        """interpret, reply to, and execute (if relevant) an Instruction."""
        status, err = "wilco", ""
        try:
            bases.validate_instruction(instruction)
            if enum(instruction, "type") == "configure":
                self._configure_from_instruction(instruction)
            # TODO, maybe: different kill behavior.
            elif enum(instruction, "type") in ("stop", "kill"):
                return self.shutdown()
            # config/shutdown etc. behavior is not performed by Actors.
            if enum(instruction, "type") != "do":
                return
            actions = self._match_task_instruction(instruction)
            if actions is None:
                return
            if instruction.id is None:
                key, noid, noid_infix = (
                    random.randint(0, int(1e7)),
                    True,
                    "noid_",
                )
            else:
                key, noid, noid_infix = instruction.id, False, ""
            threadname = f"Instruction_{noid_infix}{key}"
            # TODO: this could get sticky for the multi-step case
            self.threads[threadname] = self.exec.submit(
                self._do_actions, actions, instruction, key, noid
            )
        except bases.DoNotUnderstand:
            status = "bad_request"
            err = self.explain_match(instruction, "action")
        finally:
            # noinspection PyTypeChecker
            self._reply_to_instruction(instruction, status, err)
            self._log(
                instruction,
                category="comms",
                direction="recv",
                status=status,
                err=err,
            )

    def _trysend(self, message: Message):
        """
        try to send a message to the Station. Sleep if it doesn't work.
        """
        response, was_locked, timeout_counter = None, self.locked, count()
        waiting = False
        while response in (None, "timeout", "connection refused"):
            # if we couldn't get to the Station, log that fact, wait, and
            # retry. lock self while this is happening to ensure we don't do
            # this in big pulses.
            if response in ("timeout", "connection refused"):
                self.locked, waiting = True, True
                if next(timeout_counter) % 10 == 0:
                    self._log(response, category="comms", direction="recv")
                # TODO, maybe: this could be a separate attribute
                time.sleep(self.update_interval)
            response, _ = stsend(self._insert_config(message), *self.station)
        if waiting is True:
            self._log(
                "connection established", category="comms", direction="recv"
            )
        if enum(message, "reason") not in ("scheduled", "wilco"):
            self._log(message, category="comms", direction="sent")
        # if we locked ourselves due to bad responses, and we weren't already
        # locked for some reason -- like we often will have been if sending
        # a task report or something -- unlock ourselves.
        if was_locked is True:
            self.locked = False
        return response

    def _interpret_response(self, response):
        """interpret a response from the Station."""
        decoded = read_comm(response)
        if isinstance(decoded, dict):
            decoded = decoded["body"]
        if isinstance(decoded, pro.Instruction):
            self.instruction_queue.append(decoded)

    def talk_to_station(self, message):
        """send a Message to the Station and queue any returned Instruction."""
        response = self._trysend(message)
        self._interpret_response(response)

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
        message.state.MergeFrom(pro.NodeState(config=pack_obj(self.config)))
        return message

    def _reply_to_instruction(
        self, instruction, status: str, err: Optional[pro.PythonObject] = None
    ):
        """
        send a reply Update to an Instruction informing the Station that we
        will or won't do the thing.
        """
        mdict = self._base_message() | {
            "reason": status,
            "instruction_id": instruction.id,
        }
        msg = Parse(json.dumps(mdict), pro.Update())
        if err is not None:
            msg.MergeFrom(pro.Update(info=[pack_obj(err)]))
        self.talk_to_station(msg)


class HeadlessNode(Node):
    """
    simple Node implementation that just does stuff on its own. right now
    mostly for testing/prototyping but could easily be useful.
    """

    def __init__(self, *args, **kwargs):
        station = ("", -1)
        super().__init__(station, *args, **kwargs)
        self.message_log = []

    def talk_to_station(self, message):
        self.message_log.append(message)


def launch_node(
    station: tuple[str, int],
    name: str,
    node_module: str = "hostess.station.nodes",
    node_class: str = "Node",
):
    """simple hook for launching a node."""
    from hostess.utilities import import_module

    module: ModuleType = import_module(node_module)
    cls: Type[Node] = getattr(module, node_class)
    node: Node = cls(station, name, _is_process_owner=True)
    node.start()
    time.sleep(0.1)
    print("launcher: node started")
    while node.threads["main"].running():
        time.sleep(5)
    print("launcher: exiting")
