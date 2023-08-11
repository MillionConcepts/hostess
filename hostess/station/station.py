from __future__ import annotations

from collections import defaultdict
import datetime as dt
from pathlib import Path
import random
import socket
import sys
import time
from typing import Literal, Mapping, Optional, Any

from dustgoggles.dynamic import exc_report
from dustgoggles.func import gmap, filtern
from google.protobuf.message import Message

from hostess.caller import generic_python_endpoint
from hostess.station import bases
from hostess.station.bases import NoMatchingDelegate
from hostess.station.comm import make_comm
from hostess.station.messages import (
    pack_obj,
    unpack_obj,
    make_instruction,
    update_instruction_timestamp,
    Mailbox,
    Msg,
    unpack_message,
)
import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import enum
from hostess.station.talkie import timeout_factory
from hostess.subutils import RunCommand


class Station(bases.Node):
    """
    central control node for hostess network. can receive Updates from and
    send Instructions to Delegates.
    """

    def __init__(
            self,
            host: str,
            port: int,
            name: str = "station",
            n_threads: int = 8,
            max_inbox_mb: float = 250,
            logdir=Path(__file__).parent / ".nodelogs",
            _is_process_owner=False,
            **kwargs,
    ):
        super().__init__(
            host=host,
            port=port,
            name=name,
            n_threads=n_threads,
            can_receive=True,
            **kwargs,
        )
        self.max_inbox_mb = max_inbox_mb
        self.events, self.delegates, self.tasks = [], [], {}
        self.outboxes = defaultdict(Mailbox)
        self.tendtime, self.reset_tend = timeout_factory(False)
        self.last_handler = None
        # TODO: share log id -- or another identifier, like init time --
        #   between station and delegate
        self.logid = f"{str(random.randint(0, 10000)).zfill(5)}"
        self.logfile = Path(logdir, f"{host}_{port}_station_{self.logid}.log")
        self.__is_process_owner = _is_process_owner

    def set_delegate_properties(self, delegate: str, **propvals):
        # TODO: update delegate info record if relevant
        if len(propvals) == 0:
            raise TypeError("can't send a no-op config instruction")
        config = [
            pro.ConfigParam(paramtype="config_property", value=pack_obj(v, k))
            for k, v in propvals.items()
        ]
        self.outboxes[delegate].append(
            make_instruction("configure", config=config)
        )

    def queue_task(self, delegate: str, instruction: pro.Instruction):
        """
        queue an instruction for delegate, and keep track of its state. this
        method is intended to be used for task-type instructions rather than
        config etc. instructions we may not want to track in the same way.
        """
        if instruction.HasField("pipe"):
            raise NotImplementedError(
                "multi-step pipelines are not yet implemented"
            )
        if not enum(instruction, "type") == "do":
            raise ValueError("task instructions must have type 'do'")
        self.tasks[instruction.id] = {
            "init_time": instruction.time.ToDatetime(dt.timezone.utc),
            "sent_time": None,
            "ack_time": None,
            "status": "queued",
            "delegate": delegate,
            "name": instruction.action.name,
            "action_id": instruction.action.id,
            "description": dict(instruction.action.description),
        }
        self.outboxes[delegate].append(instruction)

    def set_delegate_config(self, delegate: str, config: Mapping):
        config = pro.ConfigParam(
            paramtype="config_dict", value=pack_obj(config)
        )
        self.outboxes[delegate].append(
            make_instruction("configure", config=config)
        )

    def shutdown_delegate(
            self, delegate: str, how: Literal["stop", "kill"] = "stop"
    ):
        self.outboxes[delegate].append(make_instruction(how))

    # TODO, maybe: signatures on these match-and-execute things are getting
    #  a little weird and specialized. maybe that's ok, but maybe we should
    #  make a more unified interface.
    def match_and_execute(self, obj: Any, category: str):
        try:
            actions = self.match(obj, category)
        except bases.NoActorForEvent:
            # TODO: _plausibly_ log this?
            return
        except (AttributeError, KeyError) as ex:
            self._log(exc_report(ex, 0), category=category)
            return
        self._log(
            obj,
            category=category,
            matches=[a.name for a in actions],
        )
        for action in actions:
            try:
                action.execute(self, obj)
            except NoMatchingDelegate:
                self._log("no delegate for action", action=action)

    def _handle_info(self, message: Message):
        """
        check info received in a Message against the Station's 'info' Actors,
        and execute any relevant actions (most likely constructing
        Instructions).
        """
        notes = gmap(unpack_obj, message.info)
        for note in notes:
            self.match_and_execute(note, "info")

    def get_delegate(self, name: str):
        return [n for n in self.delegates if n["name"] == name][0]

    def _handle_state(self, message: Message):
        try:
            delegate = self.get_delegate(message.delegateid.name)
        # TODO: how do we handle mystery-appearing delegates with dupe names
        except IndexError:
            delegate = blank_delegateinfo()
            self.delegates.append(delegate)
        message = unpack_message(message)
        delegate |= {
            "last_seen": dt.datetime.fromisoformat(message["time"]),
            "wait_time": 0,
            "interface": message["state"]["interface"],
            "cdict": message["state"]["cdict"],
            "reported_status": message["state"]["status"],
            "pid": message["delegateid"]["pid"],
            "actors": message["state"].get("actors", []),
            "sensors": message["state"].get("sensors", []),
            "busy": message["state"]["busy"],
            "host": message["delegateid"]["host"],
            "running": message.get("running", []),
            "infocount": message['state'].get('infocount', {})
        }
        for name, state in message["state"]["threads"].items():
            try:
                instruction_id = int(name.replace("Instruction_", ""))
                if self.tasks[instruction_id]["status"] not in (
                        "success",
                        "failure",
                        "crash",
                        "timeout",
                ):
                    # don't override formally reported status
                    self.tasks[instruction_id]["status"] = state.lower()
            except (ValueError, KeyError):  # main thread, sensors, etc.
                continue

    def _handle_report(self, message: Message):
        if not message.HasField("completed"):
            return
        if len(message.completed.steps) > 0:
            raise NotImplementedError
        self._log(message, category="report")
        if not message.completed.HasField("action"):
            # TODO: an unusual case. maybe log
            return
        completed = unpack_message(message.completed)
        try:
            task = self.tasks[completed["instruction_id"]]
            task["status"] = completed["action"]["status"]
            task["start_time"] = completed["action"]["time"]["start"]
            task["end_time"] = completed["action"]["time"]["end"]
            task["duration"] = completed["action"]["time"]["duration"]
        except KeyError:
            # TODO: an undesirable case, log
            pass
        obj = unpack_obj(message.completed.action.result)
        self.match_and_execute(obj, "completion")

    def _handle_wilco(self, message: pro.Update):
        # TODO: handle do not understand messages
        if not enum(message, "reason") in ("wilco", "bad_request"):
            return
        # TODO: handle config acks
        if message.instruction_id not in self.tasks.keys():
            return
        task = self.tasks[message.instruction_id]
        task["ack_time"] = message.time.ToDatetime(dt.timezone.utc)
        task["status"] = enum(message, "reason")
        # TODO: behavior in response to bad_request notifications

    def _handle_incoming_message(self, message: pro.Update):
        """
        handle an incoming message. right now just wraps _handle_info() but
        will eventually also deal with delegate state tracking, logging, etc.
        """
        for op in ("wilco", "state", "info", "report"):
            try:
                getattr(self, f"_handle_{op}")(message)
            except NotImplementedError:
                # TODO: plausibly some logging
                pass
            except Exception as ex:
                print("bad handling", op, ex)

    def _shutdown(self, exception: Optional[Exception] = None):
        """shut down the Station."""
        self.state = "shutdown" if exception is None else "crashed"
        self.exception = exception
        self._log("beginning shutdown", category="exit")
        # clear outbox etc.
        for k in self.outboxes.keys():
            self.outboxes[k] = Mailbox()
        self.actors, self.sensors = {}, {}
        for delegate in self.delegates:
            self.shutdown_delegate(delegate["name"], "stop")
        waiting, unwait = timeout_factory(timeout=30)
        # make sure every delegate is shut down, timing out at 30s --
        # this will also ensure we get all exit reports from newly-shutdown
        # delegates
        self._check_delegates()
        while any(
            n["inferred_status"] not in ("missing", "shutdown", "crashed")
            for n in self.delegates
        ):
            try:
                waiting()
            except TimeoutError:
                break
            time.sleep(0.1)
            self._check_delegates()
        unwait()
        # ensure local delegate threads are totally shut down
        still_running = None
        while still_running is not False:
            still_running = False
            for n in filter(lambda x: "obj" in x, self.delegates):
                if any(map(lambda t: t.running(), n["obj"].threads.values())):
                    still_running = True
            time.sleep(0.1)
            waiting()
        # shut down the server etc.
        # TODO: this is a little messy because of the discrepancy in thread
        #  and signal names. maybe unify this somehow.
        for k in self.signals:
            self.signals[k] = 1
        self.server.kill()
        while any(t.running() for t in self.server.threads.values()):
            try:
                waiting()
            except TimeoutError:
                break
            time.sleep(0.1)
        if exception is not None:
            self._log(
                exc_report(exception, 0), status="crashed", category="exit"
            )
        else:
            self._log("exiting", status="graceful", category="exit")

        if self.__is_process_owner:
            sys.exit()

    def _main_loop(self):
        """main loop for Station."""

        while self.signals.get("main") is None:
            self._check_delegates()
            if self.tendtime() > self.poll * 8:
                crashed_threads = self.server.tend()
                if len(crashed_threads) > 0:
                    self._log(crashed_threads, category="server_errors")
                    self.threads |= self.server.threads
                self.inbox.prune(self.max_inbox_mb)
                self.reset_tend()
            time.sleep(self.poll)

    def _check_delegates(self):
        now = dt.datetime.now(tz=dt.timezone.utc)
        for n in self.delegates:
            if n["reported_status"] in ("shutdown", "crashed"):
                n["inferred_status"] = n["reported_status"]
                continue
            if n["reported_status"] == "initializing":
                n["wait_time"] = (now - n["init_time"]).total_seconds()
            else:
                n["wait_time"] = (now - n["last_seen"]).total_seconds()
            # adding 5 seconds here as grace for network lag spikes
            if n["wait_time"] > 10 * n["update_interval"] + 5:
                n["inferred_status"] = "missing"
            elif n["wait_time"] > 3 * n["update_interval"]:
                n["inferred_status"] = "delayed"
            else:
                n["inferred_status"] = n["reported_status"]
            # TODO: trigger some behavior

    def _record_message(self, box, msg, pos):
        """
        helper function for _ackcheck(). write log entry for sent message and
        record it as sent.
        """
        # make new Msg object w/updated timestamp.
        # this is weird-looking, but, by intent, Msg object cached
        # properties are essentially immutable wrt the underlying message
        update_instruction_timestamp(msg.message)
        box[pos] = Msg(msg.message)
        self._log(box[pos].message, category="comms", direction="send")
        box[pos].sent = True

    def _select_outgoing_message(
        self, delegatename
    ) -> tuple[Optional[Mailbox], Optional[pro.Instruction], Optional[int]]:
        """
        pick outgoing message, if one exists for this delegate.
        helper function for _ackcheck().
        """
        # TODO, probably: send more than one Instruction when available.
        #  we might want a special control code for that.
        box = self.outboxes[delegatename]
        # TODO, maybe: this search will be expensive if outboxes get really big
        #  -- might want some kind of hashing
        messages = tuple(
            filter(lambda pm: pm[1].sent is False, enumerate(box))
        )
        if len(messages) == 0:
            return None, None, None  # this will trigger an empty ack message
        # ensure that we send shudown Instructions before config instructions,
        # and config Instructions before do Instructions
        # (the do instructions might need correct config to work!)
        priorities, pos, msg = ("kill", "stop", "configure"), None, None
        for priority in priorities:
            try:
                pos, msg = filtern(lambda pm: pm[1].type == priority, messages)
                break
            except StopIteration:
                continue
        if msg is None:
            pos, msg = messages[0]
        msg.sent = True
        return box, msg, pos

    def _update_task_record(self, msg):
        """
        helper function for _ackcheck(). update task record associated with a
        'do' instruction so that we know we sent it.
        """
        task_record = self.tasks[msg.id]
        task_record["sent_time"] = dt.datetime.now(tz=dt.timezone.utc)
        task_record["status"] = "sent"

    def _ackcheck(self, _conn: socket.socket, comm: dict):
        """
        callback for interpreting comms and responding as appropriate.
        should only be called inline of the ack() method of the Station's
        server attribute (a talkie.TCPTalk object).
        """
        # TODO: lockout might be too strict
        msg, self.locked = None, True
        try:
            if comm["body"] == b"situation":
                if self.state in ("shutdown", "crashed"):
                    return make_comm(b"shutting down"), "sent shutdown notice"
                return self._situation_comm(), "sent situation"
            if comm["err"]:
                # TODO: log this and send did-not-understand
                self._log("failed to decode", type="comms", conn=_conn)
                return make_comm(b"bad decode"), "notified sender bad decode"
            incoming = comm["body"]
            try:
                delegatename = incoming.delegateid.name
            except (AttributeError, ValueError):
                self._log("bad request", error="bad request", category="comms")
                return make_comm(b"bad request"), "notified sender bad request"
            # interpret the comm here in case we want to immediately send a
            # response based on its contents (e.g., in a gPhoton 2-like
            # pipeline that's mostly coordinating execution of a big list of
            # non-serial processes, we would want to immediately send
            # another task to any Delegate that tells us it's finished one)
            self._handle_incoming_message(comm["body"])
            if enum(incoming.state, "status") in ("shutdown", "crashed"):
                return make_comm(b""), "send ack to terminating delegate"
            # if we have any Instructions for the Delegate -- including ones
            # that might have been added to the outbox in the
            # _handle_incoming_message() workflow -- pick one to send
            box, msg, pos = self._select_outgoing_message(delegatename)
            # ...and if we don't have any, send empty ack comm
            if msg is None:
                return make_comm(b""), "sent ack"
            # log message and, if relevant, update task queue
            # TODO, maybe: logging and updating task queue here is much less
            #  complicated, but has the downside that it will occur _before_
            #  we confirm receipt.
            # this type should have been validated earlier in queue_task
            if msg["type"] == "do":
                self._update_task_record(msg)
            self._record_message(box, msg, pos)
            return box[pos].comm, f"sent instruction {box[pos].id}"
        except Exception as ex:
            self._log(exc_report(ex, 0), category="comms")
            return b"response failure", "failed to respond"
        finally:
            self.locked = False

    @staticmethod
    def _launch_delegate_in_subprocess(context, kwargs):
        """component function for launch_delegate"""
        endpoint = generic_python_endpoint(
            "hostess.station.delegates",
            "launch_delegate",
            payload=kwargs,
            argument_unpacking="**",
            print_result=True,
        )
        if context == "daemon":
            output = RunCommand(endpoint, _disown=True)()
        elif context == "subprocess":
            output = RunCommand(endpoint, _asynchronous=True)()
        else:
            raise ValueError(
                f"unsupported context {context}. Supported contexts are "
                f"'daemon', 'subprocess', and 'local'."
            )
        return output

    def launch_delegate(
        self,
        name,
        elements=(),
        host="localhost",
        update_interval=0.1,
        context="daemon",
        **kwargs,
    ):
        """
        launch a delegate, by default daemonized, and add it to the delegatelist.
        may also launch locally or in a non-daemonized subprocess.
        """
        # TODO: option to specify remote host and run this using SSH
        if host != "localhost":
            raise NotImplementedError
        # TODO: some facility for relaunching
        if any(n["name"] == name for n in self.delegates):
            raise ValueError("can't launch a delegate with a duplicate name")
        kwargs = {
             "station_address": (self.host, self.port),
             "name": name,
             "elements": elements,
             "update_interval": update_interval,
         } | kwargs
        delegateinfo = blank_delegateinfo() | {
            "name": name,
            "inferred_status": "initializing",
            "update_interval": update_interval,
        }
        if context == "local":
            # mostly for debugging / dev purposes
            from hostess.station.delegates import launch_delegate

            output = launch_delegate(is_local=True, **kwargs)
            delegateinfo["obj"] = output
        else:
            output = self._launch_delegate_in_subprocess(context, kwargs)
        self.delegates.append(delegateinfo)
        return output

    def save_port_to_shared_memory(self, address: Optional[str] = None):
        from dustgoggles.codex.implements import Sticky
        from dustgoggles.codex.memutilz import (
            deactivate_shared_memory_resource_tracker
        )

        deactivate_shared_memory_resource_tracker()
        address = self.name if address is None else address
        Sticky.note(
            self.port, address=f"{address}-port-report", cleanup_on_exit=True
        )

    def _situation_comm(self) -> bytes:
        from hostess.station.situation.response_organizers import situation_of

        return make_comm(pack_obj(situation_of(self)))


def blank_delegateinfo():
    return {
        "last_seen": None,
        "reported_status": "initializing",
        "init_time": dt.datetime.now(dt.timezone.utc),
        "wait_time": 0,
        "running": [],
        "interface": {},
        "actors": {},
    }


def get_port_from_shared_memory(memory_address='station'):
    from dustgoggles.codex.implements import Sticky
    from dustgoggles.codex.memutilz import (
        deactivate_shared_memory_resource_tracker
    )

    deactivate_shared_memory_resource_tracker()
    if (port := Sticky(f"{memory_address}-port-report").read()) is None:
        raise FileNotFoundError('no port at address')
    return port
