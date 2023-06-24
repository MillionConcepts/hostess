from __future__ import annotations

import datetime as dt
import random
import socket
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Literal, Mapping, Optional, Any

from dustgoggles.dynamic import exc_report
from dustgoggles.func import gmap, filtern
from google.protobuf.message import Message

import hostess.station.proto.station_pb2 as pro
from hostess.caller import generic_python_endpoint
from hostess.station import bases
from hostess.station.bases import NoMatchingNode, AllBusy
from hostess.station.messages import pack_obj, unpack_obj, make_instruction, \
    update_instruction_timestamp, Mailbox, Msg, unpack_message
from hostess.station.proto_utils import enum
from hostess.station.talkie import timeout_factory
from hostess.station.comm import make_comm
from hostess.subutils import RunCommand


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
        max_inbox_mb: float = 250,
        logdir=Path(__file__).parent / ".nodelogs",
        _is_process_owner=False
    ):
        super().__init__(
            host=host,
            port=port,
            name=name,
            n_threads=n_threads,
            can_receive=True,
        )
        self.max_inbox_mb = max_inbox_mb
        self.events, self.nodes, self.tasks = [], [], {}
        self.outboxes = defaultdict(Mailbox)
        self.tendtime, self.reset_tend = timeout_factory(False)
        self.last_handler = None
        # TODO: share log id -- or another identifier, like init time --
        #   between station and node
        self.logid = f"{str(random.randint(0, 10000)).zfill(5)}"
        self.logfile = Path(logdir, f"{host}_{port}_station_{self.logid}")
        self.__is_process_owner = _is_process_owner

    def set_node_properties(self, node: str, **propvals):
        # TODO: update node info record if relevant
        if len(propvals) == 0:
            raise TypeError("can't send a no-op config instruction")
        config = [
            pro.ConfigParam(paramtype="config_property", value=pack_obj(v, k))
            for k, v in propvals.items()
        ]
        self.outboxes[node].append(make_instruction("configure", config=config))

    def queue_task(self, node: str, instruction: pro.Instruction):
        """
        queue an instruction for node, and keep track of its state. this
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
            'init_time': instruction.time.ToDatetime(dt.timezone.utc),
            'sent_time': None,
            'ack_time': None,
            'status': 'queued',
            'name': instruction.action.name,
            'action_id': instruction.action.id,
            'description': instruction.action.description
         }
        self.outboxes[node].append(instruction)

    def set_node_config(self, node: str, config: Mapping):
        config = pro.ConfigParam(
            paramtype="config_dict", value=pack_obj(config)
        )
        self.outboxes[node].append(make_instruction("configure", config=config))

    def shutdown_node(self, node: str, how: Literal['stop', 'kill'] = 'stop'):
        self.outboxes[node].append(make_instruction(how))

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
            except NoMatchingNode:
                self._log("no node for action", action=action)
            except AllBusy:
                # TODO: instruction-queuing behavior
                pass

    def _handle_info(self, message: Message):
        """
        check info received in a Message against the Station's 'info' Actors,
        and execute any relevant actions (most likely constructing
        Instructions).
        """
        notes = gmap(unpack_obj, message.info)
        for note in notes:
            self.match_and_execute(note, "info")

    def _handle_state(self, message: Message):
        try:
            node = [
                n for n in self.nodes if n['name'] == message.nodeid.name
            ][0]
        # TODO: how do we handle mystery-appearing nodes with dupe names
        except IndexError:
            node = blank_nodeinfo()
            self.nodes.append(node)
        message = unpack_message(message)
        # TODO: insert info on running actions
        node |= {
            'last_seen': dt.datetime.fromisoformat(message['time']),
            'wait_time': 0,
            'interface': message['state']['interface'],
            'cdict': message['state']['cdict'],
            'reported_status': message['state']['status'],
            'pid': message['nodeid']['pid'],
            'actors': message['state'].get('actors', []),
            'sensors': message['state'].get('sensors', []),
            'busy': message['state']['busy']
        }

    def _handle_report(self, message: Message):
        if not message.HasField("completed"):
            return
        # TODO: handle instruction tracking
        if len(message.completed.steps) > 0:
            raise NotImplementedError
        self._log(message, category="report")
        if not message.completed.HasField("action"):
            return
        obj = unpack_obj(message.completed.action.result)
        self.match_and_execute(obj, "completion")

    def _handle_wilco(self, message: pro.Update):
        # TODO: handle do not understand messages
        if not enum(message, 'reason') in ('wilco', 'bad_request'):
            return
        # TODO: handle config acks
        if message.instruction_id not in self.tasks.keys():
            return
        task = self.tasks[message.instruction_id]
        task['ack_time'] = message.time.ToDatetime(dt.timezone.utc)
        task['status'] = enum(message, 'reason')
        # TODO: behavior in response to bad_request notifications

    def _handle_incoming_message(self, message: pro.Update):
        """
        handle an incoming message. right now just wraps _handle_info() but
        will eventually also deal with node state tracking, logging, etc.
        """
        for op in ('wilco', 'state', 'info', 'report'):
            try:
                getattr(self, f"_handle_{op}")(message)
            except NotImplementedError:
                # TODO: plausibly some logging
                pass

    def _shutdown(self, exception: Optional[Exception] = None):
        """shut down the Station."""
        self.state = "shutdown" if exception is None else "crashed"
        self._log("beginning shutdown", category="exit")
        # clear outbox etc.
        for k in self.outboxes.keys():
            self.outboxes[k] = Mailbox([])
        self.actors, self.sensors = {}, {}
        for node in self.nodes:
            self.shutdown_node(node['name'], "stop")
        waiting, unwait = timeout_factory(timeout=30)
        # make sure every node is shut down, timing out at 30s
        self._check_nodes()
        while any(
            n['inferred_status'] not in ('missing', 'shutdown', 'crashed')
            for n in self.nodes
        ):
            try:
                waiting()
            except TimeoutError:
                break
            time.sleep(0.1)
            self._check_nodes()
        unwait()
        # wait another moment to get the exit reports for sure
        # TODO: make this cleaner
        time.sleep(5)
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
            self._check_nodes()
            if self.tendtime() > self.poll * 30:
                crashed_threads = self.server.tend()
                # heuristically manage inbox size
                self.inbox.prune(self.max_inbox_mb)
                self.reset_tend()
                if len(crashed_threads) > 0:
                    self._log(crashed_threads, category="server_errors")
            time.sleep(self.poll)

    def _check_nodes(self):
        now = dt.datetime.now(tz=dt.timezone.utc)
        for n in self.nodes:
            if n['reported_status'] in ('shutdown', 'crashed'):
                n['inferred_status'] = n['reported_status']
                continue
            if n['reported_status'] == 'initializing':
                n['wait_time'] = (now - n['init_time']).total_seconds()
            else:
                n['wait_time'] = (now - n['last_seen']).total_seconds()
            # adding 5 seconds here as grace for network lag spikes
            if n['wait_time'] > 10 * n['update_interval'] + 5:
                n['inferred_status'] = 'missing'
            elif n['wait_time'] > 3 * n['update_interval']:
                n['inferred_status'] = 'delayed'
            else:
                n['inferred_status'] = n['reported_status']
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
        self, nodename
    ) -> tuple[Optional[Mailbox], Optional[pro.Instruction], Optional[int]]:
        """
        pick outgoing message, if one exists for this node.
        helper function for _ackcheck().
        """
        # TODO, probably: send more than one Instruction when available.
        #  we might want a special control code for that.
        box = self.outboxes[nodename]
        # TODO, maybe: this search will be expensive if outboxes get really big
        messages = tuple(
            filter(lambda pm: pm[1].sent is False, enumerate(box))
        )
        if len(messages) == 0:
            return None, None, None  # this will trigger an empty ack message
        # ensure that we send config Instructions before do Instructions
        # (the do instructions might need correct config to work!)
        try:
            pos, msg = filtern(lambda pm: pm[1].type == 'configure', messages)
        except StopIteration:
            pos, msg = messages[0]
        return box, msg, pos

    def _update_task_record(self, msg):
        """
        helper function for _ackcheck(). update task record associated with a
        'do' instruction so that we know we sent it.
        """
        task_record = self.tasks[msg.id]
        task_record['sent_time'] = dt.datetime.now(tz=dt.timezone.utc)
        task_record['status'] = 'sent'

    def _ackcheck(self, _conn: socket.socket, comm: dict):
        """
        callback for interpreting comms and responding as appropriate.
        should only be called inline of the ack() method of the Station's
        server attribute (a talkie.TCPTalk object).
        """
        # TODO: lockout might be too strict
        self.locked = True
        try:
            if comm["err"]:
                # TODO: log this and send did-not-understand
                return make_comm(b""), "notified sender of error"
            incoming = comm["body"]
            try:
                nodename = incoming.nodeid.name
            except (AttributeError, ValueError):
                # TODO: send not-enough-info message
                return make_comm(b""), "notified sender not enough info"
            # interpret the comm here in case we want to immediately send a
            # response based on its contents (e.g., in a gPhoton 2-like
            # pipeline that's mostly coordinating execution of a big list of
            # non-serial processes, we would want to immediately send
            # another task to any Node that tells us it's finished one)
            self._handle_incoming_message(comm["body"])
            if enum(incoming.state, "status") in ("shutdown", "crashed"):
                return None, "decline to send ack to terminated node"
            # if we have any Instructions for the Node -- including ones that
            # might have been added to the outbox in the
            # _handle_incoming_message() workflow -- pick one to send
            box, msg, pos = self._select_outgoing_message(nodename)
            # ...and if we don't have any, send empty ack comm
            if msg is None:
                return make_comm(b""), "sent ack"
            # log message and, if relevant, update task queue
            # TODO, maybe: logging and updating task queue here is much less
            #  complicated, but has the downside that it will occur _before_
            #  we confirm receipt.
            # this type should have been validated earlier in queue_task
            if msg['type'] == 'do':
                self._update_task_record(msg)
            self._record_message(box, msg, pos)
            return box[pos].comm, f"sent instruction {box[pos].id}"
        # TODO: log and handle errors in some kind of graceful way
        except Exception as ex:
            raise ex
        finally:
            self.locked = False

    @staticmethod
    def _launch_node_in_subprocess(context, kwargs):
        """component function for launch_node"""
        endpoint = generic_python_endpoint(
            "hostess.station.nodes",
            "launch_node",
            payload=kwargs,
            argument_unpacking='**',
            print_result=True
        )
        if context == 'daemon':
            output = RunCommand(endpoint, _disown=True)()
        elif context == 'subprocess':
            output = RunCommand(endpoint, _asynchronous=True)()
        else:
            raise ValueError(
                f"unsupported context {context}. Supported contexts are "
                f"'daemon', 'subprocess', and 'local'."
            )
        return output

    def launch_node(
        self,
        name,
        elements=(),
        host="localhost",
        update_interval=0.1,
        context='daemon',
        **kwargs
    ):
        """
        launch a node, by default daemonized, and add it to the nodelist.
        may also launch locally or in a non-daemonized subprocess.
        """
        # TODO: option to specify remote host and run this using SSH
        if host != "localhost":
            raise NotImplementedError
        # TODO: some facility for relaunching
        if any(n['name'] == name for n in self.nodes):
            raise ValueError("can't launch a node with a duplicate name")
        kwargs = {
            'station_address': (self.host, self.port),
            'name': name,
            'elements': elements,
            'update_interval': update_interval
        } | kwargs
        nodeinfo = blank_nodeinfo() | {
            'name': name,
            'inferred_status': 'initializing',
            'update_interval': update_interval
        }
        if context == 'local':
            # mostly for debugging / dev purposes
            from hostess.station.nodes import launch_node

            output = launch_node(is_local=True, **kwargs)
        else:
            output = self._launch_node_in_subprocess(context, kwargs)
        self.nodes.append(nodeinfo)
        return output


def blank_nodeinfo():
    return {
        'last_seen': None,
        'reported_status': 'initializing',
        'init_time': dt.datetime.now(dt.timezone.utc),
        'wait_time': 0,
        'running': [],
        'interface': {},
        'actors': [],
    }
