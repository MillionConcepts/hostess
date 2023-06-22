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
        _is_process_owner = False
    ):
        super().__init__(
            host=host,
            port=port,
            name=name,
            n_threads=n_threads,
            can_receive=True,
        )
        self.max_inbox_mb = max_inbox_mb
        self.events, self.nodes = [], []
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

    def _handle_incoming_message(self, message: pro.Update):
        """
        handle an incoming message. right now just wraps _handle_info() but
        will eventually also deal with node state tracking, logging, etc.
        """
        # TODO, maybe: log acknowledgments
        for method in ("_handle_info", "_handle_report", "_handle_state"):
            try:
                getattr(self, method)(message)
            except NotImplementedError:
                # TODO: plausibly some logging
                pass

    def _shutdown(self, exception: Optional[Exception] = None):
        """shut down the Station."""
        self._log("beginning shutdown", category="exit")
        # clear outbox etc.
        for k in self.outboxes.keys():
            self.outboxes[k] = Mailbox([])
        self.actors, self.sensors = {}, {}
        for node in self.nodes:
            self.shutdown_node(node['name'], "stop")
        # TODO: wait to make sure they are received based on state tracking,
        #  this is a placeholder
        waiting, _ = timeout_factory(timeout=5)
        while len(self.outboxes) > 0:
            try:
                waiting()
            except TimeoutError:
                break
            time.sleep(0.1)
        # shut down the server etc.
        # TODO: this is a little messy because of the discrepancy in thread
        #  and signal names. maybe unify this somehow.
        for k in self.signals:
            self.signals[k] = 1
        self.server.kill()
        if exception is not None:
            self._log(
                exc_report(exception, 0), status="crashed", category="exit"
            )
        else:
            self._log("exiting", status="graceful", category="exit")
        self.state = "stopped"
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
                return make_comm(b""), "notified sender of error"
            message = comm["body"]
            try:
                nodename = message.nodeid.name
            except (AttributeError, ValueError):
                # TODO: send not-enough-info message
                return make_comm(b""), "notified sender not enough info"
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
            box = self.outboxes[nodename]
            # TODO, maybe: kind of expensive if these get really big
            candidates = tuple(
                filter(lambda pm: pm[1].sent is False, enumerate(box))
            )
            if len(candidates) == 0:  # no queued messages
                return make_comm(b""), "sent ack"
            # ensure that config Instructions are sent before do Instructions
            # (the do instructions might need correct config to work!)
            try:
                pos, msg = filtern(
                    lambda pm: pm[1].type == 'configure', candidates
                )
            except StopIteration:
                pos, msg = candidates[0]
            update_instruction_timestamp(msg.message)
            # make new Msg object w/updated timestamp.
            # this is weird-looking, but, by intent, Msg object cached
            # properties are essentially immutable wrt the underlying message
            box[pos] = Msg(msg.message)
            # TODO: logging this here is much less complicated, but has the
            #  downside that it will occur _before_ we confirm receipt.
            self._log(box[pos].message, category="comms", direction="send")
            box[pos].sent = True
            return box[pos].comm, f"sent instruction {box[pos].id}"
        # TODO: handle this in some kind of graceful way
        except Exception as ex:
            raise ex
        finally:
            self.locked = False

    def handlers(self) -> list[dict]:
        """
        TODO: this wants to be a _lot_ more sophisticated. it should do
            something dynamic based on our tracking of what actions each node
            can perform.
        list the nodes we've internally designated as handlers.
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

    def launch_node(
        self,
        name,
        elements=(),
        host="localhost",
        update_interval=0.1,
        **kwargs
    ):
        # TODO: some facility for relaunching
        if any(n['name'] == name for n in self.nodes):
            raise ValueError("can't launch a node with a duplicate name")
        kwargs = {
            'station_address': (self.host, self.port),
            'name': name,
            'elements': elements,
            'update_interval': update_interval
        } | kwargs
        endpoint = generic_python_endpoint(
            "hostess.station.nodes",
            "launch_node",
            payload=kwargs,
            argument_unpacking='**',
            print_result=True
        )
        # TODO: option to specify remote host and run this using SSH
        if host != "localhost":
            raise NotImplementedError
        nodeinfo = blank_nodeinfo() | {
            'name': name,
            'inferred_status': 'initializing',
            'update_interval': update_interval
        }
        RunCommand(endpoint, _disown=True)()
        self.nodes.append(nodeinfo)


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
