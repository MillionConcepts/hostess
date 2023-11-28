from __future__ import annotations

from collections import defaultdict
import datetime as dt
from pathlib import Path
import socket
import sys
import time
from typing import Any, Literal, Mapping, Optional, Sequence, Union

from dustgoggles.dynamic import exc_report
from dustgoggles.func import gmap, filtern

from hostess.caller import generic_python_endpoint
from hostess.station import bases
from hostess.station.bases import NoMatchingDelegate
from hostess.station.comm import make_comm
from hostess.station.messages import (
    Mailbox,
    make_instruction,
    Msg,
    pack_obj,
    unpack_message,
    unpack_obj,
    update_instruction_timestamp,
)
import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import enum
from hostess.subutils import RunCommand
from hostess.station.talkie import timeout_factory


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
        logdir: Path = Path(__file__).parent / ".nodelogs",
        _is_process_owner: bool = False,
        **kwargs: Union[bool, tuple[Union[type[bases.Sensor], type[bases.Actor]]], float, int],
    ):
        """
        Args:
            host: hostname or address for Station. usually should be your
                external IP address for remote connections and 'localhost'
                for local use.
            port: port this Station's TCP server will listen on.
            name: name for Station.
            n_threads: how many threads should the Station use?
            max_inbox_mb: how large can the Station's inbox get, in MB, before
                it dumps older Messages?
            logdir: where should the Station write logs?
            _is_process_owner: if True, the Station will attempt to stop the
                Python interpreter when it shuts down.
            **kwargs: additional kwargs for the Node constructor (see Node
                documentation for valid options).
        """
        super().__init__(
            host=host,
            port=port,
            name=name,
            n_threads=n_threads,
            can_receive=True,
            logdir=logdir,
            **kwargs,
        )
        self.max_inbox_mb = max_inbox_mb
        self.events, self.delegates, self.relaunched, self.tasks = (
            [],
            [],
            [],
            {},
        )
        self.outboxes = defaultdict(Mailbox)
        self.tendtime, self.reset_tend = timeout_factory(False)
        self.last_handler = None
        self.__is_process_owner = _is_process_owner
        self._log("completed initialization", category="system")

    def _set_logfile(self):
        """create standardized log file name."""
        self.logfile = Path(
            self.logdir,
            f"{self.init_time}_station_{self.host}_{self.port}.log"
        )

    def set_delegate_properties(self, delegate: str, **propvals: Any):
        """
        Construct a 'configure' Instruction for a Delegate that instructs it
            to assign specific values to named properties of itself; put that
            Instruction in the outbox for that Delegate.

        Args:
            delegate: name of delegate to configure.
            propvals: argument names correspond to property names of target Delegate;
                argument values will be serialized as PythonObject Messages and then
                bundled into ConfigParam Messages.
        """
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

    def set_delegate_config(self, delegate: str, config: Mapping):
        """
        Construct a 'configure' Instruction for a Delegate that instructs it
            to merge a config dict into its existing config dict; put that
            Instruction in the outbox for that Delegate.

        Args:
            delegate: name of Delegate to configure
            config: configuration to add to the Delegate's config dict.
        """
        config = pro.ConfigParam(
            paramtype="config_dict", value=pack_obj(config)
        )
        self.outboxes[delegate].append(
            make_instruction("configure", config=config)
        )

    def queue_task(self, delegate: str, instruction: pro.Instruction):
        """
        queue an Instruction for a Delegate and set up tracking for its state.
        this method is intended for "do" Instructions that contain Actions, not
        Instructions we do not want to track in the same way (like config).
        The default InstructionFromInfo Actor uses this method to queue the
        Instructions it makes.

        Args:
            delegate: name of Delegate for which to queue task
            instruction: Instruction Message
        """
        if instruction.HasField("pipe"):
            raise NotImplementedError("multipart pipelines not implemented")
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
            "exception": None,
        }
        self.outboxes[delegate].append(instruction)

    def shutdown_delegate(
        self, delegate: str, how: Literal["stop", "kill"] = "stop"
    ):
        """
        make a shutdown Instruction and queue it in the outbox for the
        specified Delegate.

        Args:
            delegate: name of Delegate we would like to shut down
            how: "stop" if we would like the Delegate to shut down gracefully;
                "kill" if we would like the Delegate to stop immediately no
                matter what.
        """
        self.outboxes[delegate].append(make_instruction(how))

    # TODO, maybe: signatures on these match-and-execute things are getting
    #  a little weird and specialized. maybe that's ok, but maybe we should
    #  make a more unified interface.
    def match_and_execute(self, obj: Any, category: str):
        """
        Check to see if we have an Actor or Actors intended to handle `obj`
        by calling their `match()` methods with `obj`. If any of them say they
        can deal with `obj`, pass `obj` to their `execute()` methods.

        In a typical Station application, `obj` will be something a Delegate
        packed into a "completion" or "info" Update. Its type will be entirely
        application-dependent.

        Args:
            obj: object one of our Actors might be able to work with
            category: what category of Actor might be able to work with `obj`?
        """
        try:
            actors = self.match(obj, category)
        except bases.NoActorForEvent:
            # TODO: _plausibly_ log this?
            return
        except (AttributeError, KeyError) as ex:
            self._log("match crash", **exc_report(ex), category=category)
            return
        self._log(
            obj,
            category=category,
            matches=[a.name for a in actors],
        )
        for actor in actors:
            try:
                actor.execute(self, obj)
            except NoMatchingDelegate:
                self._log(
                    "no delegate for action", actor=actor, category=category
                )
            except Exception as ex:
                self._log(
                    "execution failure",
                    actor=actor,
                    category=category,
                    exception=ex
                )

    def _handle_info(self, message: pro.Update):
        """
        Handler for 'info' Updates. Unpack all the
        "notes" (serialized Python objects + metadata) bundled in an info
        Update and match each one against our Actors, executing each Actor
        that matches a note with that note.

        If `message` is an exit report, also always log its info.

        Args:
            message: Update Message from a Delegate.
        """
        notes = gmap(unpack_obj, message.info)
        if enum(message, 'reason') == 'exiting':
            self._log(
                "received exit report",
                delname=message.delegateid.name,
                reason=enum(message.state, 'status'),
                exception=notes
            )
            return
        for note in notes:
            self.match_and_execute(note, "info")

    def _handle_report(self, update: pro.Update):
        """
        Handler for "completion" Updates received from Delegates. Perform
        action tracking and cleanup, and execute any appropriate follow-on
        actions.

        Args:
            update: 'completion' Update from Delegate.
        """
        if not update.HasField("completed"):
            return
        if len(update.completed.steps) > 0:
            raise NotImplementedError
        self._log(update, category="report")
        if not update.completed.HasField("action"):
            # TODO: an unusual case. maybe log
            return
        completed = unpack_message(update.completed)
        try:
            task = self.tasks[completed["instruction_id"]]
            task["status"] = completed["action"]["status"]
            task["start_time"] = completed["action"]["time"]["start"]
            task["end_time"] = completed["action"]["time"]["end"]
            task["duration"] = completed["action"]["time"]["duration"]
            if task["exception"] is not None:
                task["exception"] = exc_report(task["exception"])
        except KeyError:
            # TODO: an undesirable case, log
            pass
        obj = unpack_obj(update.completed.action.result)
        self.match_and_execute(obj, "completion")

    def get_delegate(self, name: str) -> dict[str, Any]:
        """
        return delegate info structure for first Delegate named "name"
        (there should never be more than one unless someone has seriously
        messed with the Station).

        Args:
            name: name of Delegate

        Returns:
            info `dict` for Delegate. If Delegate is running locally,
                this `dict` will include a reference to the Delegate itself.
        """
        return [n for n in self.delegates if n["name"] == name][0]

    def _handle_state(self, update: pro.Update):
        """
        update the info dict for one of our Delegates in response to state
        and id elements of an Update Message.

        Args:
            update: Update Message from Delegate.
        """
        try:
            delegate = self.get_delegate(update.delegateid.name)
        # TODO: how do we handle mystery-appearing delegates with dupe names
        except IndexError:
            delegate = blank_delegateinfo()
            self.delegates.append(delegate)
        update = unpack_message(update)
        if delegate['reported_status'] == 'no_report':
            self._log(
                "first message from delegate",
                delname=delegate['name'],
                category="comms",
                direction="recv"
            )
        delegate |= {
            "last_seen": dt.datetime.fromisoformat(update["time"]),
            "wait_time": 0,
            "interface": update["state"]["interface"],
            "cdict": update["state"]["cdict"],
            "reported_status": update["state"]["status"],
            "pid": update["delegateid"]["pid"],
            "actors": update["state"].get("actors", []),
            "sensors": update["state"].get("sensors", []),
            "busy": update["state"]["busy"],
            "host": update["delegateid"]["host"],
            "running": update.get("running", []),
            "infocount": update["state"].get("infocount", {}),
            "init_params": update["state"]["init_params"],
        }
        for name, state in update["state"]["threads"].items():
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

    def _handle_wilco(self, update: pro.Update):
        """
        handle 'wilco' Update from a Delegate. Delegates send these on receipt
        of Instructions prior to actually performing them. Record time and
        whether or not the Delegate acknowledged  that it understood and would
        follow the Instruction.

        Args:
            update: 'wilco' Update Message from Delegate.
        """
        # TODO: handle do not understand messages
        if not enum(update, "reason") in ("wilco", "bad_request"):
            return
        # TODO: handle config acks
        if update.instruction_id not in self.tasks.keys():
            return
        task = self.tasks[update.instruction_id]
        task["ack_time"] = update.time.ToDatetime(dt.timezone.utc)
        task["status"] = enum(update, "reason")
        # TODO: behavior in response to bad_request notifications

    def _handle_incoming_message(self, update: pro.Update):
        """
        Top-level dispatcher function for handling Updates from Delegates.
        Route wilco, state, info, and report components of Update -- if any --
        to the appropriate methods.

        Args:
            update: Update Message from Delegate.
        """
        for op in ("wilco", "state", "info", "report"):
            try:
                getattr(self, f"_handle_{op}")(update)
            except Exception as ex:
                self._log(
                    "bad message handling",
                    category="comms",
                    direction="recv",
                    exception=ex,
                    op=op
                )

    @property
    def running_delegates(self) -> list[dict[str, Any]]:
        """get metadata dicts for all still-running Delegates."""
        return [
            n for n in self.delegates
            if n['inferred_status'] not in ('missing', 'shutdown', 'crashed')
        ]

    @property
    def unfinished_delegates(self) -> list[dict[str, Any]]:
        """
        get metadata dicts for all still-running Delegates executing in local
        context (this Station's process).
        """
        unfinished = []
        for n in filter(lambda x: "obj" in x, self.delegates):
            if any(map(lambda t: t.running(), n["obj"].threads.values())):
                unfinished.append(n)
        return unfinished

    def _shutdown(self, exception: Optional[Exception] = None):
        """
        shut down the Station. This method should normally be called only by
        `Station.shutdown()`.

        Args:
            exception: unhandled Exception that caused shutdown, if any.
        """
        # TODO: add some internal logging here when nodes fail to
        #  shut down or respond in a timely fashion
        self.exception = exception
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
        while len(self.running_delegates) > 0:
            try:
                waiting()
            except TimeoutError:
                self._log(
                    "delegate_shutdown_timeout",
                    category="system",
                    running=[n['name'] for n in self.running_delegates]
                )
                break
            time.sleep(0.1)
            self._check_delegates()
        unwait()
        # ensure local delegate threads are totally shut down
        while len(self.unfinished_delegates) > 0:
            time.sleep(0.1)
            try:
                waiting()
            except TimeoutError:
                self._log(
                    "local_delegate_thread_shutdown_timeout",
                    category="system",
                    running=[n['name'] for n in self.unfinished_delegates]
                )
                break
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
                self._log("self_server_shutdown_timeout", category="system")
                break
            time.sleep(0.1)
        if self.__is_process_owner:
            self._log("shutdown complete, exiting process", category='system')
            sys.exit()

    def _check_delegates(self):
        """
        Update time-based elements of metadata dicts for our Delegates.
        Crucial step of Station's main loop. Should only be called by
        `Station._main_loop()`.
        """
        now = dt.datetime.now(tz=dt.timezone.utc)
        for n in self.delegates:
            # shared kwargs for those status changes we want to log
            lkwargs = {
                'event': 'delegate_status',
                'category': 'system',
                'name': n['name']
            }
            if n["reported_status"] in ("shutdown", "crashed"):
                if n["reported_status"] != n['inferred_status']:
                    self._log(status=n['reported_status'], **lkwargs)
                n["inferred_status"] = n["reported_status"]
                continue
            if n["reported_status"] == "no_report":
                n["wait_time"] = (now - n["init_time"]).total_seconds()
            else:
                n["wait_time"] = (now - n["last_seen"]).total_seconds()
            # adding 5 seconds here as grace for network lag spikes
            if n["wait_time"] > 10 * n["update_interval"] + 5:
                if n['inferred_status'] != 'missing':
                    self._log(status='missing', **lkwargs)
                n["inferred_status"] = "missing"
            elif n["wait_time"] > 3 * n["update_interval"]:
                # don't care about logging delays
                n["inferred_status"] = "delayed"
            else:
                n["inferred_status"] = n["reported_status"]
            # TODO: trigger some behavior

    def _main_loop(self):
        """
        launch main loop for Station. This should only be called by
        `Station._start()`, which should only be called by `Station.start()`.
        """
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

    def _record_message(self, box: Mailbox, msg: Msg, pos: int):
        """
        helper function for `Station._ackcheck()`. write log entry for sent
        Message and record it as sent.

        Args:
            box: outbox for Delegate we sent Message in msg to
            msg: wrappper for Message we sent to delegate
            pos: index of msg within box
        """
        # make new Msg object w/updated timestamp.
        # this is weird-looking, but, by intent, Msg object cached
        # properties are essentially immutable wrt the underlying message
        update_instruction_timestamp(msg.message)
        box[pos] = Msg(msg.message)
        self._log(box[pos].message, category="comms", direction="send")
        box[pos].sent = True

    def _select_outgoing_message(
        self, delegate: str
    ) -> tuple[Optional[Mailbox], Optional[pro.Instruction], Optional[int]]:
        """
        Helper function for `Station._ackcheck()`. When we receive an Update
        from one of our Delegates and we've got one or more Instructions
        prepared for them, we reply with one. This function checks if we have
        any Instructions for that Delegate, and, if so, picks which we should
        send.

        Args:
            delegate: name of Delegate we're talking to.

        Returns:
            * outbox for `delegate`, or None if we don't have any
                Instructions for it
            * selected Instruction for Delegate, or None if we don't have any
                to send
            * index of Instruction within outbox, or None if there isn't one
        """
        # TODO, probably: send more than one Instruction when available.
        #  we might want a special control code for that.
        box = self.outboxes[delegate]
        # TODO, maybe: this search will be expensive if outboxes get really big
        #  -- might want some kind of hashing
        messages = tuple(
            filter(lambda pm: pm[1].sent is False, tuple(enumerate(box)))
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

    # TODO: is this always a Msg or sometimes a Message?
    def _update_task_record(self, msg: Msg):
        """
        helper function for _ackcheck(). update task record associated with a
        'do' Instruction so that we know that we sent it and when.

        Args:
            msg: wrapper for 'do' Instruction we just sent to a Delegate.
        """
        task_record = self.tasks[msg.id]
        task_record["sent_time"] = dt.datetime.now(tz=dt.timezone.utc)
        task_record["status"] = "sent"

    def _ackcheck(self, _conn: socket.socket, comm: dict) -> tuple[bytes, str]:
        """
        callback for interpreting comms and responding as appropriate.
        should only be called inline of the `ack()` method of the Station's
        `server` attribute (a `talkie.TCPTalk` object).

        Args:
            _conn: open `socket.socket` object
            comm: decoded hostess comm as produced by `read_comm()`

        Returns:
            response: response to comm
            description: loggable description of response type / status
        """
        # TODO: lockout might be too strict
        msg, self.locked = None, True
        try:
            if comm["body"] == b"situation":
                if self.state in ("shutdown", "crashed"):
                    return make_comm(b"shutting down"), "sent shutdown notice"
                return self._situation_comm(), "sent situation"
            if comm["err"]:
                self._log(
                    "failed to decode",
                    category="comms",
                    direction="recv",
                    conn=_conn
                )
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
            self._log(exc_report(ex), category="comms")
            return b"response failure", "failed to respond"
        finally:
            self.locked = False

    @staticmethod
    def _launch_delegate_in_subprocess(
        context: DelegateContext, kwargs: dict[str, Any]
    ):
        """
        Launch a delegate in a child or daemonized process. Should only be
        called by `Station.launch_delegate()`.

        Args:
            context: how to launch the delegate. Should only be 'daemon' or
                'subprocess'; if 'local', we should never have gotten here.
            kwargs: kwargs for launch, passed to `launch_delegate()` via
                `generic_python_endpoint()`
        """
        endpoint = generic_python_endpoint(
            "hostess.station.delegates",
            "launch_delegate",
            payload=kwargs,
            splat="**",
            print_result=True,
        )
        if context == "daemon":
            RunCommand(endpoint, _disown=True)()
        elif context == "subprocess":
            RunCommand(endpoint, _asynchronous=True)()
        else:
            raise ValueError(
                f"unsupported context {context}. Supported contexts for this "
                f"function are 'daemon' and 'subprocess'"
            )

    def launch_delegate(
        self,
        name: str,
        elements: Sequence[tuple[str, str]] = (),
        host: str = "localhost",
        update_interval: float = 0.25,
        context: DelegateContext = "daemon",
        **kwargs: Union[int, float],
    ) -> Optional[bases.Node]:
        """
        launch a Delegate, by default daemonized. prepare a metadata dict for
        it, and prepare ourselves to receive Messages from it.

        Args:
            name: name to assign to Delegate.
            elements: sequence of (module_name, class_name) describing Actors
                and Sensors Delegate should construct and attach to itself at
                launch.
            host: hostname or ip on which Delegate should launch. remote
                launch is not yet implemented.
            update_interval: how frequently the Delegate should send
                unprompted 'heartbeat' Updates to this Station.
            context: where to launch the Delegate in relation to this Station's
                interpreter process: "local" to run threaded in the same
                process; "subprocess" to run in a child process; "daemon" to
                run in a fully-detached process.
            kwargs: additional kwargs to pass to
                `hostess.station.delegates.launch_delegate()`.

        Returns:
            None if `context` == "subprocess" or "daemon"; the launched
                Delegate if `context` == "local".
        """
        if not self._Node__started:
            raise ValueError(
                "cannot launch delegates from an unstarted Station"
            )
        # TODO: option to specify remote host and run this using SSH (update
        #  relaunch_delegate as well when adding this feature)
        if host != "localhost":
            raise NotImplementedError
        if any(n["name"] == name for n in self.delegates):
            raise ValueError("can't launch a delegate with a duplicate name")
        kwargs = {
                 "station_address": (self.host, self.port),
                 "name": name,
                 "elements": elements,
                 "update_interval": update_interval,
                 "loginfo": {
                     # must pass logdir as a string -- delegate is not
                     # initialized yet, so this is inserted directly into
                     # generated source code
                     'logdir': str(self.logdir),
                     'init_time': self.init_time
                 }
             } | kwargs
        delegateinfo = blank_delegateinfo() | {
            "name": name,
            "inferred_status": "initializing",
            "update_interval": update_interval,
        }
        # kwargs for logging launch
        lkwargs = {'delname': name, 'elements': elements, 'category': 'system'}
        self._log("init delegate launch", **lkwargs)
        try:
            if context == "local":
                # mostly for debugging / dev purposes
                from hostess.station.delegates import launch_delegate
                output = launch_delegate(is_local=True, **kwargs)
                delegateinfo["obj"] = output
            else:
                output = self._launch_delegate_in_subprocess(context, kwargs)
            self.delegates.append(delegateinfo)
            self._log("launched delegate", **lkwargs)
            return output
        except Exception as ex:
            self._log(
                'delegate launch fail', **lkwargs, **exc_report(ex)
            )

    def relaunch_delegate(self, name: str):
        """
        Relaunch an existing Delegate with the same initialization settings,
        although not full runtime configuration. If it's still running, shut
        it down first.

        Args:
            name: name of Delegate to relaunch.
        """
        delegate = self.get_delegate(name)
        if delegate["inferred_status"] == "missing":
            pass  # TODO: os level kill
        elif delegate["inferred_status"] not in ["shutdown", "crashed"]:
            self.shutdown_delegate(name, "stop")
            waiting, unwait = timeout_factory(timeout=20)
            self._check_delegates()
            while self.get_delegate(name)["inferred_status"] not in (
                "shutdown",
                "crashed",
            ):
                try:
                    waiting()
                except TimeoutError:
                    break
                time.sleep(0.1)
                self._check_delegates()
            unwait()
        elements = []
        elements_dict = dict(delegate["actors"]) | dict(delegate["sensors"])
        for k in elements_dict.keys():
            cls = elements_dict[k].split(".")[-1]
            mod = elements_dict[k].removesuffix["." + cls]
            elements = elements + [(mod, cls)]
        elements = tuple(elements)
        host = "localhost"
        # TODO: add remote host detection/relaunch capability
        if delegate["init_params"]["_is_process_owner"]:
            context = "daemon"
            # TODO: how do you know if it's "daemon" vs "subprocess"?
        else:
            context = "local"
        self.delegates.remove(delegate)
        self.relaunched.append(delegate)
        self.launch_delegate(
            name,
            elements,
            host=host,
            context=context,
            **delegate["init_params"],
        )

    def save_port_to_shared_memory(self, address: Optional[str] = None):
        """
        write this Station's port number to a shared memory address, allowing
        other applications to query or monitor it. Specifically, calling this
        function will allow the hostess `situation` app to automatically find
        this Station.

        Args:
            address: shared memory address to write port to. Exactly what this
                means depends on the operating environment. In CPython on
                Linux, it denotes a filename in /dev/shm. if not specified,
                defaults to the name of this Station.
        """
        from dustgoggles.codex.implements import Sticky
        from dustgoggles.codex.memutilz import (
            deactivate_shared_memory_resource_tracker,
        )

        deactivate_shared_memory_resource_tracker()
        address = self.name if address is None else address
        Sticky.note(
            self.port, address=f"{address}-port-report", cleanup_on_exit=True
        )

    def _situation_comm(self) -> bytes:
        """
        Construct a hostess comm describing this Station's overall situation.
        The hostess `situation` app works by constructing human-readable
        representations of these comms.

        """
        from hostess.station.situation.response_organizers import situation_of

        return make_comm(pack_obj(situation_of(self)))


def blank_delegateinfo() -> dict[
    str, Union[None, str, dt.datetime, list, int, dict]
]:
    """
    utility function for Station. creates an empty Delegate metadata dict.

     Returns:
        "blank" Delegate metadata dict suitable for use as an element of
            Station.delegates.
    """
    return {
        "last_seen": None,
        "reported_status": "no_report",
        "inferred_status": "initializing",
        "init_time": dt.datetime.now(dt.timezone.utc),
        "wait_time": 0,
        "running": [],
        "interface": {},
        "actors": {},
    }


def get_port_from_shared_memory(memory_address: str = "station") -> int:
    """
    fetch a named Station's port number from a shared memory address. Used
    by the hostess `situation` app; can also be used by other 'plugins' or
    for ad-hoc inspection of a Station.

    Args:
        memory_address: shared memory address a Station's port number might be
            stored in. Exactly what this means depends on the environment. In
            CPython on Linux, it denotes a filename in /dev/shm.

    Returns:
        port number of Station that saved its port number to `memory_address`.
    """
    from dustgoggles.codex.implements import Sticky
    from dustgoggles.codex.memutilz import (
        deactivate_shared_memory_resource_tracker,
    )

    deactivate_shared_memory_resource_tracker()
    if (port := Sticky(f"{memory_address}-port-report").read()) is None:
        raise FileNotFoundError("no port at address")
    return port


DelegateContext: Literal["local", "subprocess", "daemon"]
"""
code denoting the relationship between a Delegate's execution context and its
supervising Station's interpreter process. "local" means that the Delegate is
(or will be) running in separate threads of the same process; "subprocess"
means a child process of the Station's interpreter process; "daemon" means
a double-forked, fully-disowned and separate process.
"""
