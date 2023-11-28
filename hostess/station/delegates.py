from __future__ import annotations

from collections import defaultdict
from itertools import count
from pathlib import Path
import random
import sys
import time
from types import MappingProxyType as MPt, ModuleType
from typing import Union, Literal, Optional, Type, Any, Mapping, Hashable, \
    Sequence

from dustgoggles.dynamic import exc_report
from dustgoggles.structures import rmerge
from google.protobuf.message import Message

from hostess.station import bases
from hostess.station.bases import Sensor, Actor, ConsumedAttributeError
from hostess.station.comm import read_comm
from hostess.station.messages import pack_obj, task_msg, unpack_obj
from hostess.station.proto_utils import make_timestamp, enum
import hostess.station.proto.station_pb2 as pro
from hostess.station.talkie import stsend, timeout_factory

ConfigParamType = Literal["config_property", "config_dict"]
GENERIC_LOGINFO = MPt(
    {'logdir': Path(__file__).parent / ".nodelogs"}
)


class Delegate(bases.Node):
    def __init__(
        self,
        station_address: tuple[str, int],
        name: str,
        elements: tuple[Union[type[bases.Sensor], type[bases.Actor]]] = (),
        n_threads: int = 4,
        poll: float = 0.08,
        timeout: int = 10,
        update_interval: float = 10,
        start: bool = False,
        loginfo: Optional[Mapping[str]] = MPt({}),
        _is_process_owner: bool = False
    ):
        """
        configurable remote processor for hostess network. can gather data
        and/or execute actions based on the elements attached to it. should
        typically be instantiated via the launch_delegate() method of the
        supervising Station.

        Args:
            station_address: (hostname, port) of supervising Station
            name: identifying name for delegate
            n_threads: max threads in executor
            elements: Sensors or Actors to add to delegate at creation.
            poll: delay, in seconds, for polling loops
            timeout: timeout, in s, for intra-hostess communications
            update_interval: interval, in s, for check-in Updates to
                supervising Station
        """
        super().__init__(
            name=name,
            n_threads=n_threads,
            elements=elements,
            start=start,
            poll=poll,
            timeout=timeout,
            _is_process_owner=_is_process_owner,
            logdir=loginfo.get('logdir', GENERIC_LOGINFO['logdir']),
            loginfo=loginfo,
            station=station_address
        )
        self.update_interval = update_interval
        self.actionable_events, self.infocount = [], defaultdict(int)
        self.actions = {}
        self.instruction_queue = []
        self.update_timer, self.reset_update_timer = timeout_factory(False)
        # TODO: add local hostname of delegate
        self.init_params = {
            "n_threads": n_threads,
            "poll": poll,
            "timeout": timeout,
            "logdir": self.logdir,
            "logfile": self.logfile,
            "update_interval": update_interval,
            "_is_process_owner": _is_process_owner
        }

    def _set_logfile(self):
        """internal function to set path to log file."""
        self.logfile = Path(
            self.logdir,
            f"{self.loginfo.get('init_time', self.init_time)}_{self.name}_"
            f"{self.station[0]}_{self.station[1]}.log"
        )

    def _sensor_loop(
        self, sensor: bases.Sensor
    ) -> dict[str, Union[str, Optional[int], Optional[Exception]]]:
        """
        continuously check a Sensor. this function must be launched in its
        own thread or it will block and be useless. NOTE: should only be
        called from _start().

        Args:
            sensor: Sensor to poll.

        Returns:
            dict with keys:
                name: name of sensor
                signal: signal sent to terminate this function (if any)
                exception: exception that terminated this function (if any)
        """
        exception = None
        try:
            while self.signals.get(sensor.name) is None:
                # noinspection PyPropertyAccess
                if not self.locked:
                    sensor.check(self)
                time.sleep(sensor.poll)
        except Exception as ex:
            exception = ex
        finally:
            sensor.close()
            return {
                "name": sensor.name,
                "signal": self.signals.get(sensor.name),
                "exception": exception,
            }

    def check_on_action(
        self, instruction_id: int
    ) -> tuple[Optional[Exception], bool]:
        """
        check whether one of this delegate's Actions completed. if it crashed,
        set its status and exception keys appropriately in this delegate's
        `actions` dict. typically called as part of the main Delegate loop,
        specifically from _check_on_actions().

        Args:
            instruction_id: numerical identifier of Action to check.

        Returns:
            exception: None if the Action terminated successfully or hasn't yet
                terminated; the Exception the Action raised if it didn't
                terminate successfully.
            done: True if the Action has terminated; False if not.
        """
        try:
            self.threads[f"Instruction_{instruction_id}"].result(0)
        except TimeoutError:
            return None, True
        except Exception as ex:
            # action crashed without setting its status as such
            self.actions[instruction_id]["status"] = "crash"
            self.actions[instruction_id]['exception'] = ex
            return ex, False
        # an action wrapped in @reported will catch exceptions and do this
        # politely instead of crashing as above
        if self.actions[instruction_id].get('exception') is not None:
            self.actions[instruction_id]['status'] = 'crash'
            return self.actions[instruction_id]['exception'], False
        return None, False

    def _check_actions(self):
        """
        check running actions (threads launched as part of a 'do'
        instruction). if any have crashed or completed, log them and report
        them to the Station, then remove them from the thread cache.
        """
        acts_to_clean, threads_to_clean = [], []
        # this runs asynchronously so iterating over bare .items() is unstable
        items = tuple(self.actions.items())
        for instruction_id, action in items:
            # TODO: multistep "pipeline" case
            exception, running = self.check_on_action(instruction_id)
            if running is True:
                continue
            # TODO: accomplish this with a wrapper
            if exception is not None:
                self._log(
                    action,
                    exception=exception,
                    status="failed",
                    category="action"
                )
            else:
                self._log(action, status="completed", category="action")
            # TODO: determine if we should reset update timer here
            response = self._report_on_action(action)
            # TODO: error handling
            if response not in ("connection refused", "err", "timeout"):
                # i.e., try again later
                acts_to_clean.append(instruction_id)
                threads_to_clean.append(f"Instruction_{instruction_id}")
        for target in acts_to_clean:
            self.actions.pop(target)
        for target in threads_to_clean:
            self.threads.pop(target)

    def _send_info(self):
        """
        construct an Update based on everything in the actionable_events
        cache and send it to the Station, then clear actionable_events.
        """
        message = self._base_message(reason="info")
        # TODO: this might want to be more sophisticated
        # TODO: arbitrary max number
        max_notes, info = 5, []
        for i in range(len(self.actionable_events)):
            info.append(self.actionable_events.pop())
            if i == max_notes - 1:
                break
        self._log(
            "sending info", info=info, category="comms", direction="send"
        )
        message.MergeFrom(pro.Update(info=[pack_obj(i) for i in info]))
        response = self.talk_to_station(message)
        # TODO: perhaps there's a better way to track this 'outbox'...
        #  but I'd rather not do it with a mailbox object, I want it to be
        #  quicker/more ephemeral
        if response in ("err", "connection refused", "timeout"):
            self.actionable_events += info

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
        """
        internal shutdown handler.

        Args:
            exception: Exception that terminated Delegate's main loop, if any.
                should be None on a 'graceful' shutdown.
        """
        # divorce oneself from actors and acts, from events and instructions
        self.actions, self.actionable_events = {}, []
        # TODO, maybe: try to kill child processes (can't in general kill
        #  threads but sys.exit should handle it)
        # signal sensors to shut down
        for k in self.threads.keys():
            self.signals[k] = 1
        # goodbye to all that
        self.instruction_queue, self.actors, self.sensors = [], {}, {}
        try:
            self.threads['exit_report'] = self.exc.submit(
                self._send_exit_report, exception
            )
        except Exception as ex:
            self._log("exit report failed", exception=ex, category="system")
        # wait to send exit report
        if 'exit_report' in self.threads:
            while self.threads['exit_report'].running():
                time.sleep(0.1)

    def _send_exit_report(self, exception: Optional[Exception] = None):
        """
        send Update to Station informing it that Delegate is exiting (and why).

        Args:
            exception: unhandled Exception that caused Delegate's main loop
                to exit. None on intentional shutdown.
        """
        self.state = "crashed" if exception is not None else "shutdown"
        msg = self._base_message(reason='exiting')
        if exception is not None:
            try:
                info = pro.Update(
                    info=[pack_obj(exc_report(exception), "exception")]
                )
            except Exception as ex:
                info = pro.Update(
                    info=[pack_obj(exc_report(ex), "exception")]
                )
            msg.MergeFrom(info)
        self.talk_to_station(msg)

    def _report_on_action(self, action: dict):
        """
        report to Station on completed/failed action. should only be called as
        part of the main loop, specifically from _check_on_actions().

        Args:
            action: a value of this delegate's `actions` dict.
        """
        msg = self._base_message(
            completed=task_msg(action), reason="completion"
        )
        # TODO: multi-step case
        return self.talk_to_station(msg)

    def _check_in(self):
        """send heartbeat Update to the Station."""
        self.talk_to_station(self._base_message(reason="heartbeat"))
        self.reset_update_timer()

    def _match_task_instruction(
        self, instruction: pro.Instruction
    ) -> list[bases.Actor]:
        """
        wrapper for self.match that specifically checks for actors that can
        execute a task described in an Instruction.

        Args:
            instruction: Instruction containing a task.

        Returns:
            list of this Delegate's Actors that match the Instruction.

        Raises:
            NoActorForEvent: if none of this Delegate's Actors match.
        """
        try:
            return self.match(instruction, "action")
        except StopIteration:
            raise bases.NoActorForEvent(
                str(self.explain_match(instruction, "action"))
            )

    def _configure_from_instruction(self, instruction: Message):
        cp_for_log, cd_for_log = {}, {}
        for param in instruction.config:
            if enum(param, "paramtype") == "config_property":
                try:
                    unpacked = unpack_obj(param.value)
                    setattr(self, param.value.name, unpacked)
                    cp_for_log[param.value.name] = unpacked
                except ConsumedAttributeError as cae:
                    self._log(
                        "error setting interface property",
                        name=param.value.name,
                        category="system",
                        exception=cae
                    )
                    raise bases.DoNotUnderstand(
                        f"error setting property: {cae}"
                    )
                except AttributeError:
                    self._log(
                        "missing requested interface property",
                        name=param.value.name,
                        category="system",
                    )
                    raise bases.DoNotUnderstand(
                        f"no property {param.value.name}"
                    )
            elif enum(param, "paramtype") == "config_dict":
                unpacked = unpack_obj(param.value)
                self.cdict = rmerge(self.cdict, unpacked)
                cd_for_log |= unpacked
            else:
                raise bases.DoNotUnderstand("unknown ConfigParamType")
        self._log(
            "configured from instruction",
            category="system",
            configuration=(cp_for_log | cd_for_log)
        )

    def _handle_instruction(self, instruction: pro.Instruction):
        """
        interpret, reply to, and execute (if relevant) an Instruction. should
        only be called as part of the main loop.

        Args:
            instruction: Instruction received from Station.
        """
        status, err = "wilco", None
        try:
            bases.validate_instruction(instruction)
            # TODO: this might be too verbose
            self._log(
                "received instruction",
                content=instruction,
                category='comms',
                direction='recv'
            )
            if enum(instruction, "type") == "configure":
                self._configure_from_instruction(instruction)
            # TODO, maybe: different kill behavior.
            elif enum(instruction, "type") in ("stop", "kill"):
                # this occurs synchronously so move it to the finally block
                pass
            elif enum(instruction, "type") == "do":
                self.execute_do_instruction(instruction)
            else:
                raise bases.DoNotUnderstand(
                    f"unknown instruction type {enum(instruction, 'type')}"
                )
        except bases.DoNotUnderstand as dne:
            status = "bad_request"
            if enum(instruction, "type") == 'do':
                err = self.explain_match(instruction, "action")
            else:
                err = dne
        finally:
            # don't duplicate exit report behavior
            if enum(instruction, "type") in ("stop", "kill"):
                return self.shutdown()
            # otherwise send wilco or bad_request reply
            # noinspection PyTypeChecker
            self._reply_to_instruction(instruction, status, err)

    def _execute_task_with_actors(
        self,
        actors: Sequence[bases.Actor],
        instruction: pro.Instruction,
        key: Optional[Hashable],
        noid: bool
    ):
        """
        helper function for execute_do_instruction(). run each matching Actor
        in sequence.

        Args:
            actors: matching actors (output of _match_task_instruction())
            instruction: "do" Instruction
            key: instruction id or randomly-generated key
            noid: True if the instruction didn't come with an id (should never
                happen), False normally
        """
        for actor in actors:
            actor.execute(self, instruction, key=key, noid=noid)

    def execute_do_instruction(self, instruction: pro.Instruction):
        """
        identify matching Actors and execute a "do" Instruction (an
        Instruction specifying an action). typically called from the
        _handle_instruction() workflow.

        Args:
            instruction: Instruction to match and execute.
        """
        # this will raise NoActorForEvent if none match
        actors = self._match_task_instruction(instruction)
        if instruction.id is None:
            # this should really never happen, but...
            key, noid, noid_infix = (
                random.randint(0, int(1e7)),
                True,
                "noid_",
            )
        else:
            key, noid, noid_infix = instruction.id, False, ""
        threadname = f"Instruction_{noid_infix}{key}"
        # TODO: this could get sticky for the multi-step case
        self.threads[threadname] = self.exc.submit(
            self._execute_task_with_actors, actors, instruction, key, noid
        )

    def _trysend(self, message: Message):
        """
        try to send a message to the Station. Sleep if it doesn't work --
        or if we're shut down, just assume the Station is dead and leave.

        args:
            message: Message to send to station.
        """
        response, was_locked, timeout_counter = None, self.locked, count()
        self.locked = True
        while response in (None, "timeout", "connection refused"):
            # if we couldn't get to the Station, log that fact, wait, and
            # retry. lock self while this is happening to ensure we don't do
            # this in big pulses.
            if response in ("timeout", "connection refused"):
                if next(timeout_counter) % 10 == 0:
                    if self.state == "stopped":
                        self._log(
                            "no response from station, completing termination",
                            category='comms'
                        )
                        self.locked = False  # TODO: sure about this?
                        return 'timeout'
                    self._log(response, category="comms", direction="recv")
                # TODO, maybe: this could be a separate attribute
                time.sleep(self.update_interval)
            response, _ = stsend(self._insert_state(message), *self.station)
        # if we locked ourselves due to bad responses, and we weren't already
        # locked for some reason -- like we often will have been if sending
        # a task report or something -- unlock ourselves.
        if was_locked is False:
            self.locked = False
        return response

    def _interpret_response(self, response: bytes) -> str:
        """
        interpret a response from the Station. If it contains an Instruction,
        append it to this Delegate's instruction_queue.

        Args:
            response: bytes containing a hostess com received from the Station.

        Returns:
            "err" if the comm failed to decode properly, "ok" if the comm
                decoded properly but did not contain an Instruction (like a
                simple acknowledgment comm), "instruction" if the comm
                contained an Instruction.
        """
        decoded = read_comm(response)
        if isinstance(decoded, dict):
            if decoded['err']:
                # TODO: log
                return "err"
            decoded = decoded["body"]
        if isinstance(decoded, pro.Instruction):
            self.instruction_queue.append(decoded)
            return "instruction"
        return "ok"

    def talk_to_station(self, message: pro.Message) -> Union[str, bytes]:
        """
        send a Message to the Station and queue any returned Instruction.

        Args:
            message: hostess protobuf Message to send to the Station

        Returns:
             status code for exchange: "ok" if successful but no Instruction
                received (simple acknowledgement comm received);
                "timeout" or "connection refused" for failed connections;
                "err" for receipt of bytes we could not decode as a comm;
                "instruction" if successful and comm contained an Instruction
        """
        response = self._trysend(message)
        if response not in ('timeout', 'connection refused'):
            response = self._interpret_response(response)
        if response in ('err', 'timeout', 'connection_refused'):
            self._log(
                message, status=response, category="comms", direction="recv"
            )
        return response

    def _running_actions_message(self) -> list[pro.TaskReport]:
        """
        helper function for constructing Updates.

        Returns:
            list of TaskReport Messages, one for each currently-running action.
        """
        running = filter(
            lambda a: a.get('status') == 'running', self.actions.values()
        )
        return list(map(task_msg, running))

    def _base_message(self, **fields) -> pro.Update:
        """
        construct a basic Update message.

        Args:
            **fields: dict of Update field names + values to add to the base
                Update.

        Returns:
            a pro.Update message suitable for sending to the Station. Contains
                delegate id, timestamp, delegate state, running actions, and
                anything passed in **fields.
        """
        # noinspection PyProtectedMember
        return pro.Update(
            delegateid=self.nodeid(),
            time=make_timestamp(),
            state=pro.DelegateState(
                status=self.state,
                # TODO: loc assignment
                loc="primary",
                can_receive=False,
                busy=self.busy(),
                threads={k: v._state for k, v in self.threads.items()}
            ),
            running=self._running_actions_message(),
            **fields
        )

    # TODO: untangle this + _base_message() workflow
    def _insert_state(self, message: pro.Update) -> pro.Update:
        """
        insert the Delegate's current state information into an Update.

        Called immediately before every attempt to send an Update to the
        Station.

        Args:
            message: Update to update.

        Returns:
            the updated Update.
        """
        state = pro.DelegateState(
            interface=pack_obj(self.config['interface']),
            cdict=pack_obj(self.config['cdict']),
            actors=self.identify_elements("actors"),
            sensors=self.identify_elements("sensors"),
            infocount=dict(self.infocount),
            init_params=pack_obj(self.init_params)
        )
        message.state.MergeFrom(state)
        return message

    def _reply_to_instruction(
        self,
        instruction: pro.Instruction,
        status: Literal["bad_request", "wilco"],
        err: Optional[Any] = None
    ):
        """
        send a reply Update to an Instruction informing the Station that we
        will or won't do the thing.

        Args:
            instruction: received Instruction
            status: "wilco" if we'll do it, "bad_request" if we won't/can't
            err: Object -- a code or Exception, usually -- explaining a
                "bad_request" status. None if status is "wilco".
        """
        msg = self._base_message()
        msg.MergeFrom(pro.Update(reason=status, instruction_id=instruction.id))
        # TODO, maybe: kinda messy?
        if err is not None:
            msg.MergeFrom(pro.Update(info=[pack_obj(err)]))
        self.talk_to_station(msg)

    def add_actionable_event(
        self,
        event: Any,
        category: Optional[Union[str, Sensor]] = None
    ):
        """
        Queue an actionable event, usually received from a Sensor, for
        transmission to the Station. This method is most often called by an
        Actor.

        Args:
            event: object we'd like Station to know about
            category: optional label for type of event or originating Sensor,
                used to update self.infocount
        """
        if isinstance(category, Sensor):
            category = category.name
        if category is not None:
            self.infocount[category] += 1
        self.actionable_events.append(event)

    station: tuple[str, int]
    loginfo: Mapping[str]


class HeadlessDelegate(Delegate):
    """
    simple Delegate implementation that just does stuff on its own. mostly for
    testing/prototyping but could easily be useful.
    """

    def __init__(self, *args, **kwargs):
        station = ("", -1)
        super().__init__(station, *args, **kwargs)
        self.message_log = []

    def talk_to_station(self, message):
        self.message_log.append(message)


# TODO: log initialization failures
def launch_delegate(
    station_address: tuple[str, int],
    name: str,
    delegate_module: str = "hostess.station.delegates",
    delegate_class: str = "Delegate",
    elements: tuple[tuple[str, str]] = None,
    is_local: bool = False,
    **init_kwargs
) -> Optional[Delegate]:
    """
    hook for launching a delegate, designed to be easily called either locally
    or from an interpreter running in a separate process. Designed to be
    called as part of the Station.launch_delegate() workflow, but may be used
    in other ways.

    Args:
        station_address: address of supervising Station
        name: name to assign to Delegate instance
        delegate_module: name of, or path to, module in which the desired
            Delegate subclass is defined
        delegate_class: name of Delegate subclass
        elements: specifications for Actors and Sensors to attach to Delegate
            instance
        is_local: is this Delegate being instantiated in the process of the
            calling Station (or other launcher) or not? if this is False,
            consider this process "owned by" the instantiated Delegate;
            terminate it when the Delegate shuts down.

    Returns:
        the instantiated Delegate if is_local is not False; None otherwise.
    """

    from hostess.utilities import import_module

    module: ModuleType = import_module(delegate_module)
    cls: Type[Delegate] = getattr(module, delegate_class)
    if is_local is False:
        init_kwargs['_is_process_owner'] = True
    delegate: Delegate = cls(station_address, name, **init_kwargs)
    for emod_name, ecls_name in elements:
        emodule: ModuleType = import_module(emod_name)
        ecls: Type[Union[Actor, Sensor]] = getattr(emodule, ecls_name)
        delegate.add_element(ecls)
    # TODO: config-on-launch
    delegate.start()
    if is_local is True:
        return delegate
    # need to prevent the interpreter from exiting in order to not mess up
    # threading if running in an unmanaged process
    while delegate.is_shut_down is False:
        time.sleep(1)
    sys.exit()
