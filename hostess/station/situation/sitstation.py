"""
sample Station with a variety of situation-relevant qualities for development
purposes.
"""
import random
import time
from itertools import chain
from pathlib import Path

import fire
from cytoolz.curried import get, valmap

from hostess.monitors import Ticker, ticked
from hostess.station.actors import InstructionFromInfo, reported
from hostess.station.bases import Actor, NoMatch
from hostess.station.messages import make_action, make_instruction
from hostess.station.station import Station
from hostess.station.tests.testing_actors import TrivialActor

SITSTATION_TICKER = Ticker()


def getsocks(station: Station):
    selkeys = {}
    for k in list(
        chain.from_iterable(station.server.queues.values())
        # with the short timeout duration, we will sometimes miss sockets,
        # but it's better than adding 0.5s per loop for a status display.
    ) + station.server.sel.select(0.05):
        if hasattr(k[0], 'fd'):  # filter intentional bad values
            selkeys[k[0].fd] = k[0]
    sprint = []
    for s in selkeys.values():
        if s.fd == station.server.sock.fileno():
            connected = "is server"
        else:
            try:
                connected = s.fileobj.getpeername()
            except OSError as ose:
                connected = str(ose).split(" Transport")[0]
        rec = {
            "fd": s.fd,
            "connected": connected,
            "events": s.events,
            "callback": "" if not hasattr(s, "data") else s.data.__name__,
        }
        sprint.append(rec)
    return sorted(sprint, key=get("fd"))


def status_display(station, n, start, loop_pause):
    dstring = (
        f"{n} loops\n"
        f"{len(station.inbox.completed)} tasks completed\n"
        f"loop latency {round(time.time() - start - loop_pause, 3)}\n"
        f"peer lock size {len(station.server.peers)}\n"
        f"--queued sockets--\n"
    )
    for rec in getsocks(station):
        dstring += f"{rec}\n"
    dstring += f"--ticks--\n{SITSTATION_TICKER}"
    # dstring += f"{DEFAULT_PROFILER}\n"
    return dstring


def sleep_trigger_instruction(note, *_, **__):
    if 'succeed' in note['match']:
        description = {'what_to_do': 'succeed'}
    else:
        description = {'what_to_do': 'fail'}
    return make_instruction(
        "do", action=make_action(description, name="sleep"),
    )


class Sleeper(Actor):

    def match(self, instruction, *_, **__):
        if instruction.action.name == 'sleep':
            return True
        raise NoMatch

    @reported
    def execute(self, node, msg, *_, **__):
        if msg.description['what_to_do'] == 'fail':
            raise Exception('failed!')
        time.sleep(self.duration)
        return 1

    def _get_duration(self):
        return self._duration

    def _set_duration(self, duration):
        self._duration = duration

    name = 'sleeper'
    actortype = 'action'
    _duration = 1
    duration = property(_get_duration, _set_duration)
    interface = ('duration',)


def make_sample_station():
    host, port = "localhost", random.randint(10000, 20000)

    station = Station(host, port)
    station.save_port_to_shared_memory()
    delkwargs = {'update_interval': 0.5, 'context': 'local'}
    station.launch_delegate(
        "watch",
        elements=[("hostess.station.actors", "FileSystemWatch")],
        **delkwargs
    )
    station.launch_delegate(
        "sleepy",
        elements=[("hostess.station.situation.sitstation", "Sleeper")],
        **delkwargs
    )
    station.add_element(InstructionFromInfo, name="dosleep")
    station.dosleep_instruction_maker = sleep_trigger_instruction
    station.dosleep_criteria = [lambda n: "match" in n.keys()]
    station.dosleep_target_name = "sleepy"
    for _ in range(50):
        station.add_element(TrivialActor)
    station.set_delegate_properties(
        "watch",
        filewatch_target="dump.txt",
        filewatch_patterns=("succeed", "fail",),
    )
    station.set_delegate_properties('sleepy', sleeper_duration=0.2)

    station._situation_comm = ticked(
        station._situation_comm, 'sent situation', SITSTATION_TICKER
    )
    station._handle_incoming_message = ticked(
        station._handle_incoming_message, 'Updates received', SITSTATION_TICKER
    )
    station.start()
    if (textfile := Path("dump.txt")).exists():
        textfile.unlink()
    return station


def _backend_loop(i, n, verbose, station):
    start = time.time()
    if i < n:
        # indirectly trigger tasks by writing text to file the Sensor on
        # the 'watch' delegate will recognize. occasionally queue the Actor on
        # the 'sleeper' delegate to fail a task by randomly writing 'fail'.
        text = 'succeed' if random.random() > 0.1 else 'fail'
        with open("dump.txt", "a") as f:
            f.write(f"{text}\n")
        loop_pause = 0.2
    else:
        loop_pause = 0.5
    if random.random() < 0.08:
        # randomly crash server threads
        random.choice(
            tuple(station.server.queues.values())
        ).append(bytearray(b"NO"))
    time.sleep(loop_pause)
    if station.state == "crashed":
        raise station.exception
    if verbose:
        print(status_display(station, i, start, loop_pause))
        # print(valmap(lambda t: t._state, station.threads))
    return i + 1


def run_sample_backend(n_writes: int = 1000, verbose: bool = True):
    station = make_sample_station()
    exception = None
    try:
        i = 0
        while True:
            i = _backend_loop(i, n_writes, verbose, station)
    except KeyboardInterrupt:
        print('\nstopping on keyboard interrupt\n')
    except Exception as ex:
        exception = ex
        print(exception)
    finally:
        station.shutdown(exception)


if __name__ == "__main__":
    fire.Fire(run_sample_backend)
