import os
import random
import time
from itertools import chain

from cytoolz.curried import get
from dustgoggles.codex.implements import Sticky
from dustgoggles.func import gmap

from hostess.monitors import DEFAULT_TICKER
from hostess.station.actors import InstructionFromInfo
from hostess.station.messages import make_action, make_instruction
from hostess.station.station import Station


def getsocks(station: Station):
    selkeys = {
        k[0].fd: k[0]
        for k in
        list(chain.from_iterable(station.server.queues.values()))
        + station.server.sel.select()
    }
    sprint = []
    for s in selkeys.values():
        if s.fd == station.server.sock.fileno():
            connected = 'is server'
        else:
            try:
                connected = s.fileobj.getpeername()
            except OSError as ose:
                connected = str(ose).split(' Transport')[0]
        rec = {
            'fd': s.fd,
            'connected': connected,
            'events': s.events,
            'callback': '' if not hasattr(s, 'data') else s.data.__name__
        }
        sprint.append(rec)
    return sorted(sprint, key=get('fd'))


def sleep_trigger_instruction(*_, **__):
    return make_instruction(
        "do",
        action=make_action({'why': 'no reason'}, name="sleep"),
    )


host, port = "localhost", random.randint(10000, 20000)
sticky = Sticky.note(port, "station-port-report", cleanup_on_exit=True)
station = Station(host, port)
watch = station.launch_delegate(
    "watch",
    elements=[
        ("hostess.station.actors", "FileSystemWatch"),
        ("logscratch", "Sleepy"),
    ],
    update_interval=0.5,
    poll=0.01,
    n_threads=8,
    context="local",
)
station.add_element(InstructionFromInfo, name="dosleep")
station.dosleep_instruction_maker = sleep_trigger_instruction
station.dosleep_criteria = [lambda n: "match" in n.keys()]
station.dosleep_target_name = "watch"
station.set_delegate_properties(
    "watch",
    filewatch_target="dump.txt",
    filewatch_patterns=("hi",),
    sleeper_duration=1,
)
station.start()
os.unlink('dump.txt')
exception = None

try:
    i, n = 0, 1000
    while True:
        start = time.time()
        if i < n:
            with open('dump.txt', 'a') as f:
                f.write('hi')
            loop_pause = 0.2
        else:
            loop_pause = 0.5
        time.sleep(loop_pause)
        if station.state == 'crashed':
            raise station.exception
        # Note that if you write quite quickly, do not expect n completed tasks
        #  (the Sensor does not trigger once per 'hi', but once per detected
        #  write)
        # socks = station.server.sel.select(1)
        # try:
        #     sock = socks[0][0].fileobj
        dstring = (
            f"{len(station.inbox.completed)} tasks completed\n"
            f"{i} loops\n"
            f"loop latency {round(time.time() - start - loop_pause, 3)}\n"
            f"peer lock size {len(station.server.peers)}\n"
            f"--socks--\n"
        )
        for rec in getsocks(station):
            dstring += f"{rec}\n"
        dstring += f"--ticks--\n{DEFAULT_TICKER}\n----"
        print(dstring)
        i += 1
except Exception as ex:
    exception = ex
    print(exception)
finally:
    station.shutdown(exception)
