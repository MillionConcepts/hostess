import os
from itertools import chain
import random
import threading
import time

from hostess.monitors import DEFAULT_TICKER
from hostess.profilers import DEFAULT_PROFILER
from hostess.station.actors import InstructionFromInfo
from hostess.station.messages import make_action, make_instruction
from hostess.station.station import Station

from dustgoggles.codex.implements import Sticky


def sleep_trigger_instruction(*_, **__):
    return make_instruction(
        "do",
        action=make_action({'why': 'no reason'}, name="sleep"),
    )


host, port = "localhost", random.randint(10000, 20000)
sticky = Sticky.note(port, "station-port-report", cleanup_on_exit=True)
station = Station(host, port)
print(port)

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
        if i < n:
            with open('dump.txt', 'a') as f:
                f.write('hi')
            time.sleep(0.2)
        else:
            time.sleep(0.5)
        # print(DEFAULT_PROFILER)
        if station.state == 'crashed':
            raise station.exception
        # Note that if you write quite quickly, do not expect n completed tasks
        #  (the Sensor does not trigger once per 'hi', but once per detected
        #  write)
        socks = (
            len(station.server.sel.select(1)) + sum(
                map(len, station.server.queues.values())
            )
        )
        print(
            f"{len(station.inbox.completed)} tasks completed\n",
            f"{i} loops\n",
            f"{socks} pending sockets\n",
            f"--ticks--\n{DEFAULT_TICKER}\n----"
        )
        i += 1
except Exception as ex:
    exception = ex
    print(exception)
finally:
    station.shutdown(exception)
