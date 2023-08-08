import os
import random
import time

from hostess.profilers import DEFAULT_PROFILER
from hostess.station.actors import InstructionFromInfo
from hostess.station.messages import make_action, make_instruction
from hostess.station.station import Station

from dustgoggles.codex.implements import Sticky


def sleep_trigger_instruction(*_, **__):
    return make_instruction("do", action=make_action({}, name="sleep"))


host, port = "localhost", random.randint(10000, 20000)
sticky = Sticky.note(port, "test_station_port", cleanup_on_exit=True)
station = Station(host, port)
print(port)

watch = station.launch_delegate(
    "watch",
    elements=[
        ("hostess.station.actors", "FileSystemWatch"),
        ("logscratch", "Sleepy"),
    ],
    update_interval=3,
    poll=0.5,
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
    sleeper_duration=2,
)
station.start()

os.unlink('dump.txt')
exception = None
try:
    for i in range(1000):
        with open('dump.txt', 'a') as f:
            f.write('hi')
        time.sleep(1)
        print(len(station.inbox.completed))
        # print(DEFAULT_PROFILER)
except Exception as ex:
    exception = ex
finally:
    station.shutdown(exception)
