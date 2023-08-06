import random
import time

from hostess.station.actors import InstructionFromInfo
from hostess.station.messages import make_action, make_instruction
from hostess.station.station import Station


def sleep_trigger_instruction(*_, **__):
    return make_instruction("do", action=make_action({}, name='sleep'))


host, port = "localhost", random.randint(10000, 20000)
station = Station(host, port)

watch = station.launch_delegate(
    "watch",
    elements=[
        ("hostess.station.actors", "FileSystemWatch"),
        ("logscratch", "Sleepy")
    ],
    update_interval=0.5,
    poll=0.5,
    context="local"
)
station.add_element(InstructionFromInfo, name='dosleep')
station.dosleep_instruction_maker = sleep_trigger_instruction
station.dosleep_criteria = [lambda n: 'match' in n.keys()]
station.dosleep_target_name = "watch"

station.set_delegate_properties(
    "watch", 
    filewatch_target="dump.txt", 
    filewatch_patterns=("hi",),
    sleeper_duration=5,
)
station.start()
print(station.port)

# for _ in range(100):
#     with open('dump.txt', 'a') as f:
#         f.write('hi')
#     time.sleep(1)