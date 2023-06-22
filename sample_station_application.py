import random
import shutil
import time
from pathlib import Path

from hostess.station.actors import InstructionFromInfo
from hostess.station.messages import (
    make_function_call_action,
    make_instruction,
)
from hostess.station.station import Station
from hostess.utilities import filterdone


# simple instruction factory: 'would you please make a thumbnail of this?'
def thumbnail_instruction(note):
    action = make_function_call_action(
        func="thumbnail",
        module="hostess.station.tests.target_functions",
        kwargs={"path": Path(note["content"])},
        context="process",
    )
    return make_instruction("do", action=action)


# host/port for station
host, port = "localhost", random.randint(10000, 20000)

# launch and config specifications for our nodes

# directory-watching node
watcher_launch_spec = {
    # add a directory-watching Sensor
    "elements": [("hostess.station.actors", "DirWatch")],
}
watcher_config_spec = {
    # what directory shall we watch?
    "dirwatch_target": "test_dir",
    # what filename patterns are we watching for?
    "dirwatch_patterns": (r".*full.*\.jpg",),
}
# thumbnail-making node
thumbnail_launch_spec = {
    # add a function-calling Actor
    "elements": [("hostess.station.actors", "FuncCaller")],
}
# nothing special to configure about the thumbnail node

# make a clean test directory
test_dir = Path("test_dir")
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir()

station = Station(host, port)

# add an Actor to the Station to make thumbnailing instructions when it hears
# about a new image file
thumbnail_checker = InstructionFromInfo
station.add_element(thumbnail_checker, name="thumbnail")

# the station now inherits the thumbnail checker's properties as attributes
# tell it to use the thumbnail instruction function
station.thumbnail_instruction_maker = thumbnail_instruction
# make sure that it only activates if the string it receives is a real path
station.thumbnail_criteria = [lambda n: Path(n["content"]).is_file()]
# tell it to send this type of instruction to the thumbnail node and no other
station.thumbnail_target_name = "thumbnail"

# start the station
station.start()

# launch and configure the nodes
station.launch_node("watcher", **watcher_launch_spec, update_interval=0.1)
station.launch_node("thumbnail", **thumbnail_launch_spec, update_interval=0.1)
station.set_node_properties("watcher", **watcher_config_spec)

# copy a squirrel picture into the directory as a test (representing some
# external change to the system)
test_file = "test_data/squirrel.jpg"
shutil.copyfile(test_file, test_dir / (Path(test_file).stem + "_full.jpg"))
time.sleep(2)
try:
    print(station.inbox.sort()['completion'][-1].display())
    while True:
        # loop forever; other _full_ .jpg files placed into the directory
        # should also get thumbnailed
        time.sleep(2)
finally:
    station.shutdown()
