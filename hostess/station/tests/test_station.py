import shutil
from pathlib import Path
import random
import shutil
import time
from pathlib import Path

from hostess.station.actors import InstructionFromInfo
from hostess.station.messages import (
    make_action,
    pack_obj,
    make_instruction,
    make_function_call_action,
)
from hostess.station.station import Station
from hostess.utilities import timeout_factory


def test_shutdown():
    host, port = "localhost", random.randint(10000, 20000)
    station = Station(host, port)
    writer = station.launch_delegate("null", context="local", update_interval=0.1)
    station.start()
    station.shutdown()
    assert all(thread.done() for thread in writer.threads.values())
    assert all(thread.done() for thread in station.threads.values())
    # TODO: test logging


def test_actions_1():
    host, port = "localhost", random.randint(10000, 20000)
    station = Station(host, port, name='test_actions_1_station', poll=0.02)
    writer = station.launch_delegate(
        "test_actions_1_writer",
        elements=[("hostess.station.actors", "FileWriter")],
        n_threads=15,
        context="local",
        update_interval=0.05,
    )
    station.start()
    station.set_delegate_properties(
        "test_actions_1_writer", filewrite_file="test.txt"
    )
    # queue 25 instructions to write 'xyzzy' to a file
    action = make_action(name="filewrite", localcall=pack_obj("xyzzy"))
    for _ in range(25):
        instruction = make_instruction("do", action=action)
        station.queue_task("test_actions_1_writer", instruction)
    waiting, _ = timeout_factory(timeout=20)
    try:
        while len(station.inbox.completed) < 25:
            waiting()
            time.sleep(0.05)
        time.sleep(0.01)  # let info propagate station-internally
        for report in station.inbox.completed:
            # do the instruction ids on the
            # station-tracked tasks and the node-sent task reports match?
            status = station.tasks[
                report['completed']['instruction_id']
            ]['status']
            # does everyone agree the tasks succeeded?
            assert status == 'success'
            assert report['action']['status'] == 'success'
            # do we have the expected content in the file?
            with open("test.txt") as stream:
                assert stream.read() == "xyzzy" * 25
            # leave logs if the test failed, delete otherwise
            writer.logfile.unlink(missing_ok=True)
            station.logfile.unlink(missing_ok=True)
    finally:
        # clean up
        station.shutdown()
        Path("test.txt").unlink(missing_ok=True)


def test_application_1():
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
    # note that there is nothing special to configure about the thumbnail node

    # make a clean test directory
    test_dir = Path("test_dir")
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir()

    station = Station(host, port)

    # add an Actor to the Station to make thumbnailing instructions when it
    # hears about a new image file
    thumbnail_checker = InstructionFromInfo
    station.add_element(thumbnail_checker, name="thumbnail")

    # the station now inherits the thumbnail checker's properties as attributes
    # tell it to use the thumbnail instruction function
    station.thumbnail_instruction_maker = thumbnail_instruction
    # make sure that it only activates if the string it receives is a real path
    station.thumbnail_criteria = [lambda n: Path(n["content"]).is_file()]
    # send this type of instruction to the thumbnail node and no other
    station.thumbnail_target_name = "thumbnail"

    # start the station
    station.start()

    # launch the nodes as daemonic processes
    station.launch_delegate("watcher", **watcher_launch_spec, update_interval=0.5)
    station.launch_delegate(
        "thumbnail",
        **thumbnail_launch_spec,
        update_interval=0.5,
        context="local"
    )
    # configure the watcher delegate
    station.set_delegate_properties("watcher", **watcher_config_spec)

    # copy a squirrel picture into the directory as a test (representing some
    # external change to the system)
    test_file = "test_data/squirrel.jpg"
    shutil.copyfile(test_file, test_dir / (Path(test_file).stem + "_full.jpg"))
    time.sleep(2)
    try:
        assert station.inbox.completed[-1]["action"]["status"] == "success"
        assert Path("test_dir/squirrel_thumbnail.jpg").exists()
    finally:
        station.shutdown()
        shutil.rmtree(test_dir)


def test_missing():
    """test of missing-node tracking"""
    # host/port for station
    host, port = "localhost", random.randint(10000, 20000)

    # make a station
    station = Station(host, port)
    station.start()

    # create a normal and fine node
    station.launch_delegate(
        'normal_node',
        elements=[('hostess.station.tests.testing_actors', 'NormalActor')],
        update_interval=0.05,
    )
    time.sleep(0.55)
    try:
        # make sure it is normal and fine
        assert station.delegates[0]['reported_status'] == 'nominal'
        # send it a normal and fine instruction
        normal_action = make_action(description={'something': 'normal'})
        normal_instruction = make_instruction('do', action=normal_action)
        station.queue_task('normal_node', normal_instruction)
        time.sleep(6)
        # make sure it has mysteriously vanished
        assert station.delegates[0]['inferred_status'] == 'missing'
    finally:
        station.shutdown()


# test_shutdown()
# test_actions_1()
# test_missing()
# test_application_1()
