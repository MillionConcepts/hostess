import atexit
from pathlib import Path
import random
import time

from hostess.station.actors import FileWriter
from hostess.station.messages import make_action, pack_obj, make_instruction
from hostess.station.nodes import Node
from hostess.station.proto_utils import enum
from hostess.station.station import Station


def test_shutdown():
    host, port = "localhost", random.randint(10000, 20000)
    station = Station(host, port)
    writer = station.launch_node('null', context='local', update_interval=0.1)
    station.start()
    station.shutdown()
    assert all(thread.done() for thread in writer.threads.values())
    assert all(thread.done() for thread in station.threads.values())
    # TODO: test logging


def test_actions_1():
    host, port = "localhost", random.randint(10000, 20000)
    station = Station(host, port)
    writer = station.launch_node(
        'writer',
        elements=[('hostess.station.actors', 'FileWriter')],
        context='local',
        update_interval=0.05
    )
    station.start()
    station.set_node_properties("writer", filewrite_file="test.txt")
    action = make_action(name="filewrite", localcall=pack_obj("hello"))
    for _ in range(10):
        instruction = make_instruction("do", action=action)
        station.queue_task('writer', instruction)
    time.sleep(1.9)
    try:
        report = station.inbox.completed[0]
        assert report['action']['name'] == 'filewrite'
        assert report['action']['status'] == 'success'
        with open("test.txt") as stream:
            assert stream.read() == "hello" * 10
    finally:
        station.shutdown()
        Path("test.txt").unlink(missing_ok=True)
        writer.logfile.unlink(missing_ok=True)
        station.logfile.unlink(missing_ok=True)


test_actions_1()
