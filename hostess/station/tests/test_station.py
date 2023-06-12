import atexit
from pathlib import Path
import random
import time

from hostess.station.actors import FileWriter
from hostess.station.messages import make_action, pack_obj, make_instruction
from hostess.station.nodes import Station, Node
from hostess.station.proto_utils import enum


def test_actions_1():
    host, port = "localhost", random.randint(10000, 20000)
    station = Station(host, port)
    station.nodes = ["writer"]
    station.start()
    writer = Node(
        station_address=(station.host, station.port),
        name="writer",
        update_interval=0.1
    )
    writer.add_element(FileWriter)
    writer.start()
    station.set_node_properties(
        "writer",
        filewrite_file="test.txt"
    )
    time.sleep(0.5)
    action = make_action(
        name="filewrite", localcall=pack_obj("hello")
    )
    for _ in range(20):
        instruction = make_instruction("do", action=action)
        station.outbox['writer'].append(instruction)
    time.sleep(3)
    try:
        report = [
            m for m in station.inbox
            if enum(m['content']['body'], "reason") == 'completion'
        ][0]
        assert report['content']['body'].completed.action.name == 'filewrite'
        assert (
            enum(report['content']['body'].completed.action, "status")
            == 'success'
        )
        with open("test.txt") as stream:
            assert stream.read() == "hello" * 10
    finally:
        station.shutdown()
        Path("test.txt").unlink(missing_ok=True)
        writer.logfile.unlink(missing_ok=True)
        station.logfile.unlink(missing_ok=True)


test_actions_1()
