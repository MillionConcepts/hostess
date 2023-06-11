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
        station=(station.host, station.port),
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
    instruction = make_instruction("do", action=action)
    station.outbox['writer'].append(instruction)
    time.sleep(0.5)
    report = [
        m for m in station.inbox
        if enum(m['content']['body'], "reason") == 'completion'
    ][0]
    try:
        assert report['content']['body'].completed.action.name == 'filewrite'
        assert (
            enum(report['content']['body'].completed.action, "status")
            == 'success'
        )
        with open("test.txt") as stream:
            assert stream.read() == "hello"
    # TODO: shut down nodes
    finally:
        Path("test.txt").unlink(missing_ok=True)
        writer.logfile.unlink()
        station.logfile.unlink()
        station.shutdown()


test_actions_1()
