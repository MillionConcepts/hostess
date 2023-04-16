import random
import re
from multiprocessing import Pool

from hostess.monitors import Stopwatch
import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import make_timestamp
import hostess.station.talkie as tk


def send_randbytes(
    host,
    port,
    n_bytes=1024,
    timeout=1,
    send_delay=0,
    chunksize=None,
    header=b"",
    eom=tk.HOSTESS_EOM,
):
    """send annoying random bytes."""
    exception = None
    randbytes = header + random.randbytes(n_bytes) + eom
    result = None
    try:
        result = tk.tcp_send(
            randbytes, host, port, timeout, send_delay, chunksize
        )
    except Exception as ex:
        exception = ex
    return randbytes, exception, result


def test_tcp_server():
    """
    test the hostess.talkie server by briefly hammering it from 7 processes
    and checking recorded input for validity. don't use protobuf decoder --
    this is supposed to be a test of base server functionality.
    """
    # launch the server with 7 threads and a 1-ms poll rate
    host, port, n_threads, rate = "localhost", 11129, 4, 0.001
    server, _events, data = tk.launch_tcp_server(host, port, n_threads, rate)
    # record time
    watch = Stopwatch()
    watch.start()
    # number of processes to hit it with
    n_workers = 7
    pool, results, messages, exceptions, res = Pool(n_workers), {}, {}, {}, {}
    # host, port, size of garbage messages, client timeout in seconds
    args = [host, port, 5 * 1024 * 1024, 5]
    # send garbage all at once
    for i in range(7):
        # add a tag so we can tell which was which at the end
        tag = f"{i}\nSTART\n".encode("ascii")
        results[i] = pool.apply_async(send_randbytes, args, {"header": tag})
    pool.close()
    pool.join()
    # how long did that take?
    print(watch.clickpeek())
    # unpack results
    for i, r in results.items():
        messages[i], exceptions[i], res[i] = r.get()
    pool.terminate()
    # check received against sent messages
    for rec in data:
        message = rec["content"]
        if len(message) == 0:
            continue
        message_id = int(
            re.search(r"\d+", message[:4].decode("ascii")).group()
        )
        assert message == messages[message_id]
    # close the socket and terminate the listeners so the test will terminate
    server["kill"]()


def test_protobuf():
    """attempt to construct a basic station protobuf Message"""
    actions = [
        pro.Action(name="imagestats", status="success", level="pipe"),
        pro.Action(name="handler", status="success", level="node"),
    ]
    rdict = {"sendtime": make_timestamp(), "task_id": 3, "actions": actions}
    report = pro.Report(**rdict)
    assert report.task_id == 3
    assert report.actions[1].name == "handler"
    return report


def test_comm():
    """check offline comm roundtrip"""
    report = test_protobuf()
    encoded = tk.make_comm(report)
    decoded = tk.read_comm(encoded)
    assert decoded["header"]["mtype"] == "Report"
    assert decoded["body"].actions[0].name == "imagestats"
    assert decoded["header"]["length"] == len(report.SerializeToString())


def test_comm_online():
    """check online comm roundtrip"""
    host, port, report = "localhost", 11222, test_protobuf()
    server, events, data = tk.stlisten(host, port)
    for _ in range(5):
        ack, _ = tk.stsend(report, host, port)
        # Timestamp is variable-length depending on nanoseconds
        assert data[-1]["event"] in ("decoded 69", "decoded 70")
        assert data[-1]["content"]["header"]["length"] in (48, 49)
        assert data[-1]["content"]["body"].actions[1].name == "handler"
        assert data[-1]["content"]["err"] == ""
        assert ack == tk.HOSTESS_ACK
    server["kill"]()
