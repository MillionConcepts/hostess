import datetime as dt
import random
import re
from multiprocessing import Pool

import dill

import hostess.station.comm
import hostess.station.proto.station_pb2 as pro
import hostess.station.talkie as tk
from hostess.monitors import Stopwatch
from hostess.station.messages import task_msg


def send_randbytes(
    host,
    port,
    n_bytes=1024,
    timeout=1,
    send_delay=0,
    chunksize=None,
    header=b"",
    eom=hostess.station.comm.HOSTESS_EOM,
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


# TODO: make it use the protobuf decoder despite my good intentions
def test_tcp_server():
    """
    test the hostess.talkie server by briefly hammering it from 7 processes
    and checking recorded input for validity. don't use protobuf decoder --
    this is supposed to be a test of base server functionality.
    """
    # launch the server with 7 threads and a 1-ms poll rate
    host, port, n_threads, rate = "localhost", 11129, 4, 0.001
    server = tk.TCPTalk(host, port, n_threads, rate, decoder=None)
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
    # unpack results
    for i, r in results.items():
        messages[i], exceptions[i], res[i] = r.get()
    pool.terminate()
    # check received against sent messages
    for rec in server.data:
        message = rec["content"]
        if len(message) == 0:
            continue
        message_id = int(
            re.search(r"\d+", message[:4].decode("ascii")).group()
        )
        assert message == messages[message_id]
    # close the socket and stop the listeners so the test will terminate
    server.kill()


def _construct_basic_action():
    actiondict = {
        'result': [0],
        'name': 'imagestats',
        'start': dt.datetime.now(dt.UTC),
        'end': dt.datetime.now(dt.UTC),
        'status': 'success',
        'instruction_id': 3,
        'id': 2
    }
    report = task_msg(actiondict)
    message = pro.Update(completed=report, instruction_id=3)
    return message


def test_protobuf():
    """attempt to construct a basic station protobuf Message"""
    message = _construct_basic_action()
    assert dill.loads(message.completed.action.result.value) == [0]


def test_comm():
    """check offline comm roundtrip"""
    message = _construct_basic_action()
    encoded = hostess.station.comm.make_comm(message)
    decoded = hostess.station.comm.read_comm(encoded)
    assert decoded["header"]["mtype"] == "Update"
    assert decoded["body"].completed.action.name == "imagestats"
    assert decoded["header"]["length"] == (
        message.ByteSize() + hostess.station.comm.WRAPPER_SIZE
    )


def test_comm_online():
    """check online comm roundtrip"""
    port = random.randint(10000, 20000)
    host, port, report = "localhost", port, _construct_basic_action()
    server = tk.TCPTalk(host, port)
    try:
        for i in range(10):
            ack, _ = tk.stsend(report, host, port, timeout=100)
            response, j = None, 0
            while response is None and j < 1000:
                for v in reversed(server.events):
                    if v['event'].startswith('decoded'):
                        response = v
                        break
            if response is None:
                raise TimeoutError
            # Timestamp is variable-length depending on nanoseconds
            assert response["event"] in (
                "decoded 96", "decoded 97", "decoded 98"
            )
            comm = hostess.station.talkie.read_comm(server.data[i].comm)
            assert comm["err"] == ""
            assert comm["header"]["length"] == int(response['event'][-2:])
            message = comm["body"]
            assert message.completed.action.result.value[0] == 128
            assert message.instruction_id == 3
            assert ack.startswith(hostess.station.comm.HOSTESS_SOH)
    finally:
        server.kill()
