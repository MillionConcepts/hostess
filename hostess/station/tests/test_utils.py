import random
import re
from multiprocessing import Pool

from hostess.monitors import Stopwatch
from hostess.station.talkie import HOSTESS_EOM, tcp_send, launch_tcp_server


def send_randbytes(
    host,
    port,
    n_bytes=1024,
    timeout=1,
    send_delay=0,
    chunksize=None,
    header=b"",
    eom=HOSTESS_EOM,
):
    exception = None
    randbytes = header + random.randbytes(n_bytes) + eom
    result = None
    try:
        result = tcp_send(
            randbytes, host, port, timeout, send_delay, chunksize
        )
    except Exception as ex:
        exception = ex
    return randbytes, exception, result


def test_tcp_server():
    """
    test the hostess.talkie server by briefly hammering it from 7 processes
    and checking recorded input for validity
    """
    host, port, n_threads, interval = "localhost", 11129, 4, 0.001
    server = launch_tcp_server(host, port, n_threads, interval)
    watch = Stopwatch()
    watch.start()
    n_workers = 7
    pool, results, messages, exceptions, res = Pool(n_workers), {}, {}, {}, {}
    args = [host, port, 5 * 1024 * 1024, 5, 0, None]
    for i in range(7):
        tag = f"{i}\nSTART\n".encode("ascii")
        results[i] = pool.apply_async(send_randbytes, args, {"header": tag})
    pool.close()
    pool.join()
    print(watch.clickpeek())
    for i, r in results.items():
        messages[i], exceptions[i], res[i] = r.get()
    pool.terminate()
    for rec in server["data"]:
        message = rec["content"]
        if len(message) == 0:
            continue
        message_id = int(
            re.search(r"\d+", message[:4].decode("ascii")).group()
        )
        assert message == messages[message_id]
    server['kill']()
