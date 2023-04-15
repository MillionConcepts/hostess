import atexit
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
from itertools import cycle
import selectors
import socket
import time

from cytoolz import curry

HOSTESS_ACK = b"\06hostess"
HOSTESS_EOM = b"\03hostess"


def tryread(conn, chunksize, eomstr):
    status, reading = "streaming", True
    try:
        data = conn.recv(chunksize)
    except OSError as ose:
        if "temporarily" not in str(ose):
            raise
        if eomstr is None:
            raise
        return None, "error", True
    if data == b"":
        status, reading = "stopped", False
    if eomstr is not None:
        if data.endswith(eomstr):
            status, reading = "eom", False
    return data, status, reading


def timeout_factory():
    starts = []

    def waiting():
        if len(starts) == 0:
            starts.append(time.time())
            return 0
        return time.time() - starts[-1]

    def nowait():
        try:
            starts.pop()
        except IndexError:
            pass

    return waiting, nowait


def read(
    sel,
    conn,
    _mask,
    chunksize=4096,
    eomstr=HOSTESS_EOM,
    timeout=5,
    delay=0.01,
):
    stream, status = b"", "unk"
    try:
        sel.unregister(conn)
        reading = True
        waiting, nowait = timeout_factory()
        while reading:
            data, status, reading = tryread(conn, chunksize, eomstr)
            if status == "error":
                if waiting() > timeout:
                    raise TimeoutError
                time.sleep(delay)
                continue
            nowait()
            stream += data
        if status in ("stopped", "eom"):
            sel.register(conn, selectors.EVENT_WRITE, curry(ack)(sel))
    except BrokenPipeError:
        status = "broken pipe"
    except TimeoutError:
        status = "timed out"
    except OSError as ose:
        status = str(ose)
    return stream, f"read {len(stream)}", status


def ack(sel, conn, _mask, ackstr=HOSTESS_ACK):
    try:
        sel.unregister(conn)
        conn.send(ackstr)
    except (KeyError, ValueError):
        # TODO: maybe log it
        pass


def accept(sel, sock, mask):
    try:
        conn, addr = sock.accept()
    except BlockingIOError:
        # TODO: do something..,
        return None, "blocking", None, "blocking"
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, curry(read)(sel))
    return None, "accept", conn.getpeername(), "ok"


def signal_factory(thread_dict):
    def signaler(name, signal=0):
        if name == "all":
            for k in thread_dict.keys():
                thread_dict[k] = signal
            return
        if name not in thread_dict.keys():
            raise KeyError
        thread_dict[name] = signal

    return signaler


def kill_factory(signaler, sock):
    def kill(signal=0):
        signaler("all", signal)
        return sock.close()

    return kill


def check_peerage(key, peers):
    try:
        peer = key.fileobj.getpeername()
        return peer, peer in peers
    except OSError:
        return None, False


def launch_read_thread(
    data, events, peers, name, queue, signals, interval=0.01
):
    while signals.get(name) is None:
        time.sleep(interval)
        try:
            key, mask, id_ = queue.pop()
        except IndexError:
            continue
        peer, peerage = check_peerage(key, peers)
        if (peerage is True) and (key.data.__name__ != "ack"):
            continue
        try:
            if key.data.__name__ == "read":
                # noinspection PyProtectedMember
                if key.fileobj._closed or (peer is None):
                    continue
                peers[peer] = True
                try:
                    stream, event, status = key.data(key.fileobj, mask)
                except KeyError:
                    # attempting to unregister an already-unregistered conn
                    continue
            elif key.data.__name__ == "ack":
                key.data(key.fileobj, mask)
                try:
                    del peers[peer]
                except KeyError:
                    pass
                continue
            else:
                stream, event, peer, status = key.data(key.fileobj, mask)
        except OSError as err:
            stream, event, peer, status = None, "oserror", "unknown", str(err)
        now = dt.datetime.now().isoformat()
        event = {
            "event": event,
            "peer": peer,
            "status": status,
            "time": now,
            "thread": name,
            "id": id_,
            "act": key.data.__name__,
        }
        events.append(event)
        if (stream is None) or (len(stream) == 0):
            continue
        data.append(event | {"content": stream})
    return {"thread": name, "signal": signals.get(name)}


def launch_selector_thread(sock, queues, signals, interval=0.01, poll=1):
    id_, cycler = 0, cycle(queues.keys())
    with selectors.DefaultSelector() as sel:
        sel.register(sock, selectors.EVENT_READ, curry(accept)(sel))
        while signals.get("select") is None:
            try:
                events = sel.select(poll)
            except TimeoutError:
                continue
            for key, mask in events:
                target = next(cycler)
                queues[target].append((key, mask, id_))
                id_ += 1
            time.sleep(interval)
    return {"thread": "select", "signal": signals.get("select")}


def launch_tcp_server(host, port, read_threads=4, interval=0.01):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    atexit.register(sock.close)
    try:
        sock.bind((host, port))
        sock.listen()
        sock.setblocking(False)
        executor = ThreadPoolExecutor(read_threads + 1)
        threads, events, data, peers = {}, [], [], {}
        queues = {i: [] for i in range(read_threads)}
        signals = {i: None for i in range(read_threads)} | {"select": None}
        shared = (signals, interval)
        threads["select"] = executor.submit(
            launch_selector_thread, sock, queues, *shared
        )
        for ix in range(read_threads):
            threads[ix] = executor.submit(
                launch_read_thread,
                data,
                events,
                peers,
                ix,
                queues[ix],
                *shared,
            )
        return {
            "threads": threads,
            "sig": signal_factory(signals),
            "sock": sock,
            "events": events,
            "data": data,
            "kill": kill_factory(signal_factory(signals), sock),
            "exec": executor,
            "queues": queues,
        }
    except Exception:
        sock.close()
        raise


def tcp_send(data, host, port, timeout=10, send_delay=0, chunksize=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        if (send_delay > 0) or (chunksize is not None):
            chunksize = 1024 if chunksize is None else chunksize
            while len(data) > 0:
                data, chunk = data[chunksize:], data[:chunksize]
                sock.send(chunk)
                time.sleep(send_delay)
        else:
            sock.sendall(data)
        try:
            return sock.recv(1024), sock.getsockname()
        except TimeoutError:
            return "timeout", sock.getsockname()
        finally:
            sock.close()
