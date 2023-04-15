import atexit
import struct
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
import selectors
import socket
import time
from types import MappingProxyType as MPt
from typing import (
    Optional,
    Callable,
    MutableMapping,
    MutableSequence,
    Mapping,
    Any,
)

from google.protobuf.json_format import MessageToDict

import hostess.station._proto.station_pb2 as hostess_proto
from hostess.utilities import curry, logstamp

HOSTESS_ACK = b"\06hostess"
HOSTESS_EOM = b"\03hostess"
HOSTESS_SOH = b"\01hostess"
CODE_TO_MTYPE = MPt({0: "Update", 1: "Instruction", 2: "Report"})
MTYPE_TO_CODE = MPt({v: k for k, v in CODE_TO_MTYPE.items()})
HEADER_STRUCT = struct.Struct("<8sB?L")


def timeout_factory() -> tuple[Callable[[], int], Callable[[], None]]:
    """
    returns a tuple of functions. calling the first starts a wait timer if not
    started, and also returns current wait time. calling the second resets the
    wait timer.
    """
    starts = []

    def waiting():
        """call me to start and check timeout."""
        if len(starts) == 0:
            starts.append(time.time())
            return 0
        return time.time() - starts[-1]

    def nowait():
        """call me to reset timeout."""
        try:
            starts.pop()
        except IndexError:
            pass

    return waiting, nowait


def signal_factory(thread_dict: MutableMapping) -> Callable[[str, int], None]:
    """
    creates a 'signaler' function that simply assigns values to a dict
    bound in enclosing scope. this is primarily intended as a simple
    inter-thread communication utility
    """

    def signaler(name, signal=0):
        if name == "all":
            for k in thread_dict.keys():
                thread_dict[k] = signal
            return
        if name not in thread_dict.keys():
            raise KeyError
        thread_dict[name] = signal

    return signaler


def kill_factory(signaler: Callable, sock: socket.socket) -> Callable:
    """
    creates a 'kill' function to signal all threads that reference the dict
    bound to `signaler` and also close `sock`.
    """

    def kill(signal: int = 0):
        """call this to shut down the server."""
        signaler("all", signal)
        return sock.close()

    return kill


def accept(
    sel: selectors.DefaultSelector, sock: socket.socket, _mask
) -> tuple[None, str, Optional[tuple], str]:
    """
    accept-connection callback for read threads. attached to keys by the
    selector.
    """
    try:
        conn, addr = sock.accept()
    except BlockingIOError:
        # TODO: do something..,
        return None, "blocking", None, "blocking"
    conn.setblocking(False)
    # tell the selector the socket is ready for a `read` callback
    sel.register(conn, selectors.EVENT_READ, curry(read)(sel))
    return None, "accept", conn.getpeername(), "ok"


def _tryread(
    conn: socket.socket, chunksize: int, eomstr: bytes
) -> tuple[Optional[bytes], str, bool]:
    """inner read-chunk-from-socket handler"""
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


def read(
    sel: selectors.DefaultSelector,
    conn: socket.socket,
    _mask,
    chunksize: int = 4096,
    eomstr: bytes = HOSTESS_EOM,
    timeout: float = 5,
    delay: float = 0.01,
) -> tuple[bytes, str, str]:
    """
    read-from-socket callback for read threads. attached to keys by the
    selector.
    """
    stream, status = b"", "unk"
    try:
        sel.unregister(conn)
        reading = True
        waiting, nowait = timeout_factory()
        while reading:
            data, status, reading = _tryread(conn, chunksize, eomstr)
            if status == "error":
                if waiting() > timeout:
                    raise TimeoutError
                time.sleep(delay)
                continue
            nowait()
            stream += data
        if status in ("stopped", "eom"):
            # tell the selector the socket is ready for an `ack` callback
            # TODO: something else for not-eom?
            sel.register(conn, selectors.EVENT_WRITE, curry(ack)(sel))
    except BrokenPipeError:
        status = "broken pipe"
    except TimeoutError:
        status = "timed out"
    except OSError as ose:
        status = str(ose)
    return stream, f"read {len(stream)}", status


def ack(
    sel: selectors.DefaultSelector,
    conn: socket.socket,
    _mask,
    ackstr: bytes = HOSTESS_ACK,
) -> tuple[None, str, str]:
    """
    receipt-of-message acknowledgement callback for read threads. attached to
    keys by the selector.
    """
    try:
        sel.unregister(conn)
        conn.send(ackstr)
        return None, "sent ack", "ok"
    except (KeyError, ValueError) as kve:
        # someone else got here first
        return None, "ack attempt", f"{kve}"


def _check_peerage(key: selectors.SelectorKey, peers: MutableMapping):
    """check already-peered lock."""
    try:
        # noinspection PyUnresolvedReferences
        peer = key.fileobj.getpeername()
        return peer, peer in peers
    except OSError:
        return None, False


def _handle_callback(callback, mask, peer, peers, peersock):
    """inner callback-handler tree for read thread"""
    if callback.__name__ == "read":
        # noinspection PyProtectedMember
        if peersock._closed or (peer is None):
            return False, "guard", False, "closed socket"
        peers[peer] = True
        try:
            # this is `read`, above
            stream, event, status = callback(peersock, mask)
        except KeyError:
            # attempting to unregister an already-unregistered conn
            return None, "guard", peer, "already unregistered"
    elif callback.__name__ == "ack":
        # this is `ack`, above
        stream, event, status = callback(peersock, mask)
        try:
            # remove peer from peering-lock dict, unless someone else
            # got to it first
            del peers[peer]
        except KeyError:
            pass
    elif callback.__name__ == "accept":
        # this is `accept`, above
        stream, event, peer, status = callback(peersock, mask)
    else:
        # who attached some weirdo function?
        stream, event, status = None, "skipped", "invalid callback"
    return stream, event, peer, status


def launch_read_thread(
    data: MutableSequence[Mapping],
    events: MutableSequence[Mapping],
    peers: MutableMapping,
    name,
    queue: MutableSequence,
    signals: MutableMapping,
    interval: float = 0.01,
) -> dict:
    """
    launch a read thread. probably should only be called by launch_tcp_server.
    must be run in a thread or it will block and be useless.
    Args:
        data: list of dicts for received data
        events: list of dicts to log events
        peers: simple lockout mechanism for peers with established reads
        name: identifier for thread
        queue: job queue, populated by selector thread
        signals: if the value corresponding to this thread's name in this
            dict is not None, thread shuts itself down.
        interval: polling rate in s

    Returns:
        An 'exit code' dict with its name and received signal.
    """
    while signals.get(name) is None:
        time.sleep(interval)
        try:
            key, mask, id_ = queue.pop()
        except IndexError:
            continue
        peer, peerage = _check_peerage(key, peers)
        callback, peersock = key.data, key.fileobj  # explanatory variables
        if (peerage is True) and (callback.__name__ != "ack"):
            # connection / read already handled
            continue
        try:
            stream, event, peer, status = _handle_callback(
                callback, mask, peer, peers, peersock
            )
            # hit exception that suggests task was already handled
            # (or unhandleable)
            if event == "guard":
                continue
        except OSError as err:
            stream, event, status = None, "oserror", str(err)
        event = {
            "event": event,
            "peer": peer,
            "status": status,
            "time": logstamp(),
            "thread": name,
            "id": id_,
            "callback": callback.__name__,
        }
        events.append(event)
        if (stream is None) or (len(stream) == 0):
            continue
        data.append(event | {"content": stream})
    return {"thread": name, "signal": signals.get(name)}


def launch_selector_thread(
    sock: socket.socket,
    queues: MutableMapping[Any, MutableSequence],
    signals: MutableMapping,
    interval: float = 0.01,
    poll: float = 1,
) -> dict:
    """
    launch the server's selector thread. probably should only be called by
    launch_tcp_server. must be run in a thread or it will block and be useless.

    Args:
        sock: listening socket for tcp server
        queues: dict of lists for associated read threads. this thread spools
            jobs to those lists from the selector.
        signals: if value of the "select" key in this dict is not None,
            the thread shuts itself down.
        interval: event spool rate in seconds.
        poll: selector polling threshold in seconds.
            (doesn't do much; this is mostly handled by the selector itself.)

    Returns:
        An 'exit code' dict with its name and received signal.
    """
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
    """
    launch a lightweight tcp server
    Args:
        host: host for socket
        port: port for socket
        read_threads: # of read threads
        interval: poll/spool interval for threads

    Returns:
        dict whose keys are:
            'threads': dict of select and read threads
            'sig': signal function to kill threads
            'kill': callable that shuts down server gracefully
            'queues': dict of current job queues
            'sock': the listening socket
            'data': list of dicts for received data
            'events': list of dicts logging events
            'exec': the thread pool executor
    """
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
    """simple utility for one-shot TCP send."""
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


def make_hostess_header(mtype="Update", is_proto=True, length=0):
    return HEADER_STRUCT.pack(
        HOSTESS_SOH, MTYPE_TO_CODE[mtype], is_proto, length
    )


def read_hostess_header(buffer):
    try:
        unpacked = HEADER_STRUCT.unpack(buffer[:15])
        assert buffer[:8] == HOSTESS_SOH
        try:
            mtype = CODE_TO_MTYPE[unpacked[1]]
        except KeyError:
            mtype = "invalid message type"
        return {"mtype": mtype, "is_proto": unpacked[2], "length": unpacked[3]}
    except (struct.error, AssertionError):
        raise IOError("invalid hostess header")


def decode_hostess_message(buffer, unpack_proto=False):
    try:
        header = read_hostess_header(buffer[:15])
    except IOError:
        return {'header': None, 'body': buffer, 'error': 'bad header'}
    if header['is_proto'] is False:
        return {'header': header, 'body': buffer[15:], 'error': 'none'}
    try:
        message = getattr(hostess_proto, header['mtype'])(buffer[15:])
    except AttributeError:
        return {'header': header, 'body': buffer[15:], 'error': 'bad mtype'}
    if unpack_proto is True:
        message = MessageToDict(message)
    return {'header': header, 'body': message, 'error': 'none'}
