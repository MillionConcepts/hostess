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
    Any, Union,
)

from google.protobuf.message import Message
from google.protobuf.json_format import MessageToDict

import hostess.station.proto.station_pb2 as hostess_proto
from hostess.utilities import curry, logstamp

HOSTESS_ACK = b"\06hostess"
HOSTESS_EOM = b"\03hostess"
HOSTESS_SOH = b"\01hostess"
# none means it's a blob/stream, not a serialized protobuf Message
CODE_TO_MTYPE = MPt({0: "none", 1: "Update", 2: "Instruction", 3: "Report"})
MTYPE_TO_CODE = MPt({v: k for k, v in CODE_TO_MTYPE.items()})
HEADER_STRUCT = struct.Struct("<8sBL")


def timeout_factory(
    raise_timeout: bool = True, timeout: float = 5
) -> tuple[Callable[[], int], Callable[[], None]]:
    """
    returns a tuple of functions. calling the first starts a wait timer if not
    started, and also returns current wait time. calling the second resets the
    wait timer.

    Args:
        raise_timeout: if True, raises timeout if waiting > timeout.
        timeout: timeout in seconds, used only if raise_timeout is True
    """
    starts = []

    def waiting():
        """call me to start and check/raise timeout."""
        if len(starts) == 0:
            starts.append(time.time())
            return 0
        interval = time.time() - starts[-1]
        if (raise_timeout is True) and (interval > timeout):
            raise TimeoutError
        return interval

    def unwait():
        """call me to reset timeout."""
        try:
            starts.pop()
        except IndexError:
            pass

    return waiting, unwait


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
    """inner read-individual-chunk-from-socket handler for `read`"""
    status, reading = "streaming", True
    try:
        data = conn.recv(chunksize)
    except OSError as ose:
        if "temporarily" not in str(ose):
            raise
        if eomstr is None:
            raise
        return None, "unavailable", True
    if data == b"":
        status, reading = "stopped", False
    if eomstr is not None:
        if data.endswith(eomstr):
            status, reading = "eom", False
    return data, status, reading


def _trydecode(decoder, stream):
    """inner stream-decode handler function for `read`"""
    nbytes = len(stream)
    try:
        stream = decoder(stream)
        status = f"decoded {nbytes}"
    except KeyboardInterrupt:
        raise
    except Exception as ex:
        status = f"decode error;{nbytes};{type(ex)};{ex}"
    return status, stream


def read(
    sel: selectors.DefaultSelector,
    conn: socket.socket,
    _mask,
    chunksize: int = 4096,
    eomstr: bytes = HOSTESS_EOM,
    timeout: float = 5,
    delay: float = 0.01,
    decoder: Optional[Callable] = None
) -> tuple[bytes, str, str]:
    """
    read-from-socket callback for read threads. attached to keys by `sel`.
    """
    stream, status = b"", "unk"
    try:
        sel.unregister(conn)
        reading = True
        waiting, unwait = timeout_factory(timeout=timeout)
        while reading:
            data, status, reading = _tryread(conn, chunksize, eomstr)
            if status == "unavailable":
                waiting()
                time.sleep(delay)
                continue
            unwait()
            stream += data
        if status in ("stopped", "eom"):
            # tell the selector the socket is ready for an `ack` callback
            # TODO: something else for not-eom?
            sel.register(conn, selectors.EVENT_WRITE, curry(ack)(sel))
            if decoder is not None:
                status, stream = _trydecode(decoder, status)
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
    decoder: Optional[Callable] = None
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
        decoder: optional callable used to decode received messages

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
    signals: MutableMapping[Union[int, str], Optional[str]],
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


def launch_tcp_server(
    host, 
    port, 
    read_threads=4, 
    interval=0.01, 
    decoder: Optional[Callable] = None
):
    """
    launch a lightweight tcp server
    Args:
        host: host for socket
        port: port for socket
        read_threads: # of read threads
        interval: poll/spool interval for threads
        decoder: optional callable used to decode received messages

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
        threads["select"] = executor.submit(
            launch_selector_thread, sock, queues, signals, interval, 1
        )
        for ix in range(read_threads):
            threads[ix] = executor.submit(
                launch_read_thread,
                data,
                events,
                peers,
                ix,
                queues[ix],
                signals,
                interval,
                decoder
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


# TODO: consider benchmarking pos-only / unnested versions
def make_header(mtype="Blob", length=0) -> bytes:
    """create a hostess header."""
    return HEADER_STRUCT.pack(HOSTESS_SOH, MTYPE_TO_CODE[mtype], length)


def make_comm(body: Union[bytes, Message]) -> bytes:
    """
    create a hostess comm from a buffer or a protobuf Message.
    automatically attach header and footer.
    """
    if "SerializeToString" in dir(body):
        # i.e., it's a protobuf Message
        buf = body.SerializeToString()
        header_kwargs = {'mtype': body.__class__.__name__, 'length': len(buf)}
    else:
        header_kwargs, buf = {'mtype': "none", 'length': len(body)}, body
    return make_header(**header_kwargs) + buf + HOSTESS_EOM


def read_header(buffer: bytes) -> dict[str, Union[str, bool, int]]:
    """attempt to read a hostess header from the first 13 bytes of `buffer`."""
    try:
        unpacked = HEADER_STRUCT.unpack(buffer[:14])
        assert buffer[:8] == HOSTESS_SOH
        try:
            mtype = CODE_TO_MTYPE[unpacked[1]]
        except KeyError:
            mtype = "invalid message type"
        return {"mtype": mtype, "length": unpacked[2]}
    except (struct.error, AssertionError):
        raise IOError("invalid hostess header")


def read_comm(
    buffer: bytes, unpack_proto: bool = False
) -> dict[str, Union[dict, bytes, Message, str]]:
    """
    read a hostess comm. if the header says the body is a protobuf, attempt to
    decode it as a hostess.station Message. if unpack_proto is True, convert
    it to a dict. return a dict containing the decoded header, the
    (possibly decoded) body, and any errors.
    """
    try:
        header = read_header(buffer[:HEADER_STRUCT.size])
    except IOError:
        return {'header': None, 'body': buffer, 'err': 'header'}
    err, body = [], buffer[HEADER_STRUCT.size:]
    if body.endswith(HOSTESS_EOM):
        body = body[:-len(HOSTESS_EOM)]
    if len(body) != header['length']:
        err.append('length')
    if header['mtype'] == 'none':
        return {'header': header, 'body': body, 'err': ';'.join(err)}
    try:
        # the value of the 'mtype' key should correspond to a hostess.station
        # protocol buffer class
        message_class = getattr(hostess_proto, header['mtype'])
        message: Message = message_class.FromString(body)
    except AttributeError:
        err.append('mtype')
        return {'header': header, 'body': body, 'err': ';'.join(err)}
    # TODO: handling for bad decode
    if unpack_proto is True:
        message = MessageToDict(message)
    return {'header': header, 'body': message, 'err': ';'.join(err)}
