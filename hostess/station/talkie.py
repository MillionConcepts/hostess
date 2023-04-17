import atexit
import selectors
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from itertools import cycle
from types import MappingProxyType as MPt
from typing import (
    Optional,
    Callable,
    MutableMapping,
    MutableSequence,
    Mapping,
    Any,
    Union,
)

from google.protobuf.message import Message

import hostess.station.proto.station_pb2 as hostess_proto
from hostess.station.proto_utils import m2d
from hostess.utilities import curry, logstamp

# acknowledgement, end-of-message, start-of-header codes
HOSTESS_ACK = b"\06hostess"
HOSTESS_EOM = b"\03hostess"
HOSTESS_SOH = b"\01hostess"
# one-byte-wide codes for Message type of comm body.
# "none" means the comm body is not a serialized protobuf Message.
CODE_TO_MTYPE = MPt(
    {0: "none", 1: "Update", 2: "Instruction", 3: "TaskReport"}
)
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
        delay = time.time() - starts[-1]
        if (raise_timeout is True) and (delay > timeout):
            raise TimeoutError
        return delay

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
    sel: selectors.DefaultSelector, sock: socket.socket
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
        event, status = f"decoded {nbytes}", "ok"
    except KeyboardInterrupt:
        raise
    except Exception as ex:
        event, status = f"read {nbytes}", f"decode error;{type(ex)};{ex}"
    return stream, event, status


def default_ack(
    sel: selectors.DefaultSelector,
    conn: socket.socket,
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


def read(
    sel: selectors.DefaultSelector,
    conn: socket.socket,
    decoder: Optional[Callable],
    ack: Callable = default_ack,
    chunksize: int = 4096,
    eomstr: bytes = HOSTESS_EOM,
    timeout: float = 5,
    delay: float = 0.01,
) -> tuple[bytes, str, str]:
    """
    read-from-socket callback for read threads. attached to keys by `sel`.
    """
    event, stream, status = None, b"", "unk"
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
                stream, event, status = _trydecode(decoder, stream)
    except BrokenPipeError:
        status = "broken pipe"
    except TimeoutError:
        status = "timed out"
    except OSError as ose:
        status = str(ose)
    event = f"read {len(stream)}" if event is None else event
    return stream, event, status


def _check_peerage(key: selectors.SelectorKey, peers: MutableMapping):
    """check already-peered lock."""
    try:
        # noinspection PyUnresolvedReferences
        peer = key.fileobj.getpeername()
        return peer, peer in peers
    except OSError:
        return None, False


def _handle_callback(callback, peer, peers, peersock, decoder, ack):
    """inner callback-handler tree for read thread"""
    if callback.__name__ == "read":
        # noinspection PyProtectedMember
        if peersock._closed or (peer is None):
            return False, "guard", False, "closed socket"
        peers[peer] = True
        try:
            # this is `read`, above
            stream, event, status = callback(peersock, decoder, ack)
        except KeyError:
            # attempting to unregister an already-unregistered conn
            return None, "guard", peer, "already unregistered"
    elif callback.__name__ == ack.__name__:
        # this is `ack`
        stream, event, status = callback(peersock)
        try:
            # remove peer from peering-lock dict, unless someone else
            # got to it first
            del peers[peer]
        except KeyError:
            pass
    elif callback.__name__ == "accept":
        # this is `accept`, above
        stream, event, peer, status = callback(peersock)
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
    poll: float = 0.01,
    decoder: Optional[Callable] = None,
    ack: Callable = default_ack
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
        poll: polling delay in s
        decoder: optional callable used to decode received messages
        ack: acknowledgment callback

    Returns:
        An 'exit code' dict with its name and received signal.
    """
    while signals.get(name) is None:
        time.sleep(poll)
        try:
            key, id_ = queue.pop()
        except IndexError:
            continue
        peer, peerage = _check_peerage(key, peers)
        callback, peersock = key.data, key.fileobj  # explanatory variables
        if (peerage is True) and (callback.__name__ != ack.__name__):
            # connection / read already handled
            continue
        try:
            stream, event, peer, status = _handle_callback(
                callback, peer, peers, peersock, decoder, ack
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
    poll: float = 0.01,
    selector_poll: float = 1,
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
        poll: event spool delay in seconds.
        selector_poll: selector polling delay in seconds.
            (doesn't do much; this is mostly handled by the selector itself.)

    Returns:
        An 'exit code' dict with its name and received signal.
    """
    id_, cycler = 0, cycle(queues.keys())
    with selectors.DefaultSelector() as sel:
        sel.register(sock, selectors.EVENT_READ, curry(accept)(sel))
        while signals.get("select") is None:
            try:
                events = sel.select(selector_poll)
            except TimeoutError:
                continue
            for key, _mask in events:
                target = next(cycler)
                queues[target].append((key, id_))
                id_ += 1
            time.sleep(poll)
    return {"thread": "select", "signal": signals.get("select")}


def launch_tcp_server(
    host,
    port,
    n_threads=4,
    poll=0.01,
    decoder: Optional[Callable] = None,
    ack: Callable = default_ack
) -> tuple[dict[str], list[dict], list[dict]]:
    """
    launch a lightweight tcp server
    Args:
        host: host for socket
        port: port for socket
        n_threads: # of read threads
        poll: poll/spool delay for threads
        decoder: optional callable used to decode received messages
        ack: callable for responding to messages -- this can be used
            to attach a Station's responder rules to the server

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
        executor = ThreadPoolExecutor(n_threads + 1)
        threads, events, data, peers = {}, [], [], {}
        queues = {i: [] for i in range(n_threads)}
        signals = {i: None for i in range(n_threads)} | {"select": None}
        threads["select"] = executor.submit(
            launch_selector_thread, sock, queues, signals, poll, 1
        )
        for ix in range(n_threads):
            threads[ix] = executor.submit(
                launch_read_thread,
                data,
                events,
                peers,
                ix,
                queues[ix],
                signals,
                poll,
                decoder,
                ack
            )
        serverdict = {
            "threads": threads,
            "sig": signal_factory(signals),
            "sock": sock,
            "kill": kill_factory(signal_factory(signals), sock),
            "exec": executor,
            "queues": queues,
        }
        return serverdict, events, data
    except Exception:
        sock.close()
        raise


def tcp_send(data, host, port, timeout=10, delay=0, chunksize=None):
    """simple utility for one-shot TCP send."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        if (delay > 0) or (chunksize is not None):
            chunksize = 4096 if chunksize is None else chunksize
            while len(data) > 0:
                data, chunk = data[chunksize:], data[:chunksize]
                sock.send(chunk)
                time.sleep(delay)
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
        header_kwargs = {"mtype": body.__class__.__name__, "length": len(buf)}
    else:
        header_kwargs, buf = {"mtype": "none", "length": len(body)}, body
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
        header = read_header(buffer[: HEADER_STRUCT.size])
    except IOError:
        return {"header": None, "body": buffer, "err": "header"}
    err, body = [], buffer[HEADER_STRUCT.size :]
    if body.endswith(HOSTESS_EOM):
        body = body[: -len(HOSTESS_EOM)]
    if len(body) != header["length"]:
        err.append("length")
    if header["mtype"] == "none":
        return {"header": header, "body": body, "err": ";".join(err)}
    try:
        # the value of the 'mtype' key should correspond to a hostess.station
        # protocol buffer class
        message_class = getattr(hostess_proto, header["mtype"])
        message: Message = message_class.FromString(body)
    except AttributeError:
        err.append("mtype")
        return {"header": header, "body": body, "err": ";".join(err)}
    # TODO: handling for bad decode
    if unpack_proto is True:
        message = m2d(message)
    return {"header": header, "body": message, "err": ";".join(err)}


@wraps(tcp_send)
def stsend(data, host, port, timeout=10, delay=0, chunksize=None):
    """wrapper for tcpsend that autoencodes data as hostess comms."""
    return tcp_send(make_comm(data), host, port, timeout, delay, chunksize)


@wraps(launch_tcp_server)
def stlisten(host, port, n_threads: int = 4, poll: float = 0.01):
    """wrapper for launch_tcp_server that autodecodes data as hostess comms."""
    return launch_tcp_server(host, port, n_threads, poll, decoder=read_comm)
