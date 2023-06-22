from __future__ import annotations

import atexit
import selectors
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from itertools import cycle
from typing import Optional, Callable

from hostess.station.comm import make_comm, read_header, read_comm
from hostess.station.messages import Mailbox

from hostess.utilities import (
    curry,
    logstamp,
    timeout_factory,
    signal_factory,
    trywrap,
)


# acknowledgement, end-of-message, start-of-header codes
# one-byte-wide codes for Message type of comm body.
# "none" means the comm body is not a serialized protobuf Message.


class TCPTalk:
    """lightweight multithreaded tcp server."""

    def __init__(
        self,
        host,
        port,
        n_threads=4,
        poll=0.01,
        decoder: Callable = read_comm,
        ackcheck: Optional[Callable] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        lock: Optional[threading.Lock] = None,
        chunksize: int = 16384,
        delay: float = 0.01,
        timeout: int = 10,
    ):
        """
        Args:
            host: host for socket
            port: port for socket
            n_threads: # of i/o threads
            poll: poll/spool delay for threads
            decoder: optional callable used to decode received messages
            ackcheck: callable for inserting message responses -- this can be
                used to attach a Station's responder rules to the server
            executor: optional ThreadPoolExecutor, if the server should run in
                existing thread pool
            lock: optional lock, if the tcp server should be subject to an
                external lockout
            chunksize: chunk size for reading responses from socket
            delay: time to wait before checking socket again on bad reads
            timeout: timeout on socket
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        atexit.register(self.sock.close)
        self.status = "initializing"
        self.timeout, self.delay, self.chunksize = timeout, delay, chunksize
        self.poll, self.decoder, self.ackcheck = poll, decoder, ackcheck
        self.sel = selectors.DefaultSelector()
        try:
            self.sock.bind((host, port))
            self.sock.listen()
            self.sock.setblocking(False)
            if executor is None:
                executor = ThreadPoolExecutor(n_threads + 1)
            self.exec, self._lock = executor, lock
            self.threads, self.events = {}, []
            self.data, self.peers = Mailbox([]), {}
            self.queues = {i: [] for i in range(n_threads)}
            self.signals = {i: None for i in range(n_threads)} | {
                "select": None
            }
            self.sig = signal_factory(self.signals)
            self.threads["select"] = executor.submit(self.launch_selector)
            for ix in range(n_threads):
                self.threads[ix] = executor.submit(self.launch_io, ix)
            self.status = "running"
        except Exception as exception:
            self.sock.close()
            self.status = "crashed"
            self.kill()
            raise

    def kill(self, signal: int = 0):
        """call this to shut down the server."""
        self.sock.close()
        if "sig" in dir(self):
            # won't be present if we encountered an error on init
            self.sig("all", signal)
        self.status = "terminated"

    def tend(self):
        if self.status == "initializing":
            return
        threads = tuple(self.threads.items())
        crashed_threads = []
        for k, v in threads:
            if v._state == "RUNNING":
                continue
            self.sig(k, 0)
            time.sleep(self.poll * 2)
            self.sig(k, None)
            crashed_threads.append(self.threads.pop(k).exception())
            if k == "select":
                self.threads["select"] = trywrap(
                    self.launch_selector, "select"
                )()
            else:
                self.threads[k] = trywrap(self.launch_io, k)(k)
        self.status = "running"
        return crashed_threads

    def _get_locked(self):
        if self._lock is None:
            return False
        return self._lock.locked()

    def _set_locked(self, _val):
        raise AttributeError("server is not directly lockable")

    locked = property(_get_locked, _set_locked)

    def _handle_callback(self, callback, peer, peersock):
        """inner callback-handler tree for i/o threads"""
        if callback.__name__ == "_read":
            # noinspection PyProtectedMember
            if peersock._closed or (peer is None):
                return False, "guard", False, "closed socket"
            self.peers[peer] = True
            try:
                # callback is self._read
                stream, event, status = callback(peersock)
            except KeyError:
                # attempting to unregister an already-unregistered conn
                return None, "guard", peer, "already unregistered"
        elif callback.__name__ == "_ack":
            # callback is self._ack
            stream, event, status = callback(peersock)
            try:
                # remove peer from peering-lock dict, unless someone else
                # got to it first
                del self.peers[peer]
            except KeyError:
                pass
        elif callback.__name__ == "_accept":
            # callback is self._accept
            stream, event, peer, status = callback(peersock)
        else:
            # who attached some weirdo function?
            stream, event, status = None, "skipped", "invalid callback"
        return stream, event, peer, status

    def launch_selector(self):
        """
        launch the server's selector thread.
        Returns:
            An dict with name, any received signal, and any exception.
        """
        id_, cycler = 0, cycle(self.queues.keys())
        self.sel.register(self.sock, selectors.EVENT_READ, self._accept)
        while self.signals.get("select") is None:
            try:
                events = self.sel.select(1)
            except TimeoutError:
                continue
            for key, _mask in events:
                target = next(cycler)
                self.queues[target].append((key, id_))
                id_ += 1
            time.sleep(self.poll)

    def launch_io(self, name):
        """
        launch a read thread in this server's executor.
        must be run in a thread or it will block and be useless.
        Args:
            name: identifier for thread

        Returns:
            dict with name, any received signal, and any exception raised.
        """
        while self.signals.get(name) is None:
            time.sleep(self.poll)
            try:
                key, id_ = self.queues[name].pop()
            except IndexError:
                continue
            peer, peerage = self._check_peerage(key)
            callback, peersock = key.data, key.fileobj  # explanatory variables
            if (peerage is True) and (callback.__name__ != "_ack"):
                # connection / read already handled
                continue
            if self.locked and callback.__name__ != "_ack":
                continue
            try:
                stream, event, peer, status = self._handle_callback(
                    callback, peer, peersock
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
            self.events.append(event)
            if (stream is None) or (len(stream) == 0):
                continue
            self.data.append(event | {"content": stream})

    def _accept(
        self, sock: socket.socket
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
        self.sel.register(conn, selectors.EVENT_READ, self._read)
        return None, "accept", conn.getpeername(), "ok"

    def _read(self, conn: socket.socket) -> tuple[bytes, str, str]:
        """
        read-from-socket callback for read threads. attached to keys by `sel`.
        """
        event, status, stream = None, "unk", b""
        try:
            self.sel.unregister(conn)
            waiting, unwait = timeout_factory(timeout=self.timeout)
            stream, length = conn.recv(self.chunksize), None
            length = read_header(stream)['length']
            while waiting() >= 0:  # syntactic handwaving.. breaks w/exception.
                if (length is not None) and (len(stream) >= length):
                    break
                data, status = self._tryread(conn)
                if status == "unavailable":
                    time.sleep(self.delay)
                    continue
                stream += data
                unwait()
            # tell the selector the socket is ready for an `ack` callback
            stream, event, status = self._trydecode(stream)
            self.sel.register(
                conn, selectors.EVENT_WRITE, curry(self._ack)(stream)
            )
        except BrokenPipeError:
            status = "broken pipe"
        except TimeoutError:
            status = "timed out"
        except (IOError, KeyError, OSError) as err:
            status = f"{type(err)}: {str(err)}"
        event = f"read {len(stream)}" if event is None else event
        return stream, event, status

    def _tryread(
        self, conn: socket.socket
    ) -> tuple[Optional[bytes], str]:
        """inner read-individual-chunk-from-socket handler for `read`"""
        status = "streaming"
        try:
            data = conn.recv(self.chunksize)
        except OSError as ose:
            if "temporarily" not in str(ose):
                raise
            return None, "unavailable"
        return data, status

    def _ack(self, data, conn: socket.socket) -> tuple[None, str, str]:
        """
        receipt-of-message acknowledgement callback for read threads.
        attached to keys by the selector.
        Args:
            conn: open socket to peer.
        """
        try:
            self.sel.unregister(conn)
            response, status = make_comm(b""), "sent_ack"
            if self.ackcheck is not None:
                response, status = self.ackcheck(conn, data)
            if response is None:
                return None, status, ""
            conn.sendall(response)
            return None, status, "ok"
        except (KeyError, ValueError) as kve:
            # someone else got here first
            return None, "ack attempt", f"{kve}"

    def _trydecode(self, stream):
        """inner stream-decode handler function for `read`"""
        nbytes = len(stream)
        try:
            stream = self.decoder(stream)
            event, status = f"decoded {nbytes}", "ok"
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            event, status = f"read {nbytes}", f"decode error;{type(ex)};{ex}"
        return stream, event, status

    def _check_peerage(self, key: selectors.SelectorKey):
        """check already-peered lock."""
        try:
            # noinspection PyUnresolvedReferences
            peer = key.fileobj.getpeername()
            return peer, peer in self.peers
        except OSError:
            return None, False


def tcp_send(
    data, host, port, timeout=10, delay=0, chunksize=None, headerread=None
):
    """simple utility for one-shot TCP send."""
    sockname = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.settimeout(timeout)
            sock.connect((host, port))
            sockname = sock.getsockname()
            if (delay > 0) or (chunksize is not None):
                chunksize = 16384 if chunksize is None else chunksize
                while len(data) > 0:
                    data, chunk = data[chunksize:], data[:chunksize]
                    sock.send(chunk)
                    time.sleep(delay)
            else:
                sock.sendall(data)
                response = read_from_socket(headerread, sock, timeout)
                return response, sockname
        except TimeoutError:
            return "timeout", sockname
        except ConnectionError:
            return "connection refused", sockname
        finally:
            sock.close()  # TODO: redundant with context manager?


def read_from_socket(headerread, sock, timeout):
    # TODO, maybe: move _tryread?
    waiting, unwait = timeout_factory(timeout=timeout)
    data = sock.recv(16384)
    response, length = data, None
    try:
        if headerread is not None:
            try:
                length = headerread(response)['length']
            except (IOError, KeyError):
                pass
        while True:
            if (length is not None) and (len(response) >= length):
                break
            data = sock.recv(16384)
            if len(data) == 0:
                if length is None:
                    break
                waiting()
                time.sleep(0.01)
                continue
            response += data
            unwait()
    finally:
        sock.close()
    return response


# TODO: consider benchmarking pos-only / unnested versions
@wraps(tcp_send)
def stsend(data, host, port, timeout=10, delay=0, chunksize=None):
    """wrapper for tcpsend that autoencodes data as hostess comms."""
    return tcp_send(
        make_comm(data),
        host,
        port,
        timeout,
        delay,
        chunksize,
        headerread=read_header
    )
