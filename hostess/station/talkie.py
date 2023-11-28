from __future__ import annotations

import atexit
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from itertools import chain, cycle
import selectors
import socket
import threading
import time
from typing import Optional, Callable, Union, Any

from hostess.station.comm import make_comm, read_header, read_comm
from hostess.station.messages import Mailbox
from hostess.utilities import (
    curry,
    logstamp,
    signal_factory,
    timeout_factory,
)


class TCPTalk:
    """lightweight multithreaded tcp server."""

    def __init__(
        self,
        host: str,
        port: int,
        n_threads: int = 4,
        poll: float = 0.01,
        decoder: Optional[Callable] = read_comm,
        ackcheck: Optional[Callable] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        lock: Optional[threading.Lock] = None,
        chunksize: int = 16384,
        delay: float = 0.01,
        timeout: int = 10,
    ):
        """
        Note: `TCPTalk` immediately starts running when initialized.

        Args:
            host: hostname for server's socket, either an ip address or a
                resolvable name like "localhost"
            port: port number for server's socket
            n_threads: number of i/o threads. server will always launch
                n_threads + 1 threads; the +1 is its selector thread.
            poll: poll/spool delay for threads
            decoder: optional callable used to decode received messages
            ackcheck: callable for inserting message responses -- this can be
                used to attach a Station's responder rules to the server
            executor: optional ThreadPoolExecutor, if the server should run in
                existing thread pool
            lock: optional lock, if the tcp server should be subject to an
                external lockout
            chunksize: chunk size for reading responses from socket
            delay: time to wait before checking socket again after failed read
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
            self.data, self.peers = Mailbox(), {}
            self.queues = {i: [] for i in range(n_threads)}
            self.signals = {i: None for i in range(n_threads)} | {
                "select": None
            }
            self.sig = signal_factory(self.signals)
            self.threads["select"] = executor.submit(self.launch_selector)
            for ix in range(n_threads):
                self.threads[ix] = executor.submit(self.launch_io, ix)
            self.status = "running"
        except Exception as _ex:
            self.sock.close()
            self.status = "crashed"
            self.kill()
            raise

    def kill(self, signal: int = 0):
        """
        immediately shut down, closing the server's socket and attempting to
        terminate all its threads.

        Args:
            signal: termination signal to send to threads. changing this
                number does nothing special by default and is intended for
                subclasses or application-specific purposes.
        """
        self.sock.close()
        if "sig" in dir(self):
            # won't be present if we encountered an error on init
            self.sig("all", signal)
        self.status = "terminated"

    def tend(self) -> Optional[list]:
        """
        check on all threads we believe to be running. If any of them aren't,
        relaunch them. Never called automatically.

        Returns:
            None, if server is still initializing. otherwise, list of
                Exceptions raised by crashed threads (empty if none crashed).
        """
        if self.status == "initializing":
            return
        threads = tuple(self.threads.items())
        crashed_threads = []
        for k, v in threads:
            # will be dict if it is a crashed thread running in trywrap
            if not isinstance(v, dict) and (v._state == "RUNNING"):
                continue
            self.sig(k, 0)
            time.sleep(self.poll * 2)
            self.sig(k, None)
            thread = self.threads.pop(k, None)
            if isinstance(thread, dict):
                exception = thread["exception"]
            elif thread is not None:
                exception = thread.exception()
            else:
                exception = None
            crashed_threads.append(exception)
            if k == "select":
                self.threads["select"] = self.exec.submit(self.launch_selector)
            else:
                self.threads[k] = self.exec.submit(self.launch_io, k)
        self.status = "running"
        return crashed_threads

    def _get_locked(self) -> bool:
        """getter for self.locked"""
        if self._lock is None:
            return False
        return self._lock.locked()

    def _set_locked(self, _val: bool):
        """
        intentionally nonfunctional setter for self.locked. Always raises
        AttributeError.
        """
        raise AttributeError("server is not directly lockable")

    locked = property(_get_locked, _set_locked)
    """
    is the server locked, preventing it from communicating with peers?
    note that TCPTalk never locks itself. Its optional lockout behavior is
    intended to be handled by some sort of lock object shared with a handler 
    application, for cases in which something needs locks for synchronization.
    """

    def _handle_callback(
        self,
        callback: Callable,
        peername: Optional[str],
        peersock: socket.socket,
    ) -> tuple[Any, str, Optional[tuple[str, int]], str]:
        """
        inner callback-handler tree for i/o threads. should only ever be
        called from an io thread loop (`TCPTalk.launch_io()`).

        Args:
            callback: one of self._read, self._ack, or self._accept. attached
                to peersock by self.sel, queued by a call to self.sel.register
                in an io or selector thread.
            peername: name of peer, if known (tuple of (ip, fileno)).
            peersock: open socket to peer

        Returns:
            stream: decoded or raw bytes read from socket, if any
            event: description of event, primarily for logging
            peername: existing or newly-discovered peername (ip, fileno), or
                None if we still don't know it
            status: code for event, primarily for control flow
        """
        if callback.__name__ == "_read":
            self.peers[peername] = True
            try:
                # callback is self._read
                stream, event, status = callback(peersock)
            except KeyError:
                # attempting to unregister an already-unregistered conn
                return None, "guard", peername, "already unregistered"
        elif callback.__name__ == "_ack":
            # callback is self._ack (usually a partially-evaluated version of
            # it constructed in self._read())
            stream, event, status = callback(peersock)
            # remove peer from peering-lock dict
            self.peers.pop(peername, None)
        elif callback.__name__ == "_accept":
            # callback is self._accept
            stream, event, peername, status = callback(peersock)
        else:
            # who attached some weirdo function?
            stream, event, status = None, "skipped", "invalid callback"
        return stream, event, peername, status

    def queued_descriptors(self) -> set[int]:
        """
        Returns:
             set of all file descriptors for currently-queued sockets.
                  Primarily for selector thread loop but can also be used
                  diagnostically.
        """
        return {s[0].fd for s in chain.from_iterable(self.queues.values())}

    # TODO: should this be running in @trywrap?
    def launch_selector(self):
        """launch the server's selector thread."""
        id_, cycler = 0, cycle(self.queues.keys())
        try:
            self.sel.register(self.sock, selectors.EVENT_READ, self._accept)
        except KeyError:  # will occur on relaunch
            pass
        while self.signals.get("select") is None:
            try:
                events = self.sel.select(1)
            except TimeoutError:
                continue
            queued = self.queued_descriptors()
            for key, _mask in events:
                # try to ensure we don't have a million pending events
                if key.fd in queued:
                    continue
                target = next(cycler)
                self.queues[target].append((key, id_))
                id_ += 1
            time.sleep(self.poll)

    # TODO: should this be running in @trywrap?
    def launch_io(self, name: Union[str, int]):
        """
        launch a read thread in this server's executor.
        must be run in a thread or it will block and be useless.

        Args:
            name: identifier for thread
        """
        while self.signals.get(name) is None:
            time.sleep(self.poll)
            try:
                key, id_ = self.queues[name].pop()
            except IndexError:
                continue
            # noinspection PyProtectedMember
            peername, peerage = self._check_peerage(key)
            callback, peersock = key.data, key.fileobj  # explanatory variables
            if (peerage is True) and (callback.__name__ != "_ack"):
                # connection / read already handled
                continue
            if self.locked and callback.__name__ != "_ack":
                continue
            try:
                stream, event, peername, status = self._handle_callback(
                    callback, peername, peersock
                )
                # task was already handled (or unhandleable)
                if event == "guard":
                    continue
            except OSError as err:
                stream, event, status = None, "oserror", str(err)
            event = {
                "event": event,
                "peer": peername,
                "status": status,
                "time": logstamp(),
                "thread": name,
                "id": id_,
                "callback": callback.__name__,
            }
            self.events.append(event)
            if (stream is None) or (len(stream) == 0):
                continue
            if not isinstance(stream["body"], bytes):  # control codes, etc.
                self.data.append(event | {"content": stream})

    def _accept(
        self, sock: socket.socket
    ) -> tuple[None, str, Optional[tuple[str, int]], str]:
        """
        accept-connection callback for i/o threads.

        Args:
            sock: TCP socket we've received a connection request on. in normal
                operation, this will always be self.sock.

        Returns:
            stream: always None. (present for signature compatibility)
            event: "blocking" if accepting connection would block; "accept"
                on successful accept
            peername: name of peer on successful accept (tuple of ip address,
                fileno). None if blocking (because peer name is not known).
            status: "blocking on self" if blocking, "ok" on successful accept
        """
        try:
            conn, addr = sock.accept()
        except BlockingIOError:
            return None, "blocking", None, "blocking on self"
        conn.setblocking(False)
        # tell the selector the socket is ready for a `read` callback
        self.sel.register(conn, selectors.EVENT_READ, self._read)
        return None, "accept", conn.getpeername(), "ok"

    def _trydecode(self, stream: bytes) -> tuple[Any, str, str]:
        """
        inner stream-decode handler function for self.read().

        Args:
            stream: bytes received from peer.

        Returns:
            stream: output of self.decoder, if successful; undecoded stream
                if not.
            event: "decoded {nbytes}" on successful decode; "read {nbytes}"
                on failed decode
            status: "ok" if successful; description of decode error if not
        """
        nbytes = len(stream)
        try:
            stream = self.decoder(stream)
            event, status = f"decoded {nbytes}", "ok"
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            event, status = f"read {nbytes}", f"decode error;{type(ex)};{ex}"
        return stream, event, status

    def _tryread(self, peersock: socket.socket) -> tuple[Optional[bytes], str]:
        """
        inner read-individual-chunk-from-socket handler for `read`

        Args:
            peersock: open socket to read chunk from

        Returns:
            data: bytes read from peersock, if any
            status: "streaming" if receive operation was successful;
                "unavailable" if peer is temporarily unavailable

        Raises:
            OSError: for any OSError other than temporary unavailability
        """
        status = "streaming"
        try:
            data = peersock.recv(self.chunksize)
        except OSError as ose:
            if "temporarily" not in str(ose):
                raise
            return None, "unavailable"
        return data, status

    def _read(self, peersock: socket.socket) -> tuple[Any, str, str]:
        """
        read-from-socket callback for i/o threads.

        Args:
            peersock: open socket to peer

        Returns:
            stream: output of self.decoder on successful read and decode;
                bytes read from peersock on successful read and failed decode;
                None on failed read
            event: description of read/decode length or read/decode error
            status: "ok" if everything went well, error code if not
        """
        event, status, stream = None, "unk", b""
        try:
            self.sel.unregister(peersock)
            waiting, unwait = timeout_factory(timeout=self.timeout)
            stream, length = peersock.recv(self.chunksize), None
            length = read_header(stream)["length"]
            while waiting() >= 0:  # syntactic handwaving. breaks w/exception.
                if (length is not None) and (len(stream) >= length):
                    break
                data, status = self._tryread(peersock)
                if status == "unavailable":
                    time.sleep(self.delay)
                    continue
                stream += data
                unwait()
            # tell the selector the socket is ready for an `ack` callback
            stream, event, status = self._trydecode(stream)
            self.sel.register(
                peersock, selectors.EVENT_WRITE, curry(self._ack)(stream)
            )
        except BrokenPipeError:
            self.peers.pop(peersock.getpeername(), None)
            status = "broken pipe"
        except TimeoutError:
            self.peers.pop(peersock.getpeername(), None)
            status = "timed out"
        except KeyError as ke:
            if "is not registered" in str(ke):
                status = f"{peersock} already unregistered"
            else:
                raise
        except BlockingIOError:
            self.peers.pop(peersock.getpeername(), None)
            status = f"cleared blocking socket {peersock.getpeername()}"
        except (IOError, OSError) as err:
            status = f"{type(err)}: {str(err)}"
        event = f"read {len(stream)}" if event is None else event
        return stream, event, status

    def _ack(
        self, data: Any, peersock: socket.socket
    ) -> tuple[None, str, str]:
        """
        acknowledgement callback for read threads. calls self.ackcheck if we
        have one; if we don't, just sends an empty comm to the peer.

        Args:
            data: decoded object or raw bytes read from peersock.
                this is typically not passed directly but rather curried into
                a copy of _ack() constructed in _read().
            peersock: open socket to peer.

        Returns:
            stream: always None. for signature compatibility
            event: "ack attempt" if attempt failed; for successful ack, if
                we have an ackcheck function, whatever status code it returned,
                or "sent_ack" if we don't have an ackcheck
            status: empty string if we have no response; "ok" on successful
                ack; description of error on failed ack
        """
        try:
            self.sel.unregister(peersock)
            response, event = make_comm(b""), "sent_ack"
            if self.ackcheck is not None:
                response, event = self.ackcheck(peersock, data)
            if response is None:
                return None, event, ""
            waiting, unwait = timeout_factory(timeout=self.timeout)
            while len(response) > 0:
                try:
                    # attempt to send chunk of designated size...
                    payload = response[: self.chunksize]
                    sent = peersock.send(payload)
                    unwait()
                    # ...but only truncate by amount we successfullly sent
                    response = response[sent:]
                except BrokenPipeError:
                    # don't need to release peerlock here because we always
                    # release it after _ack
                    return None, "ack attempt", "broken pipe"
                except OSError:
                    waiting()
                    time.sleep(self.delay)
            time.sleep(0.1)
            return None, event, "ok"
        except (KeyError, ValueError) as kve:
            if "is not registered" in str(kve):
                # someone else got here firs
                return None, "ack attempt", f"{kve}"
            raise
        except TimeoutError as te:
            return None, "ack attempt", f"{te}"

    def _check_peerage(
        self, key: Union[selectors.SelectorKey, socket.socket]
    ) -> tuple[Optional[tuple[str, int]], bool]:
        """
        check to see if another thread is already handling a peer. essentially
        a synchronization lock function.

        Args:
            key: socket to peer, or selector key wrapping socket

        Returns:
            peername: tuple of (ip, fileno), or None if connection on socket is
                not yet accepted or socket is already closed
            peered: True if another thread is handling the peer, False if not
                (including if the socket is pending/unreadable)
        """
        try:
            if hasattr(key, "fileobj"):
                # noinspection PyUnresolvedReferences
                peer = key.fileobj.getpeername()
            else:
                peer = key.getpeername()
            return peer, peer in self.peers
        except OSError:
            # most likely indicates that socket is already closed, or that
            # this is an incoming, not-yet-accepted connection
            return None, False


def read_from_socket(
    headerread: Optional[Callable[[bytes], dict]],
    sock: socket.socket,
    timeout: float,
) -> bytes:

    """
    one-shot read-all-data-from-socket function

    Args:
        headerread: optional function to read data header in order to
            determine total data size
        sock: open socket to peer
        timeout: timeout duration

    Returns:
        bytes received from peer

    Raises:
        TimeoutError: if any attempt to read a chunk of data takes more than
            timeout seconds
    """
    # TODO: check that timeout is working
    # TODO, maybe: move _tryread?
    waiting, unwait = timeout_factory(timeout=timeout)
    data = sock.recv(16384)
    response, length = data, None
    if headerread is not None:
        try:
            length = headerread(response)["length"]
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
        else:
            response += data
            unwait()
        continue
    sock.close()
    return response


def tcp_send(
    data: bytes,
    host: str,
    port: int,
    timeout: float = 10,
    delay: float = 0,
    chunksize: Optional[float] = None,
    headerread: Optional[Callable[[bytes], dict]] = None,
) -> tuple[Union[str, bytes], Optional[int]]:
    """
    one-shot send-data-over-TCP-and-get-response utility.

    Args:
        data: data to send
        host: hostname (usually ip address or 'localhost') of recipient
        port: port number of recipient
        timeout: how long to wait for successful connection / send (s)
        delay: delay between sends to socket (s)
        chunksize: chunk size for sends; None means unchunked unless a nonzero
            `delay` is specified, in which case chunk size defaults to 16384
        headerread: optional function to decode response header, specifically
            in order to determine intended length of response

    Returns:
        response: response, if successfully received (possibly an empty
            bytestring); "timeout" on timeout, "connection refused" on failed
            connection
        sockname: file descriptor for socket, if a connection was ever
            established; None if not

    """
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


# TODO: consider benchmarking pos-only / unnested versions
@wraps(tcp_send)
def stsend(
    data: bytes,
    host: str,
    port: int,
    timeout: float = 10,
    delay: float = 0,
    chunksize: Optional[int] = None,
):
    """
    wrapper for `tcp_send()` that autoencodes data as hostess comms. Used by
    Delegates to send comms to their Station.
    See `tcp_send()` for a full description of arguments and return values.
    """
    return tcp_send(
        make_comm(data),
        host,
        port,
        timeout,
        delay,
        chunksize,
        headerread=read_header,
    )
