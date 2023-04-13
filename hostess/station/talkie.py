"""lightweight function-first TCP comms"""
import atexit
from concurrent.futures import ThreadPoolExecutor
import selectors
import socket
import time


def launch_selector_thread(
    sel, event_cache, signal_cache, thread_ix, polling_interval=1
):
    while signal_cache.get(thread_ix) is None:
        time.sleep(polling_interval)
        events = sel.select(timeout=None)
        for key, mask in events:
            callback = key.data
            event_cache.append(
                {'thread': thread_ix, 'data': callback(key.fileobj, mask)}
            )
    return {'thread': thread_ix, 'signal': signal_cache.get(thread_ix)}


def signal_factory(thread_dict):
    def signaler(thread_ix, signal):
        if thread_ix not in thread_dict.keys():
            raise KeyError
        thread_dict[thread_ix] = signal

    return signaler


def launch_tcp_server(host, port, n_threads=4, polling_interval=1):
    sel = selectors.DefaultSelector()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    atexit.register(sock.close)

    def accept(sock_, _):
        try:
            conn, addr = sock_.accept()
        except BlockingIOError:
            # TODO: do something..,
            raise
        conn.setblocking(False)
        sel.register(conn, selectors.EVENT_READ, read)

    def read(conn, _):
        # will this block?
        data = conn.recv()
        sel.unregister(conn)
        conn.close()
        return data

    try:
        sock.bind((host, port))
        sock.listen()
        sock.setblocking(False)
        sel.register(sock, selectors.EVENT_READ, accept)
        executor = ThreadPoolExecutor(n_threads)
        threads, events, signals = {}, [], {i: None for i in range(n_threads)}
        for ix in range(n_threads):
            future = executor.submit(
                launch_selector_thread, sel, events, signals, ix,
                polling_interval
            )
            threads[ix] = future
    except Exception:
        sock.close()
        raise
    return {
        'threads': threads,
        'events': events,
        'signaler': signal_factory(signals),
        'socket': sock
    }


def tcp_send(data, host, port, timeout=10):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        # TODO: error handling / retry -- maybe send in a loop instead
        sock.sendall(data)