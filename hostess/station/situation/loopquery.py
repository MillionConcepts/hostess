"""
utility script to repeatedly request a Station's situation output without
running the full situation frontend. intended for dev purposes.
"""

import time

import fire

from hostess.station.station import get_port_from_shared_memory
from hostess.station.talkie import stsend


def _view_loop(port: int, timeout: float, poll: float):
    i, v, sock = 0, None, None
    while True:
        start = time.time()
        v, sock = stsend(
            b'eswadzxswzeradhbgz', 'localhost', port, timeout=timeout
        )
        if v == 'timeout':
            print('timeout')
            continue
        if v == 'connection refused':
            if i == 0:  # station not actually at saved port
                print(f'station@{port} offline\n')
                return i, v, sock
            print('\n******station disconnected******\n')
            break
        elif i == 0:
            print(f'station@{port} connected\n')
        i += 1
        print(
            f"{i}\n----\n",
            f"latency: {time.time() - start}\n",
            f"view size: {len(v)}\n",
            f"socket: {sock}\n"
        )
        time.sleep(poll)
    return i, v, sock


def view_forever(
    poll: float = 0.1,
    timeout: float = 0.5,
    station_name: str = 'station'
):
    while True:
        try:
            print(
                f'searching for port at fd {station_name}-port-report...',
                end=''
            )
            port = get_port_from_shared_memory(station_name)
            print(f'found port {port}...', end='')
        except (FileNotFoundError, TypeError, ValueError):
            print('port not found')
            time.sleep(3)
            continue
        try:
            _view_loop(port, timeout, poll)
            time.sleep(3)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            print(
                f'\n******encountered exception: {type(ex)}: {ex}******\n'
            )


if __name__ == "__main__":
    fire.Fire(view_forever)
