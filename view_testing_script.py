import time

import fire

from hostess.station.talkie import stsend
from text_app_work import find_station_port


def view_loop(port: int, timeout: float, poll: float):
    i, v, sock = 0, None, None
    while True:
        start = time.time()
        v, sock = stsend(
            b'view', 'localhost', port, timeout=timeout
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
    memory_address='station-port-report'
):
    while True:
        try:
            print(f'searching for port at fd {memory_address}...', end='')
            port = find_station_port(memory_address)
            print(f'found port {port}...', end='')
        except (FileNotFoundError, TypeError, ValueError):
            print('port not found')
            time.sleep(3)
            continue
        try:
            i, v, sock = view_loop(port, timeout, poll)
            time.sleep(3)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            print(
                f'\n******encountered exception: {type(ex)}: {ex}******\n'
            )


if __name__ == "__main__":
    fire.Fire(view_forever)
