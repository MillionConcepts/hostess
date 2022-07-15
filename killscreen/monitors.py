"""tracking, logging, and synchronization objects"""

import subprocess
import threading
import time

from dateutil import parser as dtp

from killscreen.utilities import mb, console_and_log, stamp


class FakeBouncer:
    def clean(self):
        pass

    def block(self):
        pass

    def click(self):
        pass


class Bouncer:
    """blocking rate-limiter"""

    def __init__(self, ratelimit, window=1, blockdelay=None):
        self.events = []
        self.ratelimit = ratelimit
        self.window = window
        if blockdelay is None:
            blockdelay = window / ratelimit
        self.blockdelay = blockdelay

    def clean(self):
        now = time.time()
        self.events = list(
            filter(lambda t: now - t > self.window, self.events)
        )

    def block(self):
        self.clean()
        while len(self.events) > self.ratelimit:
            time.sleep(self.blockdelay)
            self.clean()

    def click(self, block=True):
        self.clean()
        now = time.time()
        self.events.append(now)
        if block:
            self.block()


class LogMB:
    def __init__(self):
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            extra = self._seen_so_far + bytes_amount
            if mb(extra - self._seen_so_far) > 25:
                console_and_log(
                    stamp() + f"transferred {mb(extra)}MB", style="blue"
                )
            self._seen_so_far = extra


class FakeStopwatch:
    """fake simple timer object"""

    def __init__(self, digits=2, silent=False):
        self.digits = digits
        self.last_time = None
        self.start_time = None
        self.silent = silent

    def peek(self):
        return

    def start(self):
        return

    def click(self):
        return

    def total(self):
        return


class Stopwatch(FakeStopwatch):
    """
    simple timer object
    """
    def __init__(self, digits=2, silent=False):
        super().__init__(digits, silent)

    def peek(self):
        if self.last_time is None:
            return 0
        return round(time.time() - self.last_time, self.digits)

    def start(self):
        if self.silent is False:
            print("starting timer")
        now = time.time()
        self.start_time = now
        self.last_time = now

    def click(self):
        if self.last_time is None:
            return self.start()
        if self.silent is False:
            print(f"{self.peek()} elapsed seconds, restarting timer")
        self.last_time = time.time()

    def total(self):
        if self.last_time is None:
            return 0
        return round(time.time() - self.start_time, self.digits)


class TimeSwitcher:
    """
    little object that tracks changing times
    """

    def __init__(self, start_time: str = None):
        if start_time is not None:
            self.times = [start_time]
        else:
            self.times = []

    def check_time(self, string):
        try:
            self.times.append(dtp.parse(string).isoformat())
            return True
        except dtp.ParserError:
            return False

    def __repr__(self):
        if len(self.times) > 0:
            return self.times[-1]
        return None

    def __str__(self):
        return self.__repr__()


PROC_NET_DEV_FIELDS = (
    "bytes",
    "packets",
    "errs",
    "drop",
    "fifo",
    "frame",
    "compressed",
    "multicast",
)


def catprocnetdev():
    with open("/proc/net/dev") as stream:
        return stream.read()


def parseprocnetdev(procnetdev, rejects=("lo",)):
    interface_lines = filter(
        lambda l: ":" in l[:12], map(str.strip, procnetdev.split("\n"))
    )
    entries = []
    for interface, values in map(lambda l: l.split(":"), interface_lines):
        if interface in rejects:
            continue
        records = {
            field: int(number)
            for field, number in zip(
                PROC_NET_DEV_FIELDS, filter(None, values.split(" "))
            )
        }
        entries.append({"interface": interface} | records)
    return entries


class FakeNetstat:
    """fake simple network monitor."""
    def __init__(self, rejects=("lo",)):
        self.rejects = rejects
        self.absolute, self.last, self.interval, self.total = None, {}, {}, {}

    def update(self):
        return


class Netstat(FakeNetstat):
    """simple network monitor. works only on *nix at present."""
    # TODO: monitor TX as well as RX, etc.
    def __init__(self, rejects=("lo",)):
        super().__init__(rejects)
        self.update()

    def update(self):
        self.absolute = parseprocnetdev(catprocnetdev(), self.rejects)
        for line in self.absolute:
            interface, bytes_ = line["interface"], line["bytes"]
            if interface not in self.interval.keys():
                self.total[interface] = 0
                self.interval[interface] = 0
                self.last[interface] = bytes_
            else:
                self.interval[interface] = bytes_ - self.last[interface]
                self.total[interface] += self.interval[interface]
                self.last[interface] = bytes_

    def __repr__(self):
        return str(self.absolute)


def record_and_yell(message: str, cache: MutableMapping, loud: bool = False):
    """
    place message into a cache object with a timestamp; optionally print it
    """
    if loud is True:
        print(message)
    cache[dt.datetime.now().isoformat()] = message


def notary(cache):

    def note(message, loud: bool = False, eject: bool = False):
        if eject is True:
            return cache
        return record_and_yell(message, cache, loud)
    return note


def print_stats(watch, netstat):
    watch.start(), netstat.update()

    def printer(total=False, eject=False):
        netstat.update()
        if eject is True:
            return watch, netstat
        if total is True:
            text = (
                f"{watch.total()} total s,"
                f"{mb(round(first(netstat.total.values())))} total MB"
            )
        else:
            text = (
                f"{watch.peek()} s,"
                f"{mb(round(first(netstat.interval.values())))} MB"
            )
            watch.click()
        return text
    return printer


def make_monitors(
    fake: bool = False, silent: bool = True
) -> tuple[Callable, Callable]:
    if fake is True:
        stat, note = zero, zero
    else:
        log, watch, netstat = {}, Stopwatch(silent=silent), Netstat()
        stat, note = print_stats(watch, netstat), notary(log)
    return stat, note


def logstamp() -> str:
    return f"{dt.datetime.utcnow().isoformat()[:-7]}"


def dcom(string):
    return re.sub('[,\n]', ';', string.strip())


def lprint(message):
    print(message)
    with open(LOGFILE, "a") as stream:
        stream.write(message)


def log_factory(stamp, stat, log_fields):
    def log(event, **kwargs):
        center = ",".join(
            [event, *[kwargs.get(field, "") for field in log_fields]]
        )
        lprint(f"{logstamp()},{center},{stat()}\n")

    return log