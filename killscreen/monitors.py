"""tracking, logging, and synchronization objects"""
from types import MappingProxyType
from typing import MutableMapping, Callable, Optional, Mapping, Sequence, \
    Union, Literal

import datetime as dt
import re
import threading
import time

import psutil
from cytoolz import first
from dateutil import parser as dtp
from dustgoggles.func import zero

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

    fake = True


class Stopwatch(FakeStopwatch):
    """simple timer object"""

    def __init__(self, digits=2, silent=False):
        super().__init__(digits, silent)

    def peek(self):
        if self.last_time is None:
            return 0
        value = time.time() - self.last_time
        if self.digits is None:
            return value
        return round(value, self.digits)

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
        value = time.time() - self.start_time
        if self.digits is None:
            return value
        return round(value, self.digits)

    fake = False


class FakeCPUMonitor:
    """fake simple CPU usage monitor."""

    def __init__(self, times="all", round_to=2):
        self.times = times
        self.round_to = round_to
        self.interval = {}
        self.last = {}
        self.absolute = {}
        self.total = {}

    def update(self):
        return

    def display(self, which="interval", simple=False):
        return ""

    fake = True


class CPUMonitor(FakeCPUMonitor):
    """simple CPU usage monitor."""

    def __init__(self, times="all", round_to=2):
        super().__init__(times, round_to)
        if self.times == "all":
            # noinspection PyProtectedMember
            self.times = psutil.cpu_times()._fields
        self.update()

    def update(self):
        cputimes = psutil.cpu_times()
        self.absolute = {
            time_type: getattr(cputimes, time_type) for time_type in self.times
        }
        if len(self.interval) == 0:
            self.interval = {t: 0 for t in self.times}
            self.total = {t: 0 for t in self.times}
        else:
            self.interval = {
                t: self.absolute[t] - self.last[t] for t in self.times
            }
            self.total = {
                t: self.interval[t] + self.total[t] for t in self.times
            }
        self.last = self.absolute

    def display(self, which="interval", simple=False):
        infix = f"{which} " if which != "interval" else ""
        period = getattr(self, which)
        if simple is True:
            if "idle" not in period:
                raise KeyError("simple=True requires idle time monitoring.")
            items = (
                ("idle", period["idle"]),
                ("busy", sum([v for k, v in period.items() if k != "idle"])),
            )
        else:
            items = period.items()
        if self.round_to is not None:
            string = ";".join(
                [f"{k} {round(v, self.round_to)}" for k, v in items]
            )
        else:
            string = ";".join([f"{k} {v}" for k, v in items])
        return string + f" {infix}s"

    def __repr__(self):
        return str(self.absolute)

    fake = False


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

    def __init__(self, rejects=("lo",), round_to=2):
        self.rejects = rejects
        self.round_to = round_to
        self.absolute, self.last, self.interval, self.total = None, {}, {}, {}

    def update(self):
        return

    def display(self, which="interval", interface=None):
        return ""

    fake = True


class Netstat(FakeNetstat):
    """simple network monitor. works only on *nix at present."""

    # TODO: monitor TX as well as RX, etc.
    def __init__(self, rejects=("lo",), round_to=2):
        super().__init__(rejects, round_to)
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

    def display(self, which="interval", interface=None):
        infix = f"{which} " if which != "interval" else ""
        if interface is None:
            value = first(getattr(self, which).values())
        else:
            value = getattr(self, which)[interface]
        return f"{mb(value, self.round_to)} {infix}MB"

    def __repr__(self):
        return str(self.absolute)

    fake = False


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


def make_monitors(
    fake: bool = False,
    silent: bool = True,
    round_to: Optional[int] = None,
    reject_interfaces: Sequence[str] = ("lo",),
    cpu_time_types: Union[Literal["all"], Sequence[str]] = "all"
) -> tuple[Callable, Callable]:
    if fake is True:
        return zero, zero
    log = {}
    watch = Stopwatch(silent=silent, digits=round_to)
    netstat = Netstat(round_to=round_to, rejects=reject_interfaces)
    cpumon = CPUMonitor(round_to=round_to, times=cpu_time_types)
    stat, note = print_stats(watch, netstat, cpumon), notary(log)
    return stat, note


def logstamp() -> str:
    return f"{dt.datetime.utcnow().isoformat()[:-7]}"


def dcom(string):
    return re.sub("[,\n]", ";", string.strip())


def log_factory(stamper, stat, log_fields, logfile):
    def lprint(message):
        print(message)
        with open(logfile, "a") as stream:
            stream.write(message)

    def log(event, **kwargs):
        center = ",".join(
            [event, *[kwargs.get(field, "") for field in log_fields]]
        )
        lprint(f"{stamper()},{center},{stat()}\n")

    return log


def print_stats(
    watch: Optional[FakeStopwatch] = None,
    netstat: Optional[FakeNetstat] = None,
    cpumon: Optional[FakeCPUMonitor] = None,
):
    watch = FakeStopwatch() if watch is None else watch
    netstat = FakeNetstat() if netstat is None else netstat
    cpumon = FakeCPUMonitor() if cpumon is None else cpumon
    watch.start(), netstat.update(), cpumon.update()

    def printer(total=False, eject=False, simple_cpu=False, no_lap=None):
        if eject is True:
            return watch, netstat, cpumon
        netstat.update(), cpumon.update()
        if no_lap is None:
            no_lap = True if total is True else False
        if total is True:
            sec = None if watch.fake is True else f"{watch.total()} total s"
            vol = None if netstat.fake is True else netstat.display("total")
            cpu = None
            if cpumon.fake is False:
                cpu = "cpu " + cpumon.display("total", simple=simple_cpu)
        else:
            sec = None if watch.fake is True else f"{watch.peek()} s"
            vol = None if netstat.fake is True else netstat.display("interval")
            cpu = None
            if cpumon.fake is False:
                cpu = "cpu " + cpumon.display("interval", simple=simple_cpu)
        if no_lap is False:
            watch.click()
        # noinspection PyTypeChecker
        return ",".join(filter(None, [sec, vol, cpu]))

    return printer
