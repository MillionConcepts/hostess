"""tracking, logging, and synchronization objects"""
import threading
import time
from abc import ABC
from functools import partial
from typing import (
    MutableMapping,
    Callable,
    Optional,
    Union,
    Mapping,
)

from cytoolz import identity
from dateutil import parser as dtp
from dustgoggles.func import constant
import psutil

from hostess.utilities import mb, console_and_log, stamp


def memory():
    return psutil.Process().memory_info().rss


class FakeBouncer:
    def clean(self):
        pass

    def block(self):
        pass

    def click(self):
        pass


class Bouncer(FakeBouncer):
    """blocking rate-limiter"""

    def __init__(self, ratelimit=0.1, window=1, blockdelay=None):
        self.events = []
        self.ratelimit = ratelimit
        self.window = window
        if blockdelay is None:
            blockdelay = window / ratelimit
        self.blockdelay = blockdelay

    def __new__(cls, ratelimit=0.1, window=1, blockdelay=None, fake=False):
        if fake is True:
            return FakeBouncer()
        return object.__new__(cls)

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


class AbstractMonitor(ABC):
    """base monitor class"""

    def __init__(
        self,
        *,
        digits: Optional[int] = None,
        qualities: Optional[Mapping[str, str]] = None,
        instrument: Callable[[], Union[float, int, Mapping]] = constant(0),
        formatter: Callable[[float], float] = identity,
        name: Optional[str] = None,
    ):
        if isinstance(qualities, (list, tuple)):
            qualities = {i: i for i in qualities}
        self.digits = digits
        self.interval: Union[int, dict] = 0
        self.last: Union[int, dict] = 0
        self.absolute: Union[int, dict] = 0
        self.total: Union[int, dict] = 0
        self.first: Union[int, dict] = 0
        self.lap = 0
        self.qualities = qualities
        self.instrument = instrument
        self.formatter = formatter
        self.started = False
        self.paused = False
        self.unitstring = f" {self.units}" if len(self.units) > 0 else ""
        self.name = self.__class__.__name__ if name is None else name

    def _round(self, val):
        if self.digits is None:
            return val
        if isinstance(val, Mapping):
            return {k: round(v, self.digits) for k, v in val.items()}
        return round(val, self.digits)

    def _unpack_reading(self, reading):
        if isinstance(reading, tuple):
            # noinspection PyProtectedMember
            reading = reading._asdict()
        return {k: self.formatter(v) for k, v in reading.items()}

    def _update_plural(self, reading, lap):
        self.absolute = {k: reading[v] for k, v in self.qualities.items()}
        if len(self.interval) == 0:
            self.interval = {t: 0 for t in self.qualities}
            self.total = {t: 0 for t in self.qualities}
        else:
            self.interval = {
                k: self.absolute[k] - self.last[k] for k in self.qualities
            }
            if self.cumulative is True:
                self.total = {
                    k: self.interval[k] + self.total[k] for k in self.qualities
                }
            else:
                self.total = {
                    k: self.absolute[k] - self.first[k] for k in self.qualities
                }
        if lap is True:
            self.last = self.absolute

    def _update_single(self, reading, lap):
        self.absolute = reading
        # maybe need to do a 'cumulative' attribute for certain instruments
        self.interval = self.absolute - self.last
        if self.cumulative is True:
            self.total = self.total + self.interval
        else:
            self.total = self.absolute - self.first
        if lap is True:
            self.last = self.absolute
            self.lap += 1

    def update(self, lap=False):
        if self.paused is True:
            return
        if self.started is False:
            self.start()
        reading = self.instrument()
        if isinstance(reading, (tuple, Mapping)):
            reading = self._unpack_reading(reading)
            return self._update_plural(reading, lap)
        return self._update_single(self.formatter(reading), lap)

    def _display_simple(self, _which):
        raise TypeError(
            f"_display_simple() not supported for {self.__class__.__name__}"
        )

    def _display_single(self, value, which):
        return f"{self._round(value)}{self.unitstring}{which}"

    def _display_plural(self, register, which):
        values = [
            f"{quality} {self._display_single(register[quality], '')}"
            for quality in self.qualities
        ] + [which]
        return ";".join(filter(None, values))

    def display(self, which=None, say=False, simple=False):
        which = self.default_display if which is None else which
        if which == "all":
            return "\n".join(
                [self.display(this, say, simple) for this in self.registers]
            )
        if simple is True:
            return self._display_simple(which)
        register = getattr(self, which, say)
        whichprint = f" {which}" if say is True else ""
        if isinstance(register, Mapping):
            return self._display_plural(register, whichprint)
        return self._display_single(register, whichprint)

    def rec(self, which=None):
        which = self.default_display if which is None else which
        if which == "all":
            return {this: self.rec(this) for this in self.registers}
        val = getattr(self, which)
        return self._round(val)

    def peek(self, which=None, say=False, simple=False):
        which = self.default_click if which is None else which
        self.update()
        return self.display(which, say, simple)

    def click(self):
        self.update(True)

    def clickpeek(self, which=None, say=False, simple=False):
        which = self.default_click if which is None else which
        self.click()
        return self.display(which, say, simple)

    def start(self, restart=False):
        self.paused = False
        if (restart is False) and (self.started is True):
            return
        self.started = True
        reading = self.instrument()
        if isinstance(reading, (tuple, Mapping)):
            reading = self._unpack_reading(reading)
        else:
            reading = self.formatter(reading)
        if isinstance(reading, Mapping):
            self.first = {k: reading[v] for k, v in self.qualities.items()}
            self.total, self.interval, self.absolute = {}, {}, {}
        else:
            self.first = reading
        self.last = self.first
        self.update()

    def pause(self):
        self.update()
        self.paused = True

    def restart(self):
        self.start(restart=True)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.display()}"

    units = ""
    fake = True
    cumulative = False
    default_display = "total"
    default_click = "interval"
    registers = ("first", "last", "absolute", "interval", "total")


class Stopwatch(AbstractMonitor):
    """simple timekeeping device"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = time.perf_counter

    units = "s"
    fake = False


class RAM(AbstractMonitor):
    """simple memory monitoring device"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = memory
        self.formatter = mb

    units = "MB"
    fake = False


class CPU(AbstractMonitor):
    """simple CPU monitoring device"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = psutil.cpu_percent
        self.formatter = identity

    units = "%"
    fake = False
    default_display = "absolute"
    default_click = "absolute"


class CPUTime(AbstractMonitor):
    """simple CPU time monitoring device"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = psutil.cpu_times
        self.qualities = {
            "user": "user",
            "system": "system",
            "idle": "idle",
            "iowait": "iowait",
        }


class Usage(AbstractMonitor):
    """simple Disk usage monitor"""

    def __init__(self, *, digits: Optional[int] = 3, path="."):
        super().__init__(digits=digits)
        self.qualities = {"total": "total", "used": "used", "free": "free"}
        self.instrument = partial(psutil.disk_usage, path)
        self.formatter = mb

    units = "MB"
    fake = False


class DiskIO(AbstractMonitor):
    """simple Disk io monitor"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = psutil.disk_io_counters
        self.formatter = mb
        self.qualities = {
            "read": "read_bytes",
            "write": "write_bytes",
            "read count": "read_count",
            "write count": "write_count",
        }

    units = "MB"
    fake = False
    cumulative = True


class NetworkIO(AbstractMonitor):
    """simple Network io monitor"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = psutil.net_io_counters
        self.formatter = mb
        self.qualities = {
            "sent": "bytes_sent",
            "recv": "bytes_recv",
            "sent count": "packets_sent",
            "recv count": "packets_recv",
        }

    units = "MB"
    fake = False


class Load(AbstractMonitor):
    """simple Load monitoring device"""

    def __init__(self, *, digits: Optional[int] = 3):
        super().__init__(digits=digits)
        self.instrument = psutil.getloadavg
        self.qualities = {
            "1m": 0,
            "5m": 1,
            "15m": 2,
        }

    units = ""
    fake = False
    default_display = "absolute"
    default_click = "absolute"


def make_monitors(*, digits: Optional[int] = 3):
    """make a set of monitors"""
    return {
        "cpu": CPU(digits=digits),
        "cputime": CPUTime(digits=digits),
        "memory": RAM(digits=digits),
        "disk": Usage(digits=digits),
        "diskio": DiskIO(digits=digits),
        "networkio": NetworkIO(digits=digits),
        "time": Stopwatch(digits=digits),
    }


def make_stat_printer(monitors: Mapping[str, AbstractMonitor]):
    def printstats(lap=True, eject=False, **display_kwargs):
        if eject is True:
            return monitors
        for v in monitors.values():
            v.update(lap)
        return ";".join(
            [v.display(**display_kwargs) for v in monitors.values()]
        )

    return printstats


class Recorder:
    """
    wrapper class for arbitrary callable. makes its interface compatible
    with make_stat_records()
    """

    def __init__(self, func):
        self.func = func
        self.cache = None

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.cache = self.func(*args, **kwargs)

    def start(self):
        return self.update()

    def pause(self):
        return self.update()

    def rec(self, *_, **__):
        return self.cache


def make_stat_records(
    monitors: MutableMapping[str, Union[AbstractMonitor, Callable]]
):
    for key in monitors.keys():
        if not isinstance(monitors[key], AbstractMonitor):
            monitors[key] = Recorder(monitors[key])

    def recordstats(
        lap=True, eject=False, **display_kwargs
    ) -> Union[
        Mapping[str, Union[AbstractMonitor, Recorder]],
        dict[str, Union[dict, float]]
    ]:
        if eject is True:
            return monitors
        for v in monitors.values():
            v.update(lap)
        return {k: v.rec(**display_kwargs) for k, v in monitors.items()}
    return recordstats


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
