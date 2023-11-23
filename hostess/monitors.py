"""tracking, logging, and synchronization objects"""
from abc import ABC
from collections import defaultdict
from functools import partial, wraps
from pathlib import Path
import threading
import time
from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

from cytoolz import identity
from dateutil import parser as dtp
from dustgoggles.func import constant
import psutil

from hostess.utilities import mb, console_and_log, stamp, curry


def memory() -> int:
    """
    alias for psutil.Process().memory_info().rss

    Returns:
        current process's real set size in bytes
    """
    return psutil.Process().memory_info().rss


class FakeBouncer:
    """
    fake blocking rate-limiter. Placeholder for a `Bouncer` in functions that
    don't actually want to debounce.
    """

    def clean(self):
        pass

    def block(self):
        pass

    def click(self):
        pass


class Bouncer(FakeBouncer):
    """simple blocking rate-limiter."""

    def __init__(
        self,
        ratelimit: float = 0.1,
        window: float = 1,
        blockdelay: Optional[float] = None,
    ):
        """
        Args:
            ratelimit: how many events to permit within a single window
            window: size of window in seconds
            blockdelay: poll rate when blocking (default window/ratelimit)
        """
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
        """clean the list of events"""
        now = time.time()
        self.events = list(
            filter(lambda t: (now - t) < self.window, self.events)
        )

    def block(self):
        """block until there are now longer too many events within window"""
        self.clean()
        while len(self.events) > self.ratelimit:
            time.sleep(self.blockdelay)
            self.clean()

    def click(self, block: bool = True):
        """
        record an event; optionally block

        Args:
            block: if True, call block() after recording event
        """
        self.clean()
        now = time.time()
        self.events.append(now)
        if block is True:
            self.block()


class LogMB:
    """simple text logger/printer for aggregate data volume"""

    def __init__(self, threshold_mb: float = 25):
        """

        Args:
            threshold_mb: at what interval of MB to log/print
        """

        self._threshold = threshold_mb
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount: int):
        """
        Record a write/transfer/whatever. If this causes running volume to
        cross a multiple of self._threshold, print and log the running volume.

        Args:
            bytes_amount: number of bytes just written/transferred
        """
        with self._lock:
            extra = self._seen_so_far + bytes_amount
            if mb(extra - self._seen_so_far) > self._threshold:
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
        """

        Args:
            digits: number of digits to round output to. if None, don't round.
            qualities: dictionary of subtypes of monitored quantity. if None,
                the Monitor only measures one thing.
            instrument: function used to perform monitoring.
            formatter: function used to format `instrument`'s output.
            name: default name of the monitor.
        """
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

    def _round(self, val: Union[float, Mapping[str, float]]):
        """round a measurement if set to do so."""
        if self.digits is None:
            return val
        if isinstance(val, Mapping):
            return {k: round(v, self.digits) for k, v in val.items()}
        return round(val, self.digits)

    def _unpack_reading(
        self, reading: Union[tuple, Mapping]
    ) -> dict[str, float]:
        """
        unpack an instrument reading. for monitors with multiple qualities.

        Args:
            reading: a named tuple or a Mapping containing measurements for
                various qualities.

        Returns:
            formatted dictionary of readings for each quality
        """
        if isinstance(reading, tuple):
            # noinspection PyProtectedMember,PyUnresolvedReferences
            reading = reading._asdict()
        return {k: self.formatter(v) for k, v in reading.items()}

    def _update_plural(self, reading: Union[tuple, Mapping], lap: bool):
        """
        update registers for each quality from a reading.

        Args:
            reading: instrument output
            lap: record this as a 'lap/split' (i.e., reset interval)?
        """
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

    def _update_single(self, reading: float, lap: bool):
        """
        update registers from a reading.

        Args:
            reading: instrument output
            lap: record this as a 'lap/split' (i.e., reset interval)?
        """
        self.absolute = reading
        self.interval = self.absolute - self.last
        if self.cumulative is True:
            self.total = self.total + self.interval
        else:
            self.total = self.absolute - self.first
        if lap is True:
            self.last = self.absolute

    def update(self, lap: bool = False):
        """
        update the monitor, starting it if necessary. ignored if monitor is
        paused.

        Args:
            lap: record this update as a 'lap/split' (i.e., reset interval)?
        """
        if self.paused is True:
            return
        if self.started is False:
            self.start()
        reading = self.instrument()
        if isinstance(reading, (tuple, Mapping)):
            reading = self._unpack_reading(reading)
            return self._update_plural(reading, lap)
        self._update_single(self.formatter(reading), lap)

    def _display_simple(self, _which):
        """internal formatting function"""
        raise TypeError(
            f"_display_simple() not supported for {self.__class__.__name__}"
        )

    def _display_single(self, value, which):
        """internal formatting function"""
        return f"{self._round(value)}{self.unitstring}{which}"

    def _display_plural(self, register, which):
        """internal formatting function"""
        values = [
            f"{quality} {self._display_single(register[quality], '')}"
            for quality in self.qualities
        ] + [which]
        return ";".join(filter(None, values))

    def display(
        self, which: str = None, say: bool = False, simple: bool = False
    ) -> str:
        """
        return string displaying the contents of one or all registers.

        Args:
            which: which register to print, or "all" for all. None prints
                register defined in self.default_display
            say: include name of register in output?
            simple: format output tersely?
        """
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

    def rec(
        self, which: Optional[str] = None
    ) -> Union[float, dict[str, float]]:
        """
        return value of one or all registers in numeric form.

        Args:
            which: name of register. self.default_display by default. "all"
            for all.
        """
        which = self.default_display if which is None else which
        if which == "all":
            return {this: self.rec(this) for this in self.registers}
        val = getattr(self, which)
        return self._round(val)

    def peek(
        self,
        which: Optional[str] = None,
        say: bool = False,
        simple: bool = False,
    ) -> str:
        """
        peek at one or all registers. managed shorthand for self.update()
        followed by self.display().

        Args:
            which: which register (default self.default_click, "all" for all)
            say: include name of register in output?
            simple: format output tersely?
        """
        which = self.default_click if which is None else which
        self.update()
        return self.display(which, say, simple)

    def click(self):
        """shorthand for self.update(True)"""
        self.update(True)

    def clickpeek(
        self,
        which: Optional[str] = None,
        say: bool = False,
        simple: bool = False,
    ) -> str:
        """
        click the lap button and look at the monitor. managed shorthand for
        self.update(True) followed by self.display().

        Args:
            which: register to look at (default default_click, "all" for all)
            say: include name of register in output?
            simple: format output tersely?
        """
        which = self.default_click if which is None else which
        self.click()
        return self.display(which, say, simple)

    def start(self, restart: bool = False):
        """
        start the monitor. unpauses if monitor is paused.

        Args:
            restart: if monitor is already started, restart it, clearing all
            entries?
        """
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
        """pause the monitor."""
        self.update()
        self.paused = True

    def restart(self):
        """restart the monitor."""
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
    """simple disk usage monitor"""

    def __init__(self, *, digits: Optional[int] = 3, path="."):
        """
        Args:
            digits: number of digits (as in AbstractMonitor)
            path: root directory to monitor
        """
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
    """simple network I/O monitor"""

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
    """simple CPU load monitoring device"""

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


def make_monitors(*, digits: Optional[int] = 3) -> dict[str, AbstractMonitor]:
    """make a default set of monitors"""
    return {
        "cpu": CPU(digits=digits),
        "cputime": CPUTime(digits=digits),
        "memory": RAM(digits=digits),
        "disk": Usage(digits=digits),
        "diskio": DiskIO(digits=digits),
        "networkio": NetworkIO(digits=digits),
        "time": Stopwatch(digits=digits),
    }


def make_stat_printer(
    monitors: Mapping[str, AbstractMonitor]
) -> Callable[
    [bool, bool, Any, ...], Union[str, Mapping[str, AbstractMonitor]]
]:
    """
    Args:
        monitors: dictionary of AbstractMonitors

    Returns:
        a function that holds `monitors` in enclosing scope.

            when called, and `eject` (its second positional parameter) is
            False, updates all monitors, passing its first positional
            parameter (`lap`) to the `update` methods of all monitors,
            and kwargs to the `display` methods of all monitors.
            it returns a string containing the concatenated output of all
            monitor displays.

            if `eject` is True, instead returns the dictionary of monitors.
    """

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
    with `make_stat_records()`.
    """

    def __init__(self, func: Callable):
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
    monitors: MutableMapping[
        str, Union[AbstractMonitor, Callable[[Any, ...], float]]
    ]
) -> Callable[
    [bool, bool, Any, ...],
    Union[
        Mapping[str, Union[AbstractMonitor, Recorder]],
        dict[str, Union[dict, float]],
    ],
]:
    """
    Args:
        monitors: dictionary of AbstractMonitors and/or functions that return
        floats.

    Returns:
        a stat-recording function that works much the function produced by
            `make_stat_printer`, but returns a dictionary of numerical values
            rather than simply returning strings.
    """
    for key in monitors.keys():
        if not isinstance(monitors[key], AbstractMonitor):
            monitors[key] = Recorder(monitors[key])

    def recordstats(
        lap: bool = True, eject: bool = False, **display_kwargs
    ) -> Union[
        Mapping[str, Union[AbstractMonitor, Recorder]],
        dict[str, Union[dict, float]],
    ]:
        if eject is True:
            return monitors
        for v in monitors.values():
            v.update(lap)
        return {k: v.rec(**display_kwargs) for k, v in monitors.items()}

    return recordstats


def log_factory(
    stamper: Callable[[], Any],
    stat: Callable[[], Any],
    log_fields: Sequence[str],
    logfile: Union[str, Path],
) -> Callable[[Any, ...], None]:
    """
    Args:
        stamper: line identifier function (i.e., a timestamper)
        stat: statistic-generating function
        log_fields: expected kwargs to log function -- this provides an
            ordering for columns in the output CSV
        logfile: where to write the log

    Returns:
        a function that, when called, creates, prints, and writes a
            comma-separated log line.
    """

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

    def __init__(self, start_time: Optional[str] = None):
        """
        Args:
            start_time: optional start time for the timer, in any format
                recognized by dateutil.
        """
        if start_time is not None:
            self.times = [start_time]
        else:
            self.times = []

    def check_time(self, string: str) -> bool:
        """
        if the passed value is parseable as a time, append it to self.times.

        Args:
            string: stringified time (maybe)

        Returns:
            True if `string` could be parsed as a time, False if not.
        """
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


class Ticker:
    def __init__(self):
        self.counts = defaultdict(int)

    def tick(self, label):
        self.counts[label] += 1

    def __repr__(self):
        if len(self.counts) == 0:
            return "Ticker (no counts)"
        selfstr = ""
        for k, v in self.counts.items():
            selfstr += f"{k}: {v}\n"
        return selfstr

    def __str__(self):
        return self.__repr__()


@curry
def ticked(func: Callable, label: str, ticker: Ticker) -> Callable:
    """
    Modify func so that it records a tick on ticker whenever it's called.
    To use with @ syntax, do something like:

    ```
    @ticked(label='login', ticker=DEFAULT_TICKER)
    def handle_login(...
    ```
    Args:
        func: function to modify
        label: label to use for tick
        ticker: Ticker to tick

    Returns:
        modified version of `func`
    """
    @wraps(func)
    def tickoff(*args, **kwargs):
        ticker.tick(label)
        return func(*args, **kwargs)

    return tickoff


DEFAULT_TICKER = Ticker()
"""convenient shared Ticker"""
