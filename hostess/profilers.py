"""profiling and introspection utilities"""
from __future__ import annotations

import _ctypes
import gc
import inspect
from collections import defaultdict
from typing import Mapping, Union

from dustgoggles.func import zero
from pympler.asizeof import asizeof

from hostess.monitors import make_stat_records, make_stat_printer, Stopwatch
from hostess.utilities import mb


class Profiler:
    """
    simple profiling object for specific sections of code
    """

    def __init__(self, monitors):
        self.monitors = monitors
        self.printstats = make_stat_printer(self.monitors)
        self.recordstats = make_stat_records(self.monitors)
        for k, v in self.monitors.items():
            v.default_display = "interval"
        self.labels = defaultdict(self._newcaches)

    def start(self):
        for v in self.monitors.values():
            v.start()

    def pause(self):
        for v in self.monitors.values():
            v.pause()

    def restart(self):
        for v in self.monitors.values():
            v.restart()

    def context(self, label=""):
        return PContext(self, label)

    def _newcaches(self):
        caches = {}
        for k, v in self.monitors.items():
            if v.qualities is not None:
                caches[k] = {q: 0 for q in v.qualities}
            else:
                caches[k] = 0
        return caches

    def __str__(self):
        if len(self.labels) == 0:
            return f"Profiler (no readings)"
        output = "Profiler\n"
        for k, v in self.labels.items():
            output += f"{k:}\n"
            for m, r in v.items():
                output += f"  {m}: {r}\n"
        return output

    def __repr__(self):
        return self.__str__()


class PContext:
    """simple context manager for profiling"""

    def __init__(self, profiler: Profiler, label=None):
        self.profiler = profiler
        self.label = label

    def __enter__(self):
        self.profiler.restart()
        return self.profiler

    def __exit__(self, *args):
        self.profiler.pause()
        record = self.profiler.recordstats()
        for monitor, reading in record.items():
            self._save_reading(monitor, reading)

    def _save_reading(
        self, monitor: str, reading: Union[int, float, Mapping]
    ):
        if isinstance(reading, (float, int)):
            self.profiler.labels[self.label][monitor] += reading
        else:
            for quality, value in reading.items():
                self.profiler.labels[self.label][monitor][quality] += value


def filter_ipython_history(item):
    if not isinstance(item, Mapping):
        return True
    if item.get("__name__") == "__main__":
        return False
    if "_i" not in item.keys():
        return True
    return False


def analyze_references(
    obj, method, filter_literal=True, filter_ipython=True, verbose=True
):
    print_ = print if verbose is True else zero
    refs = method(obj)
    if filter_literal is True:
        refs = tuple(
            filter(lambda ref: not isinstance(ref, (float, int, str)), refs)
        )
    if filter_ipython is True:
        refs = tuple(filter(filter_ipython_history, refs))
    extra_printables = [
        None if not isinstance(ref, tuple) else ref[0] for ref in refs
    ]
    for ref, extra in zip(refs, extra_printables):
        if extra is not None:
            print_(id(ref), type(ref), id(extra), type(extra))
        print_(id(ref), type(ref))
    return refs, extra_printables


def analyze_referents(
    obj, filter_literal=True, filter_ipython=True, verbose=True
):
    return analyze_references(
        obj, gc.get_referents, filter_literal, filter_ipython, verbose
    )


def analyze_referrers(
    obj, filter_literal=True, filter_ipython=True, verbose=True
):
    return analyze_references(
        obj, gc.get_referrers, filter_literal, filter_ipython, verbose
    )


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


def def_lineno(obj):
    """Returns the line number where the object was defined."""
    try:
        return inspect.getsourcelines(obj)[1]
    except TypeError:
        return None


def identify(obj, maxlen=25):
    """identify an object"""
    identifiers = {
        "id": id(obj),
        "type": type(obj),
        "line": def_lineno(obj),
        "r": repr(obj)[:maxlen],
        "size": mb(asizeof(obj), 2)
    }
    for attr in ("__name__", "__qualname__", "__module__"):
        if hasattr(obj, attr):
            identifiers[attr] = getattr(obj, attr)
    return identifiers


def di(obj_id):
    """backwards `id`. Use with care! Can segfault."""
    return _ctypes.PyObj_FromPtr(obj_id)


def describe_frame_contents(frame):
    """describe the contents of a frame"""
    return {
        "filename": frame.f_code.co_filename,
        "lineno": frame.f_lineno,
        "name": frame.f_code.co_name,
        "locals": tuple(map(identify, frame.f_locals)),
    }


def describe_stack_contents():
    """describe the contents of the stack"""
    return tuple(map(describe_frame_contents, [s[0] for s in inspect.stack()]))


DEFAULT_PROFILER = Profiler({'time': Stopwatch()})
