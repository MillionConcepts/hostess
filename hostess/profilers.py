"""profiling and introspection utilities"""
from __future__ import annotations

import _ctypes
from collections import defaultdict
import gc
import inspect
from inspect import currentframe, getsourcelines, stack
import re
from typing import Mapping, Union

from dustgoggles.func import zero
from pympler.asizeof import asizeof

from hostess.monitors import make_stat_records, make_stat_printer, Stopwatch
from hostess.utilities import is_any, mb


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

    def reset(self):
        self.labels = defaultdict(self._newcaches)

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


def history_filter(glb):
    def filterref(item):
        if item.__class__.__name__ == "ZMQShellDisplayHook":
            return False
        if item.__class__.__name__ == "ExecutionResult":
            return False
        try:
            globalname = next(
                filter(lambda kv: kv[1] is item, glb.items())
            )[0]
        except StopIteration:
            return True
        if re.match(r"_+(i{1,3})?\d?", globalname):
            return False
        if globalname in ("In", "Out", "_ih", "_oh", "_dh"):
            return False
        return True

    return filterref


def analyze_references(
    obj, 
    method, 
    filter_literal=True, 
    filter_history=True,
    filter_scopes=True,
    verbose=False,
    glb=None,
    excluded=None,
):
    print_ = print if verbose is True else zero
    refs = method(obj)
    if filter_literal is True:
        refs = tuple(
            filter(lambda ref: not isinstance(ref, (float, int, str)), refs)
        )
    if glb is None:
        glb = currentframe().f_back.f_globals
    if filter_history is True:
        refs = tuple(filter(history_filter(glb), refs))
    excluded = [] if excluded is None else excluded
    # a source of horrible recursive confusion if not excluded
    excluded += [currentframe().f_locals, currentframe().f_globals]
    if filter_scopes is True:
        excluded += [currentframe().f_back.f_locals]
        excluded.append(glb)
    refs = tuple(r for r in refs if not is_any(r, excluded))
    extra_printables = [
        None if not isinstance(ref, tuple) else ref[0] for ref in refs
    ]
    for ref, extra in zip(refs, extra_printables):
        if extra is not None:
            print_(id(ref), type(ref), id(extra), type(extra))
        print_(id(ref), type(ref))
    return refs, extra_printables


def analyze_referents(
    obj, 
    filter_literal=True, 
    filter_history=True, 
    filter_scopes=True,
    verbose=False,
    glb=None,
    excluded=None,
):
    if filter_scopes is True:
        if excluded is None:
            excluded = [currentframe().f_back.f_locals]
    if glb is None:
        glb = currentframe().f_back.f_globals
    return analyze_references(
        obj, 
        gc.get_referents, 
        filter_literal, 
        filter_history, 
        filter_scopes,
        verbose,
        glb,
        excluded
    )


def analyze_referrers(
    obj, 
    filter_literal=True, 
    filter_history=True, 
    filter_scopes=True,
    verbose=False,
    glb=None,
    excluded=None,
):
    if filter_scopes is True:
        if excluded is None:
            excluded = [currentframe().f_back.f_locals]
    if glb is None:
        glb = currentframe().f_back.f_globals
    return analyze_references(
        obj, 
        gc.get_referrers, 
        filter_literal, 
        filter_history, 
        filter_scopes,
        verbose,
        glb,
        excluded
    )


def lineno():
    """Returns the current line number in our program."""
    return currentframe().f_back.f_lineno


def def_lineno(obj):
    """Returns the line number where the object was defined."""
    try:
        return getsourcelines(obj)[1]
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


def describe_frame_contents(frame=None):
    """describe the contents of a frame"""
    frame = frame if frame is not None else currentframe()
    return {
        "filename": frame.f_code.co_filename,
        "lineno": frame.f_lineno,
        "name": frame.f_code.co_name,
        "locals": tuple(map(identify, frame.f_locals)),
    }


def describe_stack_contents():
    """describe the contents of the stack"""
    return tuple(map(describe_frame_contents, [s[0] for s in stack()]))


DEFAULT_PROFILER = Profiler({'time': Stopwatch()})
