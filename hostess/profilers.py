"""profiling and introspection utilities"""
from __future__ import annotations

import _ctypes
from collections import defaultdict
from functools import partial
import gc
from inspect import currentframe, getsourcelines, stack
from itertools import chain
import re
from types import EllipsisType, FrameType, NoneType, NotImplementedType
from typing import Any, Callable, Collection, Mapping, Optional, Union

from cytoolz import keyfilter, valmap
from dustgoggles.func import gmap
from dustgoggles.structures import listify
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


def scopedicts(frame: FrameType) -> tuple[dict, dict, dict]:
    return (frame.f_locals, frame.f_globals, frame.f_builtins)


def val_ids(mapping):
    return set(map(id, mapping.values()))


def namespace_ids(
    frames: FrameType | Collection[FrameType] | None = None,
    include_frame_ids=False
) -> set[int]:
    """
    find ids of all top-level objects in the combined namespace(s) of 
    a frame or frames
    """
    frames = listify(frames if frames is not None else currentframe().f_back)
    ids_ = set(chain(*map(val_ids, chain(*map(scopedicts, frames)))))
    if include_frame_ids is True:
        ids_.update(map(id, frames))
    return ids_


def stack_scopedict_ids() -> set[int]:
    """
    return ids of all 'scopedicts' in stack. uses include: distinguishing 
    references held by namespaces from references held by other objects; 
    avoiding accidental 'direct' manipulation of namespaces.
    """
    return set(map(id, chain(*[scopedicts(s.frame) for s in stack()])))


def scopedict_ids(
    frames: FrameType | Collection[FrameType] | None = None
):
    frames = frames if frames is not None else currentframe().f_back
    return set(map(id, chain(*map(scopedicts, listify(frames)))))


def lineno():
    """Returns the current line number in our program."""
    return currentframe().f_back.f_lineno


def def_lineno(obj):
    """Returns the line number where the object was defined."""
    try:
        return getsourcelines(obj)[1]
    except TypeError:
        return None


IdentifyResult = dict[str, int | type | str]


def identify(
    obj: Any, 
    maxlen: int = 25, 
    getsize: bool = False
) -> IdentifyResult:
    """identify an object"""
    identifiers = {
        "id": id(obj),
        "type": type(obj),
        "line": def_lineno(obj),
        "r": repr(obj)[:maxlen],
    }
    if getsize is True:
        identifiers["size"]: mb(asizeof(obj), 2)
    for attr in ("__name__", "__qualname__", "__module__"):
        if hasattr(obj, attr):
            identifiers[attr] = getattr(obj, attr)
    return identifiers


def describe_frame_contents(frame=None):
    """describe the contents of a frame"""
    frame = frame if frame is not None else currentframe()
    return {
        "filename": frame.f_code.co_filename,
        "lineno": frame.f_lineno,
        "name": frame.f_code.co_name,
        "locals": valmap(identify, frame.f_locals),
    }


def describe_stack_contents():
    """describe the contents of the stack"""
    return tuple(map(describe_frame_contents, [s[0] for s in stack()]))


def framerec(frame: FrameType):
    return {
        'co_names': frame.f_code.co_names,
        'func': frame.f_code.co_name,
        'qual': frame.f_code.co_qualname,
        'varnames': frame.f_code.co_varnames
    }


Refnom = tuple[
    IdentifyResult, tuple[dict[str, tuple[str] | str]]
]
LITERAL_TYPES = (
    str, float, int, bool, slice, EllipsisType, NoneType, NotImplementedType
)


def yclept(obj: Any, terse=True, stepback=1) -> Refnom:
    nytta  = []
    frame = currentframe()
    for _ in range(stepback):
        frame = frame.f_back
    while frame is not None:
        rec = framerec(frame) | {'names': set()}
        if terse is True:
            rec = keyfilter(lambda k: k in ('func', 'qual', 'names'), rec)
        for scope in frame.f_locals, frame.f_globals, frame.f_builtins:
            for k, v in scope.items():
                if obj is v:
                    rec['names'].add(k)
        if len(rec['names']) > 0:
            rec['names'] = tuple(rec['names'])
            nytta.append(rec)
        frame = frame.f_back
    return identify(obj, maxlen=55, getsize=False), tuple(nytta)


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
    method: Callable[[Any], Collection], 
    filter_literal: bool = True, 
    filter_history: bool = True,
    filter_scopedicts: bool = True,
    globals_: Optional[dict[str, Any]] = None,
    exclude_ids: Collection[int] = frozenset(),
    exclude_types: Collection[type] = (),
    permit_ids: Collection[int] | None = None,
    return_objects: bool = True
) -> tuple[list[Refnom], list[Any]] | list[Refnom]:
    # TODO: check for circular reference
    refs = method(obj)
    f = currentframe()
    if filter_literal is True:
        exclude_types = list(exclude_types) + list(LITERAL_TYPES)
    refs = list(
        filter(lambda ref: not isinstance(ref, exclude_types), refs)
    )
    if filter_history is True:
        if globals_ is None:
            globals_ = currentframe().f_back.f_globals
        refs = list(filter(history_filter(globals_), refs))
    exclude_ids = set(exclude_ids)
    # sources of horrible recursive confusion
    if filter_scopedicts is True:
        exclude_ids.update(stack_scopedict_ids())
    exclude_ids.update(namespace_ids(include_frame_ids=True))
    outrefs, refnoms = [], []
    while len(refs) > 0:
        if id(ref := refs.pop()) in exclude_ids:
            continue
        if permit_ids is not None and id(ref) not in permit_ids:
            continue
        outrefs.append(ref)
        refnoms.append(yclept(ref, stepback=2))
    if return_objects is True:
        return list(refnoms), list(outrefs) 
    return list(refnoms)


def di(obj_id):
    """backwards `id`. Use with care! Can segfault."""
    return _ctypes.PyObj_FromPtr(obj_id)



class Aint:
    """
    note: not reliably monadic for all primitive types, e.g. bool
    """
    def __init__(self, obj):
        if isinstance(obj, Aint):
            self.__obj = obj.__obj
        else:
            self.__obj = obj

    def __repr__(self):
        return f"Aint({self.__obj.__repr__()})"

    def __str__(self):
        return self.__repr__()

    __add__ = lambda z, i: Aint(z.__obj + i)
    __radd__ = lambda z, i: Aint(i + z.__obj)
    __sub__ = lambda z, i: Aint(z.__obj - i)
    __rsub__ = lambda z, i: Aint(i - z.__obj)
    __truediv__ = lambda z, i: Aint(z.__obj / i)
    __rtruediv__ = lambda z, i: Aint(i / z.__obj)
    __floordiv__ = lambda z, i: Aint(z.__obj // i)
    __rfloordiv__ = lambda z, i: Aint(i // z.__obj)
    __mod__ = lambda z, i: Aint(z.__obj % i)
    __rmod__ = lambda z, i: Aint(i % z.__obj)
    __abs__ = lambda z: Aint(abs(z.__obj))
    __eq__ = lambda z, i: z.__obj == i
    __gt__ = lambda z, i: z.__obj > i
    __lt__ = lambda z, i: z.__obj < i
    __ge__ = lambda z, i: z.__obj >= i
    __le__ = lambda z, i: z.__obj <= i
    __bool__ = lambda z: bool(z.__obj)
    # spooky!
    __hash__ = lambda z: hash(z.__obj)


def t_analyze_references_1():
    def f1(num):
        x1 = Aint(num)
        xtup = (x1,)
        return f2(x1)

    def f2(y1):
        y2 = y1 + 1
        ytup = (y1, y2)
        return f3(y1, y2)

    def f3(z1, z2):
        z3 = z1 + z2
        ztup = (z1, z2, z3)
        return (
            gmap(analyze_referrers, ztup), 
            analyze_referents(ztup),
            analyze_referents(z1)
        )

    z1_z2_z3_referrers, ztup_referents, z1_referents = f1(1)
    meta0 = z1_z2_z3_referrers[0][1]
    assert len(meta0) == 3
    meta0_f = [meta0[i][0] for i in range(3)]
    meta0_names = [
        meta_f[0]['names'][0] for meta_f in meta0_f
    ]
    assert meta0_names == ['xtup', 'ytup', 'ztup']
    assert meta0[2][1]['r'] == '(Aint(1), Aint(2), Aint(3))'
    meta1 = z1_z2_z3_referrers[1][1]
    assert len(meta1) == 2
    meta1_f = [meta1[i][0] for i in range(2)]
    meta1_names = [
        meta_f[0]['names'][0] for meta_f in meta1_f
    ]
    assert meta1_names == ['ytup', 'ztup']
    assert z1_referents == ((), ())
    assert set(
        chain(map(lambda rec: rec['names'][0], ztup_referents[1][2][0]))
    ) == {'x1', 'y1', 'z1'}



DEFAULT_PROFILER = Profiler({'time': Stopwatch()})