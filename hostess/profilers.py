"""profiling and introspection utilities"""
from __future__ import annotations

import _ctypes
from collections import defaultdict
from inspect import currentframe, getsourcelines, stack
from itertools import chain
import re
from types import EllipsisType, FrameType, NoneType, NotImplementedType
from typing import Any, Callable, Collection, Mapping, Optional, Union

from cytoolz import keyfilter, valmap
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
        "r": str(obj)[:maxlen],
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


def _yclept_framerec(frame: FrameType):
    """
    return terse information about a frame's name and contents. for yclept.
    """
    return {
        'co_names': frame.f_code.co_names,
        'func': frame.f_code.co_name,
        'qual': frame.f_code.co_qualname,
        'varnames': frame.f_code.co_varnames
    }


Refnom = tuple[
    IdentifyResult, list[dict[str, tuple[str] | str | set[str]]]
]
LITERAL_TYPES = (
    str, float, int, bool, slice, EllipsisType, NoneType, NotImplementedType
)


def yclept(obj: Any, terse=True, stepback=1) -> Refnom:
    """
    Find basic identifiers for obj, along with any names for obj in all frames
    in stack, starting stepback frames back from the frame of this function.
    Args:
        obj (object): object to name
        terse (bool): include extended information in output?
        stepback (int): how many frames to step back (from the frame of this
        function) before looking for obj?
    """
    nytta = []
    frame = currentframe()
    for _ in range(stepback):
        frame = frame.f_back
    while frame is not None:
        rec = _yclept_framerec(frame) | {'names': [], 'scopes': []}
        if terse is True:
            rec = keyfilter(lambda k: k in ('func', 'qual', 'names'), rec)
        for scopename in ("locals", "globals", "builtins"):
            for varname, reference in getattr(frame, f"f_{scopename}").items():
                if obj is reference:
                    rec['names'].append(varname)
                    rec['scopes'].append(scopename)
        if len(rec['names']) > 0:
            rec['names'] = tuple(rec['names'])
            rec['scopes'] = tuple(rec['scopes'])
            nytta.append(rec)
        frame = frame.f_back
    return identify(obj, maxlen=55, getsize=False), nytta


def _filter_types(
    refs: list, permit: Collection[type], exclude: Collection[type]
) -> list:
    outrefs = []
    while len(refs) > 0:
        ref = refs.pop()
        reftype = type(ref)
        if reftype in exclude:
            continue
        if len(permit) > 0 and (reftype not in permit):
            continue
        outrefs.append(ref)
    return outrefs


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


def _filter_history(refs: list, globals_: Optional[dict]):
    if globals_ is None:
        globals_ = currentframe().f_back.f_back.f_globals
    refs = list(filter(history_filter(globals_), refs))
    return refs


def _filter_ids(
    refs: list, exclude: Collection[int], permit: Collection[int]
) -> tuple[list[Any], list[Refnom]]:
    outrefs, refnoms = [], []
    while len(refs) > 0:
        if id(ref := refs.pop()) in exclude:
            continue
        if (len(permit) > 0) and id(ref) not in permit:
            continue
        outrefs.append(ref)
        refnoms.append(yclept(ref, stepback=2))
    return refnoms, outrefs


def analyze_references(
    obj: Any,
    method: Callable[[Any], Collection],
    *,
    filter_primitive: bool = True,
    filter_history: bool = True,
    filter_scopedict: bool = True,
    filter_reflexive: bool = True,
    exclude_ids: Collection[int] = frozenset(),
    exclude_types: Collection[type] = frozenset(),
    permit_ids: Collection[int] = frozenset(),
    permit_types: Collection[type] = frozenset(),
    globals_: Optional[dict[str, Any]] = None,
    return_objects: bool = True
) -> tuple[list[Refnom], list[Any]] | list[Refnom]:
    """
    analyze 'references' to or from obj. designed, but not limited to,
    analyzing references tracked by the garbage collector.
    Notes:
        1) This function is only completely compatible with CPython.
        2) All 'exclude', 'permit', and 'filter' operations are implicitly
            connected by boolean AND. Represented as a predicate:
            (~PRIMITIVE(REF) | ~FILTER_PRMITIVE)
            & (~HISTORY(REF) | ~FILTER_HISTORY)
            & (~SCOPEDICT(REF) | ~FILTER_SCOPEDICT)
            & ((ID(REF) != ID(OBJ)) | ~FILTER_REFLEXIVE)
            & ~(ID(REF) ∈ EXCLUDE_IDS)
            & (ID(REF) ∈ PERMIT_IDS | PERMIT_IDS = ∅)
            & ~(TYPE(REF) ∈ EXCLUDE_TYPES)
            & (TYPE(REF) ∈ PERMIT_TYPES | PERMIT_TYPES = ∅)
        3) references from obj to itself are never included. This may change
           in the future.
    Args:
        obj: object of referential analysis
        method: Function whose return values define 'references' of
            obj. gc.get_referents and gc.get_referrers are the intended and
            tested values.
        filter_primitive: ignore 'primitive' (str, bool, &c) objects?
        filter_history: attempt to ignore 'history' objects (intended for
            ipython)?
        filter_scopedict: ignore _direct_ references to the locals, globals,
            and builtins dicts of all frames in stack (_not_ the values of
            these dictionaries?)
        filter_reflexive: ignore references from obj to itself?
        exclude_ids: denylist of reference ids.
        exclude_types: denylist of reference types.
        permit_ids: allowlist of reference ids.
        permit_types: allowlist of reference types.
        return_objects: return objects in set of references, or only
            descriptions of those objects?
        globals_: optional dictionary of globals to use in filtering.
            currently only used in history filtering. If this argument is None,
            history filtering uses the globals of the calling frame.
    """
    refs = list(method(obj))
    exclude_types, permit_types = set(exclude_types), set(permit_types)
    if filter_primitive is True:
        exclude_types.update(LITERAL_TYPES)
    # type exclusions are easier to perform here. id exclusions need to come
    # at the end of the function in order to filter the objects in this
    # namespace.
    if len(exclude_types) + len(permit_types) > 0:
        refs = _filter_types(refs, exclude_types, permit_types)
    if filter_history is True:
        refs = _filter_history(refs, globals_)
    exclude_ids = set(exclude_ids)
    if filter_scopedict is True:
        exclude_ids.update(stack_scopedict_ids())
    # the frame of this function, along with all objects in its namespace, are
    # always excluded from analysis -- with the exception of obj if
    # filter_reflexive is False.
    exclude_ids.update(namespace_ids(include_frame_ids=True))
    # do this via the negative because id(obj) should be in namespace_ids()
    if filter_reflexive is False:
        exclude_ids.difference_update({id(obj)})
    # TODO, maybe -- consider also allowing arguments to this function
    refnoms, outrefs = _filter_ids(refs, exclude_ids, permit_ids)
    if return_objects is True:
        return refnoms, outrefs
    return refnoms


# noinspection PyUnresolvedReferences
def di(obj_id: int) -> Any:
    """backwards `id`. Use with care! Can segfault."""
    return _ctypes.PyObj_FromPtr(obj_id)


DEFAULT_PROFILER = Profiler({'time': Stopwatch()})
