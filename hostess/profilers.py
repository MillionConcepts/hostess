"""profiling and introspection utilities"""
from __future__ import annotations

import _ctypes
from collections import defaultdict
from inspect import currentframe, getsourcelines, stack
from itertools import chain
import re
from types import EllipsisType, FrameType, NoneType, NotImplementedType
from typing import (
    Any, Callable, Collection, Literal, Mapping, Optional, Union
)

from cytoolz import keyfilter, valmap
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


def scopedicts(
    frame: FrameType, scopes=('locals', 'globals', 'builtins')
) -> tuple[dict, ...]:
    """
    WARNING: caller is responsible for clearing references to locals, etc.
    """
    outscopes = []
    for scope in scopes:
        outscopes.append(getattr(frame, f"f_{scope}"))
    return tuple(outscopes)


def val_ids(mapping):
    return set(map(id, mapping.values()))


def _maybe_release_locals(localdict, frame):
    """
    the locals dict of the top-level module frame is the
    same as its globals dict, and retrieving it from the frame here
    gives us the actual dict. we do NOT want to delete all members
    of the top-level module frame here.
    conversely, locals dicts retrieved from lower frames are just
    copies. modifying them will not affect the locals available in
    those frames. HOWEVER, references to everything in those copies
    will hang around forever until _that_ frame fully dies, and
    clearing the copy is the only reliable way to prevent that from
    happening.
    """
    if frame.f_code.co_name != "<module>":
        localdict.clear()
        return True
    return False


def namespace_ids(
    frames: Union[FrameType, Collection[FrameType], None] = None,
    include_frame_ids=False,
) -> set[int]:
    """
    find ids of all top-level objects in the combined namespace(s) of
    a frame or frames
    """
    if frames is None:
        frames = [currentframe().f_back]
    ids = set()
    for frame in frames:
        localdict, globaldict, builtindict = scopedicts(frame)
        ids.update(chain(*map(val_ids, (localdict, globaldict, builtindict))))
        _maybe_release_locals(localdict, frame)
    if include_frame_ids is True:
        ids.update(map(id, frames))
    return ids


def _add_scopedict_ids(frame, ids, lids, scopenames):
    sdicts = {
        k: v for k, v in zip(scopenames, scopedicts(frame, scopenames))
    }
    for k, v in sdicts.items():
        sid = id(v)
        ids.add(sid)
        if k == "locals":
            if _maybe_release_locals(v, frame) is True:
                lids.add(sid)



ScopeName = Literal["locals", "globals", "builtins"]
"""
string that gives the name of a Python scope, not including enclosing/nonlocal 
scope.
"""


def scopedict_ids(
    frames: Union[FrameType, Collection[FrameType], None] = None,
    *,
    getstack: bool = False,
    scopenames: Collection[ScopeName] = ('locals', 'globals', 'builtins'),
    distinguish_locals: bool = True
):
    """
    return ids of all 'scopedicts' (locals, globals, builtins) in frames (by
    default, just the caller's frame.) uses include: distinguishing 
    references held by namespaces from references held by other objects; 
    avoiding accidental 'direct' manipulation of namespaces.

    Args:
        getstack: if True, ignore the frames argument and instead look at
            all levels of the stack above the caller's frame.
        distinguish_locals: if True, return a tuple whose elements are:  
            [0] all ids  
            [1] just local-scope ids below top level
    """
    if getstack is True:
        frames = [s.frame for s in stack()[:-1]]
    frames = frames if frames is not None else [currentframe().f_back]
    ids, lids = set(), set()
    for frame in frames:
        _add_scopedict_ids(frame, ids, lids, scopenames)
    if distinguish_locals is True:
        return ids, lids
    return ids


def lineno() -> int:
    """Returns the current line number in our program."""
    return currentframe().f_back.f_lineno


def def_lineno(obj) -> Optional[int]:
    """Returns the line number where the object was defined, if available."""
    try:
        return getsourcelines(obj)[1]
    except TypeError:
        return None


IdentifyResult = dict[str, Union[int, type, str]]
"""
record representing information about a Python object, as produced by 
`identify` and functions that call it.
"""


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
        identifiers["size"] = mb(asizeof(obj), 2)
    for attr in ("__name__", "__qualname__", "__module__"):
        if hasattr(obj, attr):
            identifiers[attr] = getattr(obj, attr)
    return identifiers


def describe_frame_contents(frame=None):
    """describe the contents of a frame"""
    frame = frame if frame is not None else currentframe().f_back
    try:
        return {
            "filename": frame.f_code.co_filename,
            "lineno": frame.f_lineno,
            "name": frame.f_code.co_name,
            "locals": valmap(identify, frame.f_locals),
        }
    finally:
        _maybe_release_locals(frame.f_locals, frame)


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
    IdentifyResult, list[dict[str, Union[tuple[str], str, set[str]]]]
]
LITERAL_TYPES = (
    str, float, int, bool, slice, EllipsisType, NoneType, NotImplementedType
)


def _add_varnames(obj, sdict, rec, scopename):
    for varname, reference in sdict.items():
        if obj is reference:
            rec['names'].append(varname)
            rec['scopes'].append(scopename)


def yclept(obj: Any, terse: bool = True, stepback: int = 1) -> Refnom:
    """
    Find basic identifiers for obj, along with any names for obj in all frames
    in stack, starting stepback frames back from the frame of this function.

    Args:
        obj: object to name
        terse: include extended information in output?
        stepback: how many frames to step back (from the frame of this
            function) before looking for obj?
    """
    nytta = []
    frame = currentframe()
    for _ in range(stepback):
        frame = frame.f_back
    while frame is not None:
        rec = _yclept_framerec(frame) | {'names': [], 'scopes': []}
        if terse is True:
            rec = keyfilter(
                lambda k: k in ('func', 'qual', 'names', 'scopes'), rec
            )
        localdict, globaldict, builtindict = scopedicts(frame)
        _add_varnames(obj, globaldict, rec, "globals")
        _add_varnames(obj, builtindict, rec, "builtins")

        if frame.f_code.co_name != "<module>":
            # don't bother adding redundant local varnames at top level
            _add_varnames(obj, localdict, rec, "locals")
            localdict.clear()  # see _maybe_release_locals
        del globaldict, localdict, builtindict
        if len(rec['names']) > 0:
            rec['names'] = tuple(rec['names'])
            rec['scopes'] = tuple(rec['scopes'])
            nytta.append(rec)
        frame = frame.f_back
    res = identify(obj, maxlen=55, getsize=False), nytta
    del obj  # explicitly releasing obj clears ref faster
    return res


def _filter_types(
    refs: list,
    permit: Collection[type],
    exclude: Collection[type],
    lids: Collection[int]
) -> list:
    outrefs = []
    for ref in refs:
        reftype = type(ref)
        try:
            assert reftype not in exclude
            assert not (len(permit) > 0 and (reftype not in permit))
            outrefs.append(ref)
        except AssertionError:
            if id(ref) in lids:
                ref.clear()
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


def _filter_history(
    refs: list, globals_: Optional[dict], lids: Collection[int]
):
    if globals_ is None:
        globals_ = currentframe().f_back.f_back.f_globals
    outrefs, hfilt = [], history_filter(globals_)
    for ref in refs:
        if history_filter(globals_):
            outrefs.append(ref)
        elif id(ref) in lids:
            ref.clear()
    return outrefs


def _filter_ids(
    refs: list,
    permit: Collection[int],
    exclude: Collection[int],
    lids: Collection[int]
) -> tuple[list[Any], list[Refnom]]:
    outrefs, refnoms = [], []
    for ref in refs:
        try:
            assert id(ref) not in exclude
            assert (len(permit) == 0) or (id(ref) in permit)
            outrefs.append(ref)
            refnoms.append(yclept(ref, stepback=2))
        except AssertionError:
            if id(ref) in lids:
                ref.clear()
    return refnoms, outrefs


def _get_referencing_scopedicts(obj, existing_ids):
    outscopes = []
    # if you do NOT slice the stack to -2, it will create a reference cycle
    # from the local namespace of the caller to itself, preventing it from ever
    # being garbage collected.
    for scopedict in chain(*[scopedicts(s.frame) for s in stack()[:-2]]):
        if id(scopedict) in existing_ids:
            continue
        if id(obj) in map(id, scopedict.values()):
            outscopes.append(scopedict)
    return outscopes


# TODO, maybe: option to explicitly check variables of higher level namespaces
#  for references to obj (not references from those namespaces to obj, but to
#  other members of the namespaces)
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
    return_objects: bool = False
) -> Union[tuple[list[Refnom], list[Any]], list[Refnom]]:
    """
    analyze 'references' to or from obj. designed, but not limited to,
    analyzing references tracked by the garbage collector.
    Notes:

    1. TAKE SPECIAL CARE WHEN DECORATING THIS FUNCTION OR CALLING IT FROM
        A LAMBDA FUNCTION OR GENERATOR EXPRESSION, NO MATTER HOW HARMLESS-
        LOOKING. These operations may add references that are difficult to
        recognize or interpret. Calls that do not add context are much
        safer.
    2. This function is only completely compatible with CPython.
    3. All 'exclude', 'permit', and 'filter' operations are implicitly
        connected by boolean AND. Represented as a predicate:
        ```
        (~PRIMITIVE(REF) | ~FILTER_PRMITIVE)
        & (~HISTORY(REF) | ~FILTER_HISTORY)
        & (~SCOPEDICT(REF) | ~FILTER_SCOPEDICT)
        & ((ID(REF) != ID(OBJ)) | ~FILTER_REFLEXIVE)
        & ~(ID(REF) ∈ EXCLUDE_IDS)
        & (ID(REF) ∈ PERMIT_IDS | PERMIT_IDS = ∅)
        & ~(TYPE(REF) ∈ EXCLUDE_TYPES)
        & (TYPE(REF) ∈ PERMIT_TYPES | PERMIT_TYPES = ∅)
        ```
    4. references from obj to itself are never included. This may change
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
            currently only used in history filtering. If this argument is 
            None, history filtering uses the globals of the calling frame.
    """
    refs = list(method(obj))
    objid = id(obj)
    exclude_types, permit_types = set(exclude_types), set(permit_types)
    del obj  # explicitly releasing obj clears reference from frame faster
    if filter_primitive is True:
        exclude_types.update(LITERAL_TYPES)
    # ensure we can always clear copies of locals dicts we might have received
    # from the method(obj) call
    sids, lids = scopedict_ids(getstack=True, distinguish_locals=True)
    # type exclusions are easier to perform here. id exclusions need to come
    # at the end of the function in order to filter the objects in this
    # namespace.
    if len(exclude_types) + len(permit_types) > 0:
        refs = _filter_types(refs, permit_types, exclude_types, lids)
    if filter_history is True:
        refs = _filter_history(refs, globals_, lids)
    exclude_ids = set(exclude_ids)
    if filter_scopedict is True:
        exclude_ids.update(sids)
    # the frame of this function, along with all objects in its namespace, are
    # always excluded from analysis
    exclude_ids.update(namespace_ids(include_frame_ids=True))
    # do this via the negative in case a copy of obj was hanging around here
    # somehow
    if filter_reflexive is False:
        exclude_ids.difference_update({objid})
    # TODO, maybe -- consider also allowing arguments to this function to be
    #  included in analysis
    refnoms, outrefs = _filter_ids(refs, permit_ids, exclude_ids, lids)
    if return_objects is True:
        return refnoms, outrefs
    return refnoms


# noinspection PyUnresolvedReferences
def di(obj_id: int) -> Any:
    """
    backwards `id`. Use with care! Can segfault.

    Args:
        obj_id: id of desired object, as returned by `id(obj)`.

    Returns:
        Object corresponding to `obj_id`.
    """
    return _ctypes.PyObj_FromPtr(obj_id)


DEFAULT_PROFILER = Profiler({'time': Stopwatch()})
