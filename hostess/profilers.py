"""profiling and introspection utilities"""
from __future__ import annotations

import _ctypes
from collections import defaultdict
from inspect import currentframe, getsourcelines, stack
from itertools import chain
import re
from types import EllipsisType, FrameType, NoneType, NotImplementedType
from typing import (
    Any,
    Callable,
    Collection,
    Hashable,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

from cytoolz import keyfilter, valmap
from pympler.asizeof import asizeof

from hostess.monitors import (
    AbstractMonitor,
    make_stat_printer,
    make_stat_records,
    Stopwatch,
)
from hostess.utilities import mb


class Profiler:
    """
    simple profiling object for specific sections of code.

    example of use:
    ```
    from array import array
    from hostess.monitors import RAM, Stopwatch
    from hostess.profilers import Profiler

    prof = Profiler({'time': Stopwatch(), 'memory': RAM()})
    with prof.context("f"):
        var1 = array("B", [0 for _ in range(1024**2 * 100)])
    with prof.context("g"):
        var2 = array("B", [0 for _ in range(1024**2 * 250)])
    print(prof)
    ```
    general form of expected output (exact results are system-dependent):
    ```
    Profiler
    f
      time: 2.935
      memory: 105.47
    g
      time: 7.171
      memory: 261.76
    ```
    """

    def __init__(self, monitors: MutableMapping[str, AbstractMonitor]):
        """
        Args:
            monitors: dictionary of AbstractMonitor objects
                (see hostess.monitors for examples)
        """
        self.monitors = monitors
        self.printstats = make_stat_printer(self.monitors)
        self.recordstats = make_stat_records(self.monitors)
        for k, v in self.monitors.items():
            v.default_display = "interval"
        self.labels = defaultdict(self._newcaches)

    def start(self):
        """start all the monitors."""
        for v in self.monitors.values():
            v.start()

    def pause(self):
        """pause all the monitors."""
        for v in self.monitors.values():
            v.pause()

    def restart(self):
        """restart all the monitors."""
        for v in self.monitors.values():
            v.restart()

    def context(self, label: str = "") -> PContext:
        """
        create a context manager that profiles a section of code.

        Args:
            label: label for the code section, possibly shared between
                multiple sections. useful when it is desirable to distinguish
                specific steps of a pipeline, 'categories' of activity, etc.
        """
        return PContext(self, label)

    def reset(self):
        """clear this Profiler, removing all existing readings."""
        self.labels = defaultdict(self._newcaches)

    def _newcaches(self) -> dict:
        """internal data structure initialization function."""
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
    """
    simple context manager for profiling. typically instantiated via a
    Profiler's context() method, though this is not mandatory.
    """

    def __init__(self, profiler: Profiler, label: str = ""):
        """
        Args:
            profiler: associated Profiler; readings generated by this PContext
                will be stored in that Profiler's labels data structure.
            label: optional label for this PContext's profiling results.
        """
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

    def _save_reading(self, monitor: str, reading: Union[int, float, Mapping]):
        """
        internal function called on context block exit. saves profiling
        results to the associated Profiler.
        """
        if isinstance(reading, (float, int)):
            self.profiler.labels[self.label][monitor] += reading
        else:
            for quality, value in reading.items():
                self.profiler.labels[self.label][monitor][quality] += value


def scopedicts(
    frame: FrameType,
    scopes: Sequence[ScopeName] = ("locals", "globals", "builtins"),
) -> tuple[dict, ...]:
    """
    fetch specified scopes from a frame and return them in a tuple. the
    elements of the tuple should be equivalent to the results of calling,
    e.g., locals() within the passed frame.

    WARNING: caller is responsible for clearing references to locals, etc.
    calling this function with no cleanup deep in a call stack may lead to
    undesired dangling references.

    Args:
        frame: frame (as generated by, e.g., inspect.currentframe()) from
            which to fetch scopes.
        scopes: names of scopes to fetch from frame.

    Returns:
        tuple of dictionaries representing the specified scopes of frame;
            their keys are variable names and their values are the associated
            objects.
    """
    outscopes = []
    for scope in scopes:
        outscopes.append(getattr(frame, f"f_{scope}"))
    return tuple(outscopes)


def val_ids(mapping: Mapping[Hashable, Any]) -> set[int]:
    """
    get ids of all values in mapping.

    Args:
        mapping: mapping whose values are the objects to id.

    Returns:
        set of ids of all objects in mapping's values.
    """
    return set(map(id, mapping.values()))


def _maybe_release_locals(localdict: MutableMapping, frame: FrameType) -> bool:
    """
    Possibly purge a dictionary, depending on the name of `frame`'s code.

    Tedious description of technical rationale:

    the `locals` dict of the top-level module frame is the same as its
    `globals` dict. retrieving it from a frame gives us the _actual_ `globals`
    `dict`, not a copy of it. we do NOT want to casually delete all members
    of the top-level module while pretending to merely inspect it.

    conversely, `locals` `dicts` retrieved from lower frames are only copies.
    modifying them will not affect the actual namespaces of those frames.
    HOWEVER, references to everything in those copies will hang around forever
    until _that_ frame fully dies, which, in most programs, will badly confuse
    the Python garbage collector and cause horrible memory leaks. clearing the
    copies is the only reliable way to prevent that from happening.

    Args:
        localdict: a dict that might be a copy of `frame`'s locals, or might
            be an actual view into its locals
        frame: frame `localdict` came from

    Returns:
        True if we cleared `localdict`, False we didn't
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
    a frame or frames.
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
    sdicts = {k: v for k, v in zip(scopenames, scopedicts(frame, scopenames))}
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
    scopenames: Sequence[ScopeName] = ("locals", "globals", "builtins"),
    distinguish_locals: bool = True,
):
    """
    return ids of all 'scopedicts' (locals, globals, builtins) in frames (by
    default, just the caller's frame.) uses include: distinguishing
    references held by namespaces from references held by other objects;
    avoiding accidental 'direct' manipulation of namespaces.

    Args:
        frames: a single frame, a collection of frames, or None. if None,
            get ids of scopedicts of the caller's frame.
        getstack: if True, ignore the frames argument and instead look at
            all levels of the stack above the caller's frame.
        scopenames: names of scopes to fetch.
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
    obj: Any, maxlen: int = 25, getsize: bool = False
) -> IdentifyResult:
    """
    identify an object.

    Args:
        obj: object to identify
        maxlen: maximum length of string representation of `obj` in return
        getsize: if True, attempt to determine the size of `obj`. may be slow
            or unreliable.

    Returns:
        dict giving id, type, string
            representation (possibly truncated), size in MB (if requested),
            and, if available, __name__, __qualname__, and __module__, and
            line number.
    """
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
        "co_names": frame.f_code.co_names,
        "func": frame.f_code.co_name,
        "qual": frame.f_code.co_qualname,
        "varnames": frame.f_code.co_varnames,
    }


Refnom = tuple[
    IdentifyResult, list[dict[str, Union[tuple[str], str, set[str]]]]
]
LITERAL_TYPES = (
    str,
    float,
    int,
    bool,
    slice,
    EllipsisType,
    NoneType,
    NotImplementedType,
)


def _add_varnames(obj, sdict, rec, scopename):
    """
    helper function for yclept(). creates records describing the variables in
    a given scope.
    """
    for varname, reference in sdict.items():
        if obj is reference:
            rec["names"].append(varname)
            rec["scopes"].append(scopename)


def yclept(obj: Any, terse: bool = True, stepback: int = 1) -> Refnom:
    """
    Find basic identifiers for obj, along with any names for obj in all frames
    in stack, starting stepback frames back from the frame of this function.

    Args:
        obj: object to name
        terse: exclude extended information from output?
        stepback: how many frames to step back (from the frame of this
            function) before looking for obj?
    """
    nytta = []
    frame = currentframe()
    for _ in range(stepback):
        frame = frame.f_back
    while frame is not None:
        rec = _yclept_framerec(frame) | {"names": [], "scopes": []}
        if terse is True:
            rec = keyfilter(
                lambda k: k in ("func", "qual", "names", "scopes"), rec
            )
        localdict, globaldict, builtindict = scopedicts(frame)
        _add_varnames(obj, globaldict, rec, "globals")
        _add_varnames(obj, builtindict, rec, "builtins")

        if frame.f_code.co_name != "<module>":
            # don't bother adding redundant local varnames at top level
            _add_varnames(obj, localdict, rec, "locals")
            localdict.clear()  # see _maybe_release_locals
        del globaldict, localdict, builtindict
        if len(rec["names"]) > 0:
            rec["names"] = tuple(rec["names"])
            rec["scopes"] = tuple(rec["scopes"])
            nytta.append(rec)
        frame = frame.f_back
    res = identify(obj, maxlen=55, getsize=False), nytta
    del obj  # explicitly releasing obj clears ref faster
    return res


def _filter_types(
    refs: list,
    permit: Collection[type],
    exclude: Collection[type],
    lids: Collection[int],
) -> list:
    """
    helper function for analyze_references(). provides blocklist/allowlist
    behavior based on object types.

    Args:
        refs: list of referencing objects
        permit: types to explicitly permit. if there is at least one type,
            this functions as a strict allowlist.
        exclude: types to explicitly exclude.
        lids: ids of all known copies of other frames' `locals` dicts.

    Returns:
        filtered list of references.
    """
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


def history_filter(glb: dict[str, Any]) -> Callable[[Any], bool]:
    """
    generate a predicate function that attempts to filter jupyter/ipython
    history-related objects in the context of a particular global namespace.

    Args:
        glb: relevant globals dict

    Returns:
        function that returns False if its single argument looks like it's an
            ipython/jupyter history-related object or internal, and True if
            not.
    """

    def filterref(item):
        if item.__class__.__name__ == "ZMQShellDisplayHook":
            return False
        if item.__class__.__name__ == "ExecutionResult":
            return False
        try:
            globalname = next(filter(lambda kv: kv[1] is item, glb.items()))[0]
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
) -> list:
    """
    helper function for analyze_references(). attempts to remove references to
    ipython/jupyter history variables.

    Args:
        refs: list of reference objects
        globals_: optional specified globals dictionary. if not given, uses
            the globals of the parent frame of the caller.
        lids: ids of all known copies of other frames' locals dicts.

    Returns:
        filtered list of references.
    """
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
    lids: Collection[int],
) -> tuple[list[Any], list[Refnom]]:
    """
    helper function for analyze_references(). provides blocklist/allowlist
    behavior based on object ids.

    Args:
        refs: list of reference objects
        permit: list of allowed ids. if any are given, this functions as a
            strict allowlist.
        exclude: list of excluded ids
        lids: ids of all known copies of other frames' locals dicts

    Returns:
        filtered_refs: filtered list of references
        refnoms: Refnom dicts for each member of filtered_refs
    """
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


def _get_referencing_scopedicts(
    obj: Any, existing_ids: Collection[int]
) -> list[dict]:
    """
    check for `obj` in the globals and locals dicts of all stack frames above
    the caller's. return all dicts in which obj has a name or names.

    Args:
        obj: object to check for
        existing_ids: ids of scopedicts that should be ignored

    Returns:
        list of locals and globals dicts that reference obj
    """
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
    return_objects: bool = False,
) -> Union[tuple[list[Refnom], list[Any]], list[Refnom]]:
    """
    analyze 'references' to or from obj. designed for, but not limited to,
    analyzing references tracked by the Python garbage collector.
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
        filter_history: attempt to ignore ipython 'history' objects?\
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


DEFAULT_PROFILER = Profiler({"time": Stopwatch()})
"""convenient shared Profiler that measures execution times for code blocks."""
