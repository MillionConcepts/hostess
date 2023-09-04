from collections import defaultdict
import gc
from inspect import currentframe
from itertools import chain
from operator import is_
import random
import sys
from typing import (
    Any, Collection, Literal, Mapping, MutableMapping, MutableSequence
)
import warnings

from cytoolz import valmap
from dustgoggles.test_utils import random_nested_dict
import numpy as np
import pandas as pd
import pympler
from _pydevd_bundle.pydevd_suspended_frames import _ObjectVariable

from hostess.monitors import memory
from hostess.profilers import (
    analyze_references,
    describe_frame_contents, 
    describe_stack_contents, 
    di,
    identify,
    yclept,
    scopedict_ids,
    namespace_ids
)
from hostess.utilities import is_any


    # for attr in ("close",):  # TODO: other obvious ones...
    #     if not hasattr(obj, attr):
    #         continue
    #     try:
    #         getattr(obj)()
    #     except (ValueError, TypeError, OSError):
    #         pass


class StillReferencedError(ValueError):
    pass


ITERATIONS = [0]
DEPTH = [0]


def rip_from_collection(obj, ref, doppelganger=None):
    if isinstance(ref, set):
        try:
            ref.remove(obj)
        except KeyError:
            return False
        if doppelganger is not None:
            ref.add(doppelganger)
        return True
    elif not isinstance(ref, Collection):
        return False
    elif isinstance(ref, Mapping):
        to_pop = tuple(filter(lambda kv: is_any(obj, kv), ref.items()))
        if len(to_pop) == 0:
            return False
        for item in to_pop:
            if doppelganger is not None:
                ref[item[0]] = doppelganger
            else:
                ref.pop(item[0])
        return True
    elif isinstance(ref, MutableSequence):
        indices = tuple(filter(lambda iv: iv[1] is obj, enumerate(ref)))
        indices = [index[0] for index in indices]
        if len(indices) == 0:
            return False
        decrement = 0
        for index in indices:
            if doppelganger is None:
                ref.pop(index - decrement)
                decrement += 1
            else:
                ref[index] = doppelganger 
        return True
    elif isinstance(ref, Mapping):
        to_remove = filter(lambda kv: is_any(obj, kv), ref.items())
        to_remove = [kv[0] for kv in to_remove]
        if len(to_remove) == 0:
            return False
        if doppelganger is None:
            # TODO: could cause problems for multidicts and similar
            new = ref.__class__({k: v for k, v in ref if k not in to_remove})
        else:
            new = {}
            for k, v in ref.items():
                if k in to_remove:
                    new[k] = doppelganger
                else:
                    new[k] = v
            new = ref.__class__(new)
    elif doppelganger is None:
        new = ref.__class__(filter(lambda i: i is not obj, ref))
        if len(new) == len(ref):
            return False
    else:
        i = 0
        new = []
        for item in ref:
            if item is not obj:
                new.append(item)
                continue
            i += 1
            if doppelganger is not None:
                new.append(doppelganger)
        if i == 0:
            return False
        new = ref.__class__(new)
    return ref, new


def rip_from_attrs(obj, ref, doppelganger=None):
    if not hasattr(ref, "__dict__"):
        return False
    attrs = [k for k, v in ref.__dict__.items() if v is obj]
    if len(attrs) == 0:
        return False
    try:
        for attr in attrs:
            try:
                setattr(ref, attr, doppelganger)
            except AttributeError:
                ref.__dict__.pop(attr)
                ref.__dict__[attr] = doppelganger
    except (KeyError, IndexError, TypeError):
        return False
    return True


# TODO, maybe: construct a tuple of 'preexisting'
# gc-tracked ids at the top and always restrict to that?
# would not have to do the exclude ids garbage 
def disintegrate(
    obj: Any, 
    doppelganger: Any = None, 
    leftovers: Literal["ignore", "raise", "warn"] = "ignore",
    globals_: dict[str, Any] = None,
    exclude_ids: set[int] = frozenset(),
) -> tuple[int, int]:
    ### DEBUG ! !! 
    ITERATIONS[0] += 1
    DEPTH[0] += 1
    print(
        f"\n\n----ITERATION {ITERATIONS[0]} (d: {DEPTH[0]})----\n"
        f"replacing {str(obj)[:20]} {id(obj)} with {str(doppelganger)[:20]} {id(doppelganger)}"
    )
    if DEPTH[0] > 7:
        raise ValueError
    if ITERATIONS[0] > 10:
        raise ValueError
    ### /DEBUG (though we might actually want a less strict recursion check)
    # subtract 2 from getrefcount's output because one l
    # reference exists in this function's local scope and
    # one reference exists in getrefcount's local scope
    start_refcount = sys.getrefcount(obj) - 2
    if not gc.is_tracked(obj):
        try:
            return  {
                "failed": [], 
                "refcount": sys.getrefcount(obj) - 2,
                "start_refcount": start_refcount,
                "succeeded": [],
                "untracked": True,
            }
        finally:
            del obj
    exclude_ids = set(exclude_ids)
    # TODO: does forbidding objects in this frame ever prevent
    #  finding cyclic references? 
    exclude_ids.update(scopedict_ids())
    exclude_ids.update(namespace_ids(include_frame_ids=True))
    succeeded, failed = [], []
    refnoms, refs = analyze_references(
        obj, 
        filter_history=False,
        filter_scopedicts=False,
        exclude_ids=exclude_ids,
        method=gc.get_referrers
    )
    for i in range(len(refnoms)):
        # TODO, maybe: ignore uncollected frames rather than running the 
        #  manual collect every time, slow
        gc.collect()  
        refnom, ref = refnoms[i], refs[i]
        ripped = rip_from_collection(obj, ref, doppelganger)
        if ripped is True:
            succeeded.append(refnom)
            continue
        if isinstance(ripped, tuple):
            ripobj, ripdop = ripped
            exclude_ids.update(namespace_ids())
            res = disintegrate(
                ripobj,
                ripdop,
                exclude_ids=exclude_ids,
            )
            # TODO: track what we recursed into
            # refnom['success_ids'] = r[1]['id']
            ### ! DEBUG ! ! !
            DEPTH[0] -= 1  
            if len(res['failed']) > 1:
                failtype = f"failcount: {len(res['failed'])}"
            elif res['untracked'] is True:
                failtype = "not tracked"
            # TODO: not sure how to determine the 'correct'
            # number of references here. maybe have 
            # analyze_references return the number of excluded
            # refs or something, not counting the ones it internally
            # excludes?
            # elif res['refcount'] > 2:
            #     failtype = f"refcount: {res['refcount']}"
            else:
                succeeded.append(refnom)
                continue
            print(f"failing with recursion on {str(ripobj)[:200]} ({failtype})")
            ### /DEBUG
            failed.append(refnom)
            continue
        if rip_from_attrs(obj, ref, doppelganger) is True:
            succeeded.append(refnom)
            continue
        print(f"failing with no recursion on {str(ref)[:200]}!!!")
        failed.append(refnom)
    output = {
        "failed": failed, 
        "refcount": sys.getrefcount(obj) - 2,
        "start_refcount": start_refcount,
        "succeeded": succeeded,
        "untracked": False,
    }
    if output['refcount'] < 0:
        warnings.warn(
            "less than the expected number of references during closeout."
        )
    if (
        (leftovers == "warn") 
        and (len(output['failed']) + output['refcount'] > 0)
    ):
        warnings.warn("leftover references after disintegration")
    if (
        (leftovers == "raise") 
        and (len(output['failed']) + output['refcount']  > 0)
    ):
        raise StillReferencedError
    return output


def arbput(
    *objects, 
    mapping=None, 
    maxdepth=3, 
    size=20, 
    valtypes=(list, dict), 
    keytypes=(str,)
):
    entries = []
    if mapping is None:
        mapping = random_nested_dict(
            size, 
            maxdepth=maxdepth, 
            types=valtypes, 
            keytypes=keytypes
        )
    targets = random.choices(
        tuple(mapping.keys()), 
        k=min(len(mapping.keys()), len(objects))
    )
    for obj, target in zip(objects, targets):
        if isinstance(mapping[target], Mapping):
            entries += arbput(obj, mapping=mapping[target])
        elif isinstance(mapping[target], Collection):
            mapping[target] = list(mapping[target]) + [obj]
            entries.append(mapping[target])
        else:
            mapping[target] = obj
            if not any(isinstance(v, Mapping) for v in mapping.values()):
                entries.append(mapping)
    return mapping, entries
