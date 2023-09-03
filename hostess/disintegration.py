import gc
from inspect import currentframe
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

from hostess.monitors import memory
from hostess.profilers import (
    analyze_referents,
    analyze_referrers,
    describe_frame_contents, 
    describe_stack_contents, 
    di,
    identify
)
from hostess.utilities import is_any


class StillReferencedError(ValueError):
    pass


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
            if item is obj:
                continue
            new.append(item)
            i += 1
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
                ref.__dict__.pop(nattr)
                ref.__dict__[attr] = doppelganger
    except (KeyError, IndexError, TypeError):
        return False
    return True

II = [0]

def disintegrate(
    obj: Any, 
    doppelganger: Any = None, 
    leftovers: Literal["ignore", "raise", "warn"] = "ignore",
    glb = None,
    excluded = None,
) -> tuple[int, int]:
    # subtract 2 from getrefcount's output because one 
    # reference exists in this function's local scope and
    # one reference exists in getrefcount's local scope
    start_refcount = sys.getrefcount(obj) - 2
    for attr in ("close",):  # TODO: other obvious ones...
        if not hasattr(obj, attr):
            continue
        try:
            getattr(obj)()
        except (ValueError, TypeError, OSError):
            pass
    if not gc.is_tracked(obj):
        try:
            return  {
                "failed": 0, 
                "refcount": sys.getrefcount(obj) - 2,
                "start_refcount": start_refcount,
                "succeeded": 0,
                "untracked": True,
            }
        finally:
            del obj
    excluded = [] if excluded is None else excluded
    # a source of terrible recursive confusion if not excluded
    excluded += [currentframe().f_locals, currentframe().f_globals]    
    if glb is None:
        glb = currentframe().f_back.f_globals
    succeeded, failed, targets = 0, 0, []
    for ref in analyze_referrers(
        obj, 
        filter_history=False, 
        verbose=False
    )[0]:
        ripped = rip_from_collection(obj, ref, doppelganger)
        if ripped is True:
            succeeded += 1
            continue
        if isinstance(ripped, tuple):
            targets.append(ripped)
            continue
        if rip_from_attrs(obj, ref, doppelganger) is True:
            succeeded += 1
            continue
        failed += 1
    while len(targets) > 0:
        res = disintegrate(*t, excluded=excluded, glb=glb)
        if isinstance(res, list):
            return res
        # may have an extra reference actually...
        if max(res['failed'], res['refcount']) > 0:
            failed += 1
            continue
        succeeded += 1
    del targets
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
    try:
        if (leftovers == "warn") and (output['failed'] + output['refcount']) > 0:
            warnings.warn("leftover references after disintegration")
        if (leftovers == "raise") and (output['failed'] + output['refcount']) > 0:
            raise StillReferencedError
        return output
    finally:
        del obj


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
