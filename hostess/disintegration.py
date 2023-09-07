import gc
from itertools import chain
import random
import sys
from types import FrameType
from typing import (
    Any,
    Collection,
    Literal,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Optional,
)
import warnings

from dustgoggles.test_utils import random_nested_dict

from hostess.profilers import analyze_references, scopedict_ids, yclept
from hostess.utilities import is_any


class StillReferencedError(ValueError):
    pass


class NoTargetError(KeyError):
    """targeted object not present in collection."""
    pass


def get_tracked_ids():
    return set(map(id, gc.get_objects()))


class BailoutError(Exception):
    pass


# debug objects
ITERATIONS, DEPTH = [0], [0]


def resect_from_mutable_collection(obj, ref, doppelganger=None):
    if isinstance(ref, set):
        try:
            ref.remove(obj)
        except KeyError:
            raise NoTargetError
        if doppelganger is not None:
            ref.add(doppelganger)
    elif hasattr(ref, 'items'):
        to_pop = tuple(filter(lambda kv: is_any(obj, kv), ref.items()))
        if len(to_pop) == 0:
            return False
        for item in to_pop:
            if doppelganger is not None:
                ref[item[0]] = doppelganger
            else:
                ref.pop(item[0])
    elif hasattr(ref, 'pop'):
        indices = tuple(filter(lambda iv: iv[1] is obj, enumerate(ref)))
        indices = [index[0] for index in indices]
        if len(indices) == 0:
            raise NoTargetError
        decrement = 0
        for index in indices:
            if doppelganger is None:
                ref.pop(index - decrement)
                decrement += 1
            else:
                ref[index] = doppelganger
    else:
        raise NotImplementedError(
            f"Don't know how to handle object of type {type(ref)}"
        )


def rip_from_attrs(obj, ref, doppelganger=None):
    if not hasattr(ref, "__dict__"):
        return False
    attrs = [k for k, v in ref.__dict__.items() if v is obj]
    if len(attrs) == 0:
        for attr in attrs:
            try:
                setattr(ref, attr, doppelganger)
            except AttributeError:
                ref.__dict__.pop(attr)
                ref.__dict__[attr] = doppelganger
        return False
    try:
        pass
    except (KeyError, IndexError, TypeError):
        return False
    return True


# noinspection PyArgumentList
def doppelgangerize_immutable(
    original: Any,
    immutable_referrer: Collection,
    doppelganger: Optional[Any] = None,
) -> Collection:
    if isinstance(immutable_referrer, Mapping):
        to_remove = filter(
            lambda kv: is_any(original, kv), immutable_referrer.items()
        )
        to_remove = [kv[0] for kv in to_remove]
        if len(to_remove) == 0:
            raise NoTargetError
        if doppelganger is None:
            # TODO: could cause problems for multidicts and similar
            new = immutable_referrer.__class__(
                {k: v for k, v in immutable_referrer if k not in to_remove}
            )
        else:
            new = {}
            for k, v in immutable_referrer.items():
                if k in to_remove:
                    new[k] = doppelganger
                else:
                    new[k] = v
            new = immutable_referrer.__class__(new)
    elif doppelganger is None:
        new = immutable_referrer.__class__(
            filter(lambda it: it is not original, immutable_referrer)
        )
        if len(new) == len(immutable_referrer):
            raise NoTargetError
    else:
        i = 0
        new = []
        for item in immutable_referrer:
            if item is not original:
                new.append(item)
                continue
            i += 1
            if doppelganger is not None:
                new.append(doppelganger)
        if i == 0:
            raise NoTargetError
        new = immutable_referrer.__class__(new)
    return new


def _kidnap_and_replace_immutables(
    obj, doppelganger, immutables, permit_ids, _debug
):
    out = {'success': [], 'failure': []}
    new_doppelganger_ids = set()
    for refnom, ref in immutables:
        # gc.collect()  # probably with ignoring FrameType we don't need this
        if id(ref) not in permit_ids:
            continue  # replaced by a doppelganger down below!
        new_doppelganger = doppelgangerize_immutable(obj, ref, doppelganger)
        permit_ids.add(id(new_doppelganger))
        permit_ids.remove(id(ref))
        new_doppelganger_ids.add(id(new_doppelganger))
        result = disintegrate(
            ref,
            new_doppelganger,
            permit_ids,
            _debug=_debug,
            return_permit_ids=True
        )
        if _debug is True:
            DEPTH[0] -= 1
            print(f"----BOUNCE TO DEPTH {DEPTH[0]}----")
        # TODO: probably redundant
        permit_ids.update(result["permit_ids"])
        if len(result["failure"]) > 1:
            failtype = f"failcount: {len(result['failure'])}"
        # TODO: not sure how to determine the 'correct'
        #  number of references here. maybe have analyze_references return the
        #  number of excluded refs or something, not counting the ones it
        #  internally excludes?
        # elif res['refcount'] > 2:
        #     failtype = f"refcount: {res['refcount']}"
        else:
            out['success'].append(refnom)
            continue
        print(f"failing with recursion on {str(ref)[:200]} ({failtype})")
        out['failure'].append(refnom)
    return new_doppelganger_ids, out


def _check_doppelgangers(
    obj, boss_doppelganger, new_doppelganger_ids, permit_ids, _debug
):
    refnoms, referencing_doppelgangers = analyze_references(
        obj,
        gc.get_referrers,
        filter_history=False,
        filter_scopedict=False,
        permit_ids=new_doppelganger_ids,
        return_objects=True
    )
    if len(referencing_doppelgangers) == 0:
        return {'success': [], 'failure': []}
    # note that we have no particular expectation that any of the new
    # doppelgangers will refer to obj -- the object a doppelganger replaced
    # may have only contained a reference in a containing object that has
    # already been removed, etc. referencing_doppelgangers may be an empty
    # list, and that's fine.
    for i in range(len(refnoms)):
        refnom, ref = refnoms[i], referencing_doppelgangers[i]
        # a doppelganger should only ever be an immutable collection
        if not isinstance(ref, Collection):
            raise TypeError(f"Non-Collection doppelganger of type {type(ref)}")
        if isinstance(ref, (MutableSequence, MutableSet, MutableMapping)):
            raise TypeError(f"Mutable doppelganger of type {type(ref)}")
    # TODO, maybe: hopefully recursive doppelganger checks either:
    #  1) imply cyclic references, so are essentially impossible to verify;
    #  2) will be handled at lower levels of this disintegrate() recursion;
    #  so we are ignoring them. but it is not entirely certain it is ok.
    _, out = _kidnap_and_replace_immutables(
        obj, boss_doppelganger, referencing_doppelgangers, permit_ids, _debug
    )
    for doppelnom in chain(out['success'], out['failure']):
        doppelnom[0]['note'] = 'doppelganger'
    return out


def _resect_from_mutables(obj, doppelganger, refnoms, refs):
    immutables, results = [], {'success': [], 'failure': []}
    print('resecting from mutables')
    print(refnoms)
    for i in range(len(refnoms)):
        refnom, ref = refnoms[i], refs[i]
        try:
            if isinstance(ref, Collection):
                if not isinstance(
                    ref, (MutableSequence, MutableMapping, MutableSet)
                ):
                    immutables.append((refnom, ref))
                    continue
                resect_from_mutable_collection(obj, ref, doppelganger)
            else:
                rip_from_attrs(obj, ref, doppelganger)
            results['success'].append(refnom)
        except NoTargetError:
            results['success'].append(refnom)
            print(f"failing with no recursion on {str(ref)[:200]}!!!")
    return immutables, results


def get_stack_local_names(obj):
    framerecs = []
    _, nytta = yclept(obj, stepback=3)
    for framerec in nytta:
        print(framerec)
        names = []
        for name, scope in zip(framerec['names'], framerec['scopes']):
            if scope != 'locals':
                continue
            names.append(name)
        if len(names) > 0:
            del framerec['scopes']
            framerecs.append(framerec | {'names': names})
    return framerecs


def disintegrate(
    obj: Any,
    doppelganger: Any = None,
    permit_ids: set[int] = None,
    leftovers: Literal["ignore", "raise", "warn"] = "ignore",
    _debug=False,
    return_permit_ids=False
) -> dict:
    if _debug is True:
        ITERATIONS[0] += 1
        DEPTH[0] += 1
        print(
            f"\n----ITERATION {ITERATIONS[0]} (d: {DEPTH[0]})----\n"
            f"replacing {str(obj)[:20]} {id(obj)} with "
            f"{str(doppelganger)[:20]} {id(doppelganger)}"
        )
        if DEPTH[0] > 8:
            print("¡BAILING FOR DEPTH!")
            raise BailoutError
        if ITERATIONS[0] > 30:
            print("¡BAILING FOR ITERATIONS!")
            raise BailoutError
    if permit_ids is None:  # should only ever be true at top recursion level
        permit_ids = get_tracked_ids()
    # similarly, do not try to disintegrate references from locals dicts below
    # top level
    _, lids = scopedict_ids(
        scopenames=('locals',), getstack=True, distinguish_locals=True
    )
    permit_ids.difference_update(lids)
    # # subtract 2 from getrefcount's output because one reference exists in
    # # this function's local scope and one reference exists in getrefcount's
    # # local scope
    out = {
        "success": [],
        "failure": [],
        "start_refcount": sys.getrefcount(obj) - 2,
        # check to see what higher levels of the stack contain obj in frame
        # locals namespace. it is not possible to delete local references below
        # top level from other frames, so this may be useful information
        "local_refs": get_stack_local_names(obj),
        "tracked": gc.is_tracked(obj)
    }
    refnoms, refs = analyze_references(
        obj,
        filter_history=False,
        filter_scopedict=False,
        permit_ids=permit_ids,
        method=gc.get_referrers,
        exclude_types=(FrameType,),
        return_objects=True
    )
    # simpler case
    immutables, res = _resect_from_mutables(obj, doppelganger, refnoms, refs)
    out["success"] += res["success"]
    out["failure"] += res["failure"]
    # horrible case
    new_doppelganger_ids, res = _kidnap_and_replace_immutables(
        obj, doppelganger, immutables, permit_ids, _debug
    )
    out["success"] += res["success"]
    out["failure"] += res["failure"]
    # check new doppelgangers for presence of obj at top level
    if len(new_doppelganger_ids) > 0:
        res = _check_doppelgangers(
            obj, doppelganger, new_doppelganger_ids, permit_ids, _debug
        )
        out["success"] += res["success"]
        out["failure"] += res["failure"]
    out |= {"refcount": sys.getrefcount(obj) - 2}
    if out["refcount"] < 0:
        warnings.warn(
            "less than the expected number of references during closeout."
        )
    if (leftovers == "warn") and (
        len(out["failed"]) + out["refcount"] > 0
    ):
        warnings.warn("leftover references after disintegration")
    if (leftovers == "raise") and (
        len(out["failed"]) + out["refcount"] > 0
    ):
        raise StillReferencedError
    if return_permit_ids is True:
        out |= {'permit_ids': permit_ids}
    if ITERATIONS[0] == 4 and DEPTH[0] == 1:
        for r in gc.get_referrers(obj):
            print(yclept(r))
            try:
                r.clear()
                print('called clear')
            except AttributeError as ae:
                print(ae)
    return out


def arbput(
    *objects,
    mapping=None,
    maxdepth=3,
    size=20,
    valtypes=(list, dict),
    keytypes=(str,),
):
    entries = []
    if mapping is None:
        mapping = random_nested_dict(
            size, maxdepth=maxdepth, types=valtypes, keytypes=keytypes
        )
    targets = random.choices(
        tuple(mapping.keys()), k=min(len(mapping.keys()), len(objects))
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
