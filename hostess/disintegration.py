import gc
from inspect import currentframe
import random
from typing import Collection, Mapping, MutableMapping, MutableSequence

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

def rip_from_sequence(obj, ref):
    if isinstance(ref, MutableMapping):
        iterator = iter(ref.items())
    elif isinstance(ref, MutableSequence):
        iterator = enumerate(ref)
    else:
        return False
    index_vals = tuple(filter(lambda iv: iv[1] is obj, iterator))
    if len(index_vals) == 0:
        return False
    for index_val in index_vals:
        ref.pop(index_val[0])
    return True

def rip_from_attrs(obj, ref):
    if not hasattr(ref, "__dict__"):
        return False
    attrvals = tuple(filter(lambda kv: kv[1] is obj, ref.__dict__.items()))
    if len(attrvals) == 0:
        print('sorry')
        return False
    try:
        for attrval in attrvals:
            ref.__dict__.pop(attrval[0])
            setattr(ref, attrval[0], None)
    except AttributeError as ae:
        return False
    return True


def disintegrate(obj):
    remaining = 0
    for ref in analyze_referrers(obj, filter_history=False, verbose=False)[0]:
        print(type(ref))
        if rip_from_sequence(obj, ref):
            continue
        if rip_from_attrs(obj, ref):
            continue
        remaining += 1
    return remaining


def arbput(*objects, mapping=None):
    entries = []
    if mapping is None:
        mapping = random_nested_dict(
            10, 
            maxdepth=6, 
            types=(list, str, int), 
            keytypes=(str,)
        )
    for obj in objects:
        target = random.choice(tuple(mapping.keys()))
        if isinstance(mapping[target], Mapping):
            entries += arbput(obj, mapping=mapping[target])
        elif isinstance(mapping[target], Collection):
            mapping[target] = list(mapping[target]) + [obj]
            entries.append(mapping[target])
        else:
            mapping[target] = obj
            if not any(isinstance(v, Mapping) for v in mapping.values()):
                entries.append(mapping)
    return entries
