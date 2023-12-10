import gc
import time
from functools import partial
from itertools import chain

import numpy as np

from hostess.monitors import Stopwatch
from hostess.profilers import (
    analyze_references,
    di,
    identify,
    IdentifyResult,
    Profiler,
    RefAlarm,
    Refnom
)
from hostess.tests.utilz import pointlessly_nest, Aint


def test_profiler():
    """simple test of Profiler for codeblock timing"""
    watch = Stopwatch(digits=1)
    profiler = Profiler({'time': watch})
    with profiler.context("ctx_1"):
        time.sleep(0.1)
    with profiler.context("ctx_2"):
        time.sleep(0.2)
    with profiler.context("ctx_3"):
        time.sleep(0.3)
    assert profiler.labels['ctx_1']['time'] == 0.1
    assert profiler.labels['ctx_2']['time'] == 0.2
    assert profiler.labels['ctx_3']['time'] == 0.3


def test_di():
    """test that di() works and doesn't segfault"""
    somelist = [1, 2, 3]
    assert pointlessly_nest(id(somelist), di)[0] == 1


def test_identify():
    """test that identify() fetches desired identifiers"""
    arr = np.random.default_rng().integers(0, 255, 100000, np.uint8)
    identification = pointlessly_nest(arr, partial(identify, getsize=True))
    assert identification['id'] == id(arr)
    assert identification['type'] == np.ndarray
    assert identification['line'] is None
    assert identification['r'] == str(arr)[:25]
    assert identification['size'] == 0.1


def test_analyze_references_1():
    """test basic reference-finding functionality of analyze_references"""
    list_1 = [1, 2, 3]
    list_2 = [2, 3, list_1]
    assert analyze_references(
        list_1, gc.get_referrers, return_objects=True
    )[1][0] is list_2
    assert analyze_references(
        list_2, gc.get_referents, return_objects=True
    )[1][0] is list_1


def test_analyze_references_2():
    """test more sophisticated functionality of analyze_references"""
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
        zer = []
        for z in ztup:
            zer.append(
                analyze_references(
                    z,
                    method=gc.get_referrers,
                    return_objects=True)
            )
        ztr = analyze_references(
            ztup, method=gc.get_referents, return_objects=True
        )
        z1r = analyze_references(
            z1, method=gc.get_referents, return_objects=True
        )
        return zer, ztr, z1r

    z1_z2_z3_referrers, ztup_referents, z1_referents = f1(1)
    # tuple of (IdentifyResult, list[Refnom]) for each object that refers to
    # z1 (these objects should be xtup, ytup, and ztup)
    meta1: list[tuple[IdentifyResult, list[Refnom]]] = z1_z2_z3_referrers[0][0]
    assert len(meta1) == 3
    # first reference context description (a Refnom dict) for z1, z2, z3
    meta1_f: list[Refnom] = [meta1[i][1][0] for i in range(3)]
    # first explicitly-assigned variable name found for xtup, ytup, ztup
    meta0_names: list[str] = [meta_f['names'][0] for meta_f in meta1_f]
    assert meta0_names == ['xtup', 'ytup', 'ztup']
    # basic identifiers for ztup
    ztup_identifyresult: IdentifyResult = meta1[2][0]
    # should be str(ztup)
    assert ztup_identifyresult['r'] == '(Aint(1), Aint(2), Aint(3))'
    # same deal but for objects that refer to z2 (should be ytup and ztup)
    meta2 = z1_z2_z3_referrers[1][0]
    assert len(meta2) == 2
    meta2_f = [meta2[i][1][0] for i in range(2)]
    meta2_names = [meta_f['names'][0] for meta_f in meta2_f]
    assert meta2_names == ['ytup', 'ztup']
    # z1 should refer only to its type
    assert z1_referents[1] == [Aint]
    # we should have found the 'original' name of each element of ztup
    assert set(
        chain(map(lambda rec: rec['names'][0], ztup_referents[0][2][1]))
    ) == {'x1', 'y1', 'z1'}


def test_refalarm():
    """simple test of basic RefAlarm functionality"""
    x, y, z = [0, 0, 0], [0, 0, 0], [0, 0, 0]
    x[1] = z

    def pointlessly_assign_to_index_1(seq, obj):
        seq[1] = obj

    alarm = RefAlarm(verbosity="quiet")
    with alarm.context():
        pointlessly_assign_to_index_1(y, z)
    assert alarm.refcaches['default'] == [
        [{'name': 'test_refalarm', 'mismatches': {'z': 1}}]
    ]
