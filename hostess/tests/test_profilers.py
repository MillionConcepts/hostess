import time

import numpy as np

from hostess.monitors import Stopwatch
from hostess.profilers import Profiler, analyze_referents, analyze_referrers, di, \
    identify
from hostess.tests.utilz import pointlessly_nest


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


def test_ref_analysis():
    """
    test that the reference printing functions properly print reflexive
    references
    """
    list_1 = [1, 2, 3]
    list_2 = [2, 3, list_1]
    assert analyze_referents(list_2, verbose=False)[0][0] is list_1
    assert analyze_referrers(list_1, verbose=False)[0][0] is list_2


def test_di():
    """test that di() works and doesn't segfault"""
    somelist = [1, 2, 3]
    assert pointlessly_nest(id(somelist), di)[0] == 1


def test_identify():
    """test that identify() fetches desired identifiers"""
    arr = np.random.default_rng().integers(0, 255, 100000, np.uint8)
    identification = pointlessly_nest(arr, identify)
    assert identification['id'] == id(arr)
    assert identification['type'] == np.ndarray
    assert identification['line'] is None
    assert identification['r'] == repr(arr)[:25]
    assert identification['size'] == 0.1
