import time

from hostess.monitors import Stopwatch
from hostess.profilers import Profiler, print_referents, print_referrers, di


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


def test_refprinters():
    """
    test that the reference printing functions properly print reflexive
    references
    """
    list_1 = [1, 2, 3]
    list_2 = [2, 3, list_1]
    assert print_referents(list_2)[0] is list_1
    assert print_referrers(list_1)[0] is list_2


def test_di():
    """test that di() works and doesn't segfault"""
    def pointless_layer(id_):
        return di(id_)[0]

    somelist = [1, 2, 3]
    assert pointless_layer(id(somelist)) == 1



