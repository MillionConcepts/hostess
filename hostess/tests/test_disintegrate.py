import gc
import sys
import time

import numpy as np
from cytoolz import identity

from hostess.disintegration import disintegrate
from hostess.monitors import memory
from hostess.profilers import yclept


# from hostess.profilers import di, yclept
# from hostess.tests._leaky_module import get_poisson_distribution, _innocuous

def rval(val):
    return len(val)
#
def test_disintegrate_2():
    # YOU CAN EXECUTE A MODULE TWICE WITH DIFFERENT QUALIFIED NAMES?!!
    # from _leaky_module import innocuous
    # array = get_poisson_distribution(hugeness=25)
    # usage = memory()
    # print(usage)
    # assert usage > array.size * array.itemsize
    # del array
    # gc.collect()
    print('initial mem', memory())
    # assert abs((memory() - usage) / usage) < 0.1
    # array = get_poisson_distribution(hugeness=26)
    array = np.random.poisson(2, (4000, 10000))
    # assert memory() > usage * 0.95 + array.size * array.itemsize
    # # print('caller locals', id(locals()))
    # print(_innocuous.secret)
    # len(array)
    print('mem after zeros', memory())
    # refs = gc.get_referrers(array)
    print(sys.getrefcount(array))
    yclept(array)
    # disintegrate(array, _debug=True)
    # _ = rval(array)
    gc.collect()
    print(sys.getrefcount(array))
    del array
    gc.collect()
    # time.sleep(0.2)
    # refs2 = gc.get_referrers(di(array_id))
    # for r in refs2:
    #     if id(r) in tuple(map(id, refs)):
    #         continue
    #     print(yclept(r))
        # print(id(r))
        # print(r.keys())
        # pass
    # print(_innocuous.secret)
    # # print(result)
    # import sys
    # gc.collect()
    print('mem after delete', memory())
    # print(sys.getrefcount(di(array_id)[1]))
    # array = np.random.poisson(2, (4000, 10000))
    # time.sleep(1)
    # gc.collect()
    # print('mem after new assign', memory())
    # array = np.random.poisson(2, (4000, 10000))
    # len(array)
    # time.sleep(1)
    # gc.collect()
    # print('mem after new assign', memory())
    # print(di(array_id))
    # print(1)
    # print(sys.getrefcount(array))
    # for r in gc.get_referrers(array):
    #     print(r)
    # array_id = id(array)
    # print(_innocuous.secret)
    # for _ in range(10):
    #     print(di(array_id))


    # assert memory() < (usage - array.size * array.itemsize) * 1.05



test_disintegrate_2()
