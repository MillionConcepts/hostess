"""module that leaks memory"""
from inspect import stack, currentframe

import numpy as np

from hostess.profilers import _yclept_framerec, describe_frame_contents

class _InnocuousNamespace:
    def __init__(self):
        SECRET_ENCAPSULATED_GLOBAL = []
        self.secret = SECRET_ENCAPSULATED_GLOBAL


_innocuous = _InnocuousNamespace()

# class _ArrayLogger:
#     def __init__(self, array):
#         self.array = array
#
#     def log(self):
#         pass
#
#     def __del__(self):
#         # if id(self.array) not in map(
#         #         id, _innocuous._InnocuousNamespace__secret
#         # ):
#         #     _innocuous._InnocuousNamespace__secret.append(self.array)
#         _innocuous.secret.append(self.array)
#         pass


def get_poisson_distribution(lam=1, hugeness=20):
    array = np.random.poisson(lam, 2 ** hugeness)
    # record activity using our highly performant logger
    # _innocuous.secret.append(array)
    # _ArrayLogger(array).log()
    return array
