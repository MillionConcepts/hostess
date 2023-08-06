from operator import attrgetter

from dustgoggles.structures import dig_and_edit, valonly
from inspect import getsource


def has_callables(obj):
    if callable(obj):
        return True
    if "__iter__" in dir(obj) and any(callable(o) for o in obj):
        return True

def maybe_getsource(obj):
    try:
        return getsource(obj)
    except (TypeError, ValueError):
        return obj


# TODO: need to use a link or something for textualize Tree --
#  newlines are verboten
def callables_to_source(obj):
    if callable(obj):
        return attrgetter('__qualname__')(obj)
    if "__iter__" in dir(obj):
        return list(map(attrgetter('__qualname__'), obj))