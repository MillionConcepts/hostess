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
        return str(obj)


def sourcerec(obj):
    return {
        'name': f"{obj.__class__.__module__}{obj.__class__.__name__}",
        'source': maybe_getsource(obj)
    }


def callable_info(obj):
    if callable(obj):
        return sourcerec(obj)
    if "__iter__" in dir(obj):
        return list(map(sourcerec, obj))
    raise TypeError(f"don't know how to handle {type(obj)}")
