import re
from copy import deepcopy
from types import FunctionType
from typing import Callable

from dustgoggles.func import gmap
from dustgoggles.structures import dig_and_edit, valonly
from inspect import getsource


def has_callables(obj):
    if callable(obj):
        return True
    if "__iter__" in dir(obj) and any(callable(o) for o in obj):
        return True


def digsource(obj: Callable):
    """
    wrapper for inspect.getsource that attempts to work on objects like
    Dynamic, functools.partial, etc.
    """
    if hasattr(obj, "source"):
        return obj.source
    if isinstance(obj, FunctionType):
        return getsource(obj)
    if "func" in dir(obj):
        # noinspection PyUnresolvedReferences
        return getsource(obj.func)
    raise TypeError(f"cannot get source for type {type(obj)}")


def getdef(func: Callable, get_docstring: bool = True) -> str:
    """
    return a string containing the 'definition portion' of func's
    source code (including annotations and inline comments).
    optionally also append its docstring (if it has one).

    caveats:
    1. may not work on functions with complicated inline comments
     in their definitions.
    2. does not work on lambdas; may not work on some other classes
     of dynamically-defined functions.
    """
    defstring = re.search(
        r"def.*?\) ?(-> ?(\w|[\[\]])*?[^\n:]*)?:",
        digsource(func),
        re.M + re.DOTALL,
    ).group()
    if (func.__doc__ is None) or (get_docstring is False):
        return defstring
    return defstring + '\n    """' + func.__doc__ + '"""\n'


def maybe_getdef(obj, maxchar=100):
    try:
        return getdef(obj, get_docstring=False)[:maxchar]
    except (TypeError, ValueError, AttributeError):
        return str(obj)[:maxchar]


def sourcerec(obj):
    return {
        "name": f"{obj.__class__.__module__}{obj.__class__.__name__}",
        "def": maybe_getdef(obj),
    }


def callable_info(obj):
    if callable(obj):
        return sourcerec(obj)
    if "__iter__" in dir(obj):
        return gmap(sourcerec, obj)
    raise TypeError(f"don't know how to handle {type(obj)}")


def pack_config(config: dict[str]) -> dict[str]:
    packed = {"interface": {}, "cdict": {}}
    for k, v in config["interface"].items():
        if has_callables(v):
            packed["interface"][k] = callable_info(v)
        else:
            packed["interface"][k] = str(v)
    # TODO: find some clunky efficient way to do this one
    # noinspection PyTypedDict
    packed["cdict"] = dig_and_edit(
        deepcopy(config["cdict"]),
        valonly(has_callables),
        valonly(callable_info),
    )
    return packed


def element_dict(elements):
    return {
        k: f"{v.__class__.__module__}.{v.__class__.__name__}"
        for k, v in elements
    }


def pack_delegate(ddict: dict[str]) -> dict[str]:
    literals = (
        "inferred_status",
        "wait_time",
        "busy",
        "name",
        "pid",
        "host",
        "actors",
        "sensors",
    )
    packed = {k: ddict.get(k) for k in literals}
    packed["config"] = pack_config(
        {"cdict": ddict["cdict"], "interface": ddict["interface"]}
    )
    packed["running"] = [
        {
            "name": r["action"]["name"],
            "time": r["action"]["time"],
            "status": r["action"]["status"],
            "instruction_id": r["instruction_id"],
            "description": r["action"].get("description"),
        }
        for r in ddict["running"]
    ]
    return packed


# TODO: to avoid the potentially unbounded linear time complexity increase,
#  we need to partially cache this, or send only the most recent 100 tasks, or
#  find another optimization.
def pack_tasks(tasks: dict[int]) -> dict:
    literals = (
        "name",
        "delegate",
        "status",
        "init_time",
        "sent_time",
        "ack_time",
        "start_time",
        "end_time",
        "duration",
        "description",  # TODO: maybe a bad idea w/huge description,
    )
    return {id_: {k: t.get(k) for k in literals} for id_, t in tasks.items()}
