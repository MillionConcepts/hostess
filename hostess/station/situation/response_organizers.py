"""
functions for describing and organizing Station attributes in preparation for
sending them to the situation frontend
"""

from copy import deepcopy
from inspect import getsource
import re
from types import FunctionType
from typing import Callable, Any, Union

from dustgoggles.func import gmap
from dustgoggles.structures import dig_and_edit, valonly

from hostess.station.station import Station


def has_callables(obj: Any) -> bool:
    if callable(obj):
        return True
    if "__iter__" in dir(obj) and any(callable(o) for o in obj):
        return True
    return False


def digsource(obj: Callable) -> str:
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


def maybe_getdef(obj: Any, maxchar: int = 100) -> str:
    try:
        return getdef(obj, get_docstring=False)[:maxchar]
    except (TypeError, ValueError, AttributeError):
        return str(obj)[:maxchar]


def sourcerec(obj: Any) -> dict[str, str]:
    rec = {"def": maybe_getdef(obj)}
    if hasattr(obj, "__name__"):
        rec["name"] = obj.__name__
    else:
        rec["name"] = obj.__class__.__name__
    return rec


def callable_info(obj: Any) -> Union[dict[str, str], tuple[dict[str, str]]]:
    if callable(obj):
        return sourcerec(obj)
    if "__iter__" in dir(obj):
        return gmap(sourcerec, obj)
    raise TypeError(f"don't know how to handle {type(obj)}")


def pack_config(cdict, interface) -> tuple[dict[str], dict[str]]:
    cdict_out, interface_out = {}, {}
    for k, v in interface.items():
        if has_callables(v):
            interface_out[k] = callable_info(v)
        else:
            interface_out[k] = str(v)
    # TODO: find some clunky efficient way to do this one
    # noinspection PyTypedDict
    cdict_out = dig_and_edit(
        deepcopy(cdict),
        valonly(has_callables),
        valonly(callable_info),
    )
    return cdict_out, interface_out


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
        "infocount",
    )
    packed = {k: ddict.get(k) for k in literals}
    packed["logfile"] = ddict["init_params"]["logfile"]
    packed["cdict"], packed["interface"] = pack_config(
        ddict.get("cdict", {}), ddict.get("interface", {})
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
        "description",  # TODO: maybe a bad idea w/huge description
    )
    return {
        id_: {k: t.get(k) for k in literals} for id_, t in tuple(tasks.items())
    }


def situation_of(station: Station) -> dict:
    # noinspection PyDictCreation
    props = {
        "name": station.name,
        "host": station.host,
        "port": station.port,
        "logfile": station.logfile,
        "actors": station.identify_elements("actors"),
        # TODO: make this more efficient
        "delegates": {d["name"]: pack_delegate(d) for d in station.delegates},
        # weird dict constructions for protection against mutation
        "threads": {k: v._state for k, v in station.threads.items()},
        "tasks": pack_tasks(station.tasks),
    }
    props["cdict"], props["interface"] = pack_config(
        station.config["cdict"], station.config["interface"]
    )
    return props
