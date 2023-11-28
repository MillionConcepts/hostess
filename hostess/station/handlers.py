"""shared helper functions for Station objects"""
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import (
    Any, Callable, Collection, Mapping, Optional, Sequence, Union
)

from google.protobuf.message import Message

from hostess.station.messages import unpack_obj
import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import enum, m2d
from hostess.subutils import (
    defer,
    deferinto,
    make_watch_caches,
    watched_process,
)
from hostess.utilities import get_module


def unpack_callargs(arguments: Sequence[pro.PythonObject]) -> dict[str, Any]:
    """
    unpack PythonObject Messages into a dict of kwargs.

    Args:
        arguments: sequence of PythonObject Messages

    Returns:
        dict constructed from deserialized content of `arguments`, suitable
            for being splatted into a function
    """
    kwargs = {}
    for arg in arguments:
        if any((arg.value is None, arg.name is None)):
            raise ValueError("need both value and argument name")
        value = unpack_obj(arg)
        kwargs[arg.name] = value
    return kwargs


def make_function_call(action: pro.Action) -> tuple[dict[str, list], Callable]:
    """
    parse an Action Message containing specifications for a function call and
    create a "deferred" version of a call that matches those specifications.

    Args:
        action: hostess Action Message that specifies a function call.

    Returns:
        caches: dict of lists the deferred call will write its stdout, stderr,
            and return values into
        deferred: partially-evaluated and wrapped function constructed from
            the function call specification in `action`. call it to actually
            perform the action.
    """
    if action.func is None:
        raise TypeError("Can't actually do this without a function.")
    if action.module is not None:
        try:
            module = get_module(action.module)
        except (AttributeError, ImportError):
            raise FileNotFoundError("module not found")
        try:
            func = getattr(module, action.func)
        except AttributeError:
            raise ImportError("function not found in module")
    else:
        try:
            func = getattr("__builtins__", action.func)
        except AttributeError:
            raise ImportError("function not found in builtins")
    kwargs = unpack_callargs(action.arguments)
    if (ctx := enum(action, "context")) in ("thread", "unknowncontext", None):
        caches = {"result": [], "pid": [os.getpid()]}
        return caches, deferinto(func, _target=caches["result"], **kwargs)
    elif ctx in ("detached", "process"):
        fork, caches = ctx == "detached", make_watch_caches()
        call = defer(watched_process(func, caches=caches, fork=fork), **kwargs)
        return caches, call
    else:
        raise ValueError(f"unknown context {ctx}")


def make_actiondict(action: pro.Action) -> dict[str, Any]:
    """
    construct a standardized dict for recording the results of an action
    described by `action`.

    Args:
        action: a pro.Action message

    Returns:
        a dict initialized from basic identifying information in `action`,
            intended to be used as a value of a `Node.actions` dict.
    """
    return {
        "name": action.name,
        "id": action.id,
        "description": action.description,
        "start": dt.datetime.now(dt.UTC),
        "stop": None,
        "status": "running",
    }


def tail_file(
    position: Optional[int], *, path: Optional[Path] = None, **_
) -> tuple[Optional[int], list[str]]:
    """
    simple file-tail function for use in Sensors that watch a file.

    Args:
        position: byte offset from start of file at which to begin reading.
            if None, start at the beginning of the file.
        path: path to file. Typically partially evaluated into the function by
            the Sensor, not explicilty passed.

    Returns:
        end: position of last read byte of file, or None if the file
            doesn't exist.
        lines: all lines of file between `position` and `end`.
    '"""
    if path is None:
        return position, []
    if not path.exists():
        return None, []
    if position is None:
        position = os.stat(path).st_size - 1
    if os.stat(path).st_size - 1 == position:
        return position, []
    if os.stat(path).st_size - 1 < position:
        position = os.stat(path).st_size - 1
        return position, []
    with path.open() as stream:
        stream.seek(position)
        lines = stream.readlines()
        position = stream.tell()
        return position, lines


def watch_dir(
    contents: list[str], *, path: Optional[Path] = None, **_
) -> tuple[Optional[list[str]], list[str]]:
    """
    simple ls-like diff for use by Sensors intended that watch a directory.
    """
    if path is None:
        return contents, []
    if not path.exists():
        return contents, []
    current = list(map(str, path.iterdir()))
    if contents is None:
        return current, []
    return current, list(set(current).difference(contents))


SKIPKEYS = frozenset(
    {
        'delegateid',
        'state',
        'running',
        'arguments',
        'localcall',
        'data',
        'result',
        'config'
    }
)
"""
keys of Node-internal data structures that we don't generally want to write 
into logs, either because they often have huge values or because they're 
generally redundant.
"""


def json_sanitize(
    value: Any,
    maxlen: int = 128,
    maxdepth: int = 1,
    skipkeys: Collection[str] = SKIPKEYS,
    depth: int = 0,
    skip: bool = False
) -> Union[str, int, float, list[str], dict[str, Union[str, dict[str, str]]]]:
    """
    Attempt to make an object representable in JSON, with standardized
    formatting conventions that include automated skipping, truncation, etc.
    Primarily intended as a helper function for `flatten_for_json()`.

    Args:
        value: object to make representable
        maxlen: maximum length of string representations of elements of
            sanitized object
        maxdepth: maximum depth to dig into nested objects.
        skipkeys: keys or fields to omit from output.
        depth: current dig depth. automatically incremented in recursive calls
            to this function.
        skip: if True, just return the literal string '<skipped>'

    Returns:
        JSON-sanitized version of `value`.
    """
    if skip is True:
        return "<skipped>"
    if isinstance(value, Message):
        value = m2d(value)
    if isinstance(value, (int, float)):
        return value
    elif isinstance(value, bytes):
        value = "<binary>"
    if isinstance(value, str):
        value = str(value[:maxlen])
    elif isinstance(value, Mapping):
        if depth > maxdepth:
            value = "<skipped mapping at max log depth>"
        else:
            return {
                json_sanitize(k): json_sanitize(
                    v, maxlen, maxdepth, skipkeys, depth + 1, k in skipkeys
                )
                for k, v in value.items()
            }
    elif isinstance(value, Collection):
        return [json_sanitize(e) for e in value]
    else:
        value = repr(value)
    return value[:maxlen]


def flatten_for_json(
    event: Union[Message, dict],
    maxlen: int = 128,
    maxdepth: int = 3,
    skipkeys: Collection[str] = SKIPKEYS
) -> dict[str, str]:
    """
    simple log-formatting function.

    Args:
        event: protobuf Message or dict to flatten
        maxlen: maximum length for stringified values of flattened dict
        maxdepth: maximum depth to dig into `event` before truncating
        skipkeys: keys / Message field of `event` to ignore in logginc

    Returns:
         flattened version of `event` w/stringified values, possibly truncated,
              ready to be passed to `json.dump()`.
    """
    # TODO: if this ends up being unperformant with huge messages, do something
    if isinstance(event, Message):
        event = m2d(event)
    return json_sanitize(event, maxlen, maxdepth, skipkeys)
