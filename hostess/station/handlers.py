"""helper functions for actors."""
from __future__ import annotations
import datetime as dt
import json
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
import os
from pathlib import Path
import sys
from types import ModuleType
from typing import Optional, Union

from cytoolz import valmap
from dustgoggles.func import gmap
from dustgoggles.structures import unnest

from google.protobuf.message import Message

from hostess.station.messages import unpack_obj
import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import enum, m2d
from hostess.subutils import (
    make_watch_caches,
    defer,
    watched_process,
    deferinto,
)


def get_module(module_name: str) -> ModuleType:
    """
    dynamically import a module by name. check to see if it's already in
    sys.modules; if not, just try to import it; if that doesn't work, try to
    interpret module_name as a path.
    """
    if module_name in sys.modules:
        return sys.modules[module_name]
    if Path(module_name).stem in sys.modules:
        return sys.modules[module_name]
    try:
        return import_module(module_name)
    except ModuleNotFoundError:
        pass
    spec = spec_from_file_location(Path(module_name).stem, module_name)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[Path(module_name).stem] = module
    return module


def unpack_callargs(arguments: list[pro.PythonObject]):
    """unpack PythonObject Messages into a dict of kwargs."""
    kwargs = {}
    for arg in arguments:
        if any((arg.value is None, arg.name is None)):
            raise ValueError("need both value and argument name")
        value = unpack_obj(arg)
        kwargs[arg.name] = value
    return kwargs


def make_function_call(action: pro.Action):
    """
    factory function for creating a deferred callable, along with caches that
    will contain the callable's return value, stdout, and stderr (if relevant),
    from an Action defined in an Instruction.
    """
    if action.func is None:
        raise TypeError("Can't actually do this without a function.")
    if action.module is not None:
        func = getattr(get_module(action.module), action.func)
    else:
        func = getattr("__builtins__", action.func)
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


def actiondict(action: Message) -> dict:
    """
    standardized dict for recording running action. results/stdout/stderr
    may be inserted into this dict. it is suitable for being inserted as a
    value of a Node.actions dict.
    """
    return {
        "name": action.name,
        "id": action.id,
        "start": dt.datetime.utcnow(),
        "stop": None,
        "status": "running",
    }


def tail_file(
    position: Optional[int], *, path: Optional[Path] = None, **_
) -> tuple[Optional[int], list[str]]:
    """simple file-tail function for use in Sensors that watch a file."""
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
    """simple ls diff for use by Sensors that watch a directory."""
    if path is None:
        return contents, []
    if not path.exists():
        return contents, []
    current = list(map(str, path.iterdir()))
    if contents is None:
        return current, current
    return current, list(set(current).difference(contents))


def flatten_into_json(event: Union[Message, dict], maxsize: int = 64) -> str:
    """very simple, semi-placeholder log-formatting function."""
    # TODO: if this ends up being unperformant with huge messages, do something
    if isinstance(event, Message):
        event = m2d(event)
    return json.dumps(gmap(lambda v: v[:maxsize], valmap(str, unnest(event))))
