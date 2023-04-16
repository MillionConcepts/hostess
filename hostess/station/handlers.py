"""default action handlers"""
from __future__ import annotations
import datetime as dt
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
import json
import os
from pathlib import Path
import struct
import sys

import dill
from google.protobuf.message import Message

import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import enum
from hostess.subutils import make_watch_caches, defer, watched_process, \
    deferinto


def tbd(*_, **__):
    raise NotImplementedError


def get_module(module_name: str):
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


def unpack_callargs(arguments):
    kwargs = {}
    for arg in arguments:
        if any((arg.value is None, arg.name is None)):
            raise ValueError("need both value and argument name")
        if enum(arg, "compression") not in ("nocompression", None):
            raise NotImplementedError
        if enum(arg, "serialization") == "json":
            value = json.loads(arg.value)
        elif enum(arg, "serialization") == "pickle":
            value = dill.loads(arg.value)
        elif arg.scanf is not None:
            value = struct.unpack(arg.scanf, arg.value)
        else:
            value = arg.value
        kwargs[arg.name] = value
    return kwargs


def make_function_call(action: pro.Action):
    if action.func is None:
        raise TypeError("Can't actually do this without a function.")
    if action.module is not None:
        func = getattr(get_module(action.module), action.func)
    else:
        func = getattr("__builtins__", action.func)
    kwargs = unpack_callargs(action.arguments)
    if (ctx := enum(action, "context")) in ("thread", "unknowncontext", None):
        caches = {"result": [], "pid": [os.getpid()]}
        return caches, deferinto(func, _target=caches['result'], **kwargs)
    elif ctx in ("detached", "process"):
        fork, caches = ctx == "detached", make_watch_caches()
        call = defer(watched_process(func, caches=caches, fork=fork), **kwargs)
        return caches, call
    else:
        raise ValueError(f"unknown context {ctx}")


#
# def dispatch_task(task: Message):
#     func = {'action': dispatch_action, 'pipe': tbd}[task.WhichOneof("task")]
#     return func(task)


def actiondict(action: Message):
    """standardized dict for recording running action"""
    return {
        "name": action.name,
        "id": action.id,
        "start": dt.datetime.utcnow(),
        "stop": None
    }
