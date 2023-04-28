"""utilities for interpreting and constructing specific Messages."""

from __future__ import annotations

import json
import random
import sys
import struct
from types import NoneType
from typing import Optional, Any, Collection, Literal, Union

import dill
from dustgoggles.func import gmap
from more_itertools import split_when, all_equal
import numpy as np

from hostess.station.proto import station_pb2 as pro
from hostess.station.proto_utils import make_timestamp, enum, dict2msg


def byteorder() -> str:
    """what is the system byteorder (as struct.Struct wants to hear it)"""
    return "<" if sys.byteorder == "little" else ">"


def scalarchar(scalar) -> tuple[str, Optional[str]]:
    """
    what is the correct struct code for this object? also return a description
    of the 'string' type if it is str, bytes, or NoneType
    """
    if not isinstance(scalar, (str, bytes, int, float, bool, NoneType)):
        raise TypeError(f"{type(scalar)} is not supported by scalarchar.")
    if isinstance(scalar, (str, bytes, NoneType)):
        repeat = len(scalar) if scalar is not None else 1
        return f"{repeat}s", type(scalar).__name__.lower()
    # noinspection PyUnresolvedReferences
    return np.min_scalar_type(scalar).char, None


def obj2scanf(obj) -> tuple[str, Optional[str]]:
    """
    construct a struct / scanf string for an object, along with a description
    of the 'string' type if it is str, bytes, or None. can handle most basic
    data types, as well as unmixed lists or tuples of the same, although it's
    silly to use it on lists or tuples containing mixed data types or many
    distinct strings/bytestrings -- it would be easier just to pickle them
    or dump them as JSON, because the struct string will be long enough to
    cancel out any benefits of the terser binary packing.
    """
    if not isinstance(
        obj, (str, bytes, int, float, list, tuple, bool, NoneType)
    ):
        raise TypeError(f"{type(obj)} is not supported.")
    if not isinstance(obj, (list, tuple)):
        return scalarchar(obj)
    chars = gmap(scalarchar, obj)
    if all(c[1] is None for c in chars):
        chars = split_when(chars, lambda x, y: x[0] != y[0])
        return "".join([f"{len(char)}{char[0][0]}" for char in chars]), None
    if not all_equal(c[1] for c in chars):
        raise TypeError("arrays of mixed string types are not supported.")
    return "".join(f"{char[0]}" for char in chars), chars[0][1]


def make_action(description=None, **fields):
    """construct a default pro.Action message"""
    if fields.get("id") is None:
        fields["id"] = random.randint(int(1e7), int(1e8))
    action = pro.Action(description=description, **fields)
    if (action.WhichOneof("command") is None) and (description is None):
        raise TypeError("must pass a description or command message.")
    return action


def default_arg_packing(kwargs: dict[str, Any]) -> list[pro.PythonObject]:
    """
    pack a kwarg dict into a list of pro.PythonObjects to be inserted into a
    Message.
    """
    interp = []
    for k, v in kwargs.items():
        obj = pack_obj(v, k)
        interp.append(obj)
    return interp


# TODO: optional base64 encoding for some channels
def pack_obj(obj: Any, name: str = "") -> pro.PythonObject:
    """
    default function for serializing an in-memory object as a pro.PythonObject
    Message.
    """
    if isinstance(obj, (str, bytes, int, float)):
        scanf, chartype = obj2scanf(obj)
        if isinstance(obj, str):
            obj = obj.encode('utf-8')
        elif isinstance(obj, NoneType):
            obj = b"\x00"
        obj = pro.PythonObject(
            name=name,
            scanf=scanf,
            chartype=chartype,
            value=struct.pack(scanf, obj),
        )
    else:
        obj = pro.PythonObject(
            name=name, serialization="dill", value=dill.dumps(obj)
        )
    return obj


def make_function_call_action(
    func: str,
    module: Optional[str] = None,
    kwargs: Union[list[pro.PythonObject], dict[str, Any], None] = None,
    context: Literal["thread", "process", "detached"] = "thread",
    **action_fields
) -> pro.Action:
    """
    make an Action describing a function call task to be inserted into an
    Instruction.
    """
    if "name" not in action_fields:
        action_fields["name"] = func
    try:
        # if kwargs is already a list of PythonObjects, don't try to repack
        assert isinstance(kwargs[0], pro.PythonObject)
        objects = kwargs
    except (AssertionError, KeyError):
        objects = default_arg_packing(kwargs)
    call = pro.FunctionCall(
        func=func, module=module, context=context, arguments=objects
    )
    return make_action(**action_fields, functioncall=call)


def make_instruction(instructiontype, **kwargs) -> pro.Instruction:
    """make an Instruction Message."""
    if kwargs.get("id") is None:
        kwargs['id'] = random.randint(int(1e7), int(1e8))
    instruction = pro.Instruction(
        time=make_timestamp(),
        type=instructiontype,
        **kwargs
    )
    if instruction.type == "do" and instruction.task is None:
        raise ValueError("must assign a task for a 'do' action.")
    return instruction


def unpack_obj(obj: pro.PythonObject) -> Any:
    """default deserialization function for pro.PythonObject Messages"""
    if enum(obj, "compression") not in ("nocompression", None):
        # TODO: handle inline compression
        raise NotImplementedError
    if enum(obj, "serialization") == "json":
        value = json.loads(obj.value)
    elif enum(obj, "serialization") == "dill":
        value = dill.loads(obj.value)
    elif obj.scanf:
        unpacked = struct.unpack(obj.scanf, obj.value)
        if any(isinstance(v, bytes) for v in unpacked):
            chartype = enum(obj, "chartype")
            if chartype == "str":
                unpacked = tuple(map(lambda s: s.decode('utf-8'), unpacked))
            elif chartype == "nonetype":
                unpacked = [None for _ in unpacked]
        value = unpacked if len(unpacked) > 1 else unpacked[0]
    else:
        value = obj.value
    return value


def completed_task_msg(actiondict: dict, steps=None) -> pro.TaskReport:
    """
    construct a TaskReport from an action dict (like the ones produced by
    watched_process and derivatives) to add to an Update.
    """
    if steps is not None:
        raise NotImplementedError
    fields = {}
    if len(actiondict['result']) > 1:
        raise NotImplementedError
    if len(actiondict['result']) == 1:
        fields['result'] = pack_obj(actiondict.pop('result')[0])
    fields['time'] = dict2msg(actiondict, pro.ActionTime)
    action = dict2msg(actiondict, pro.ActionReport)
    action.MergeFrom(pro.ActionReport(**fields))
    return pro.TaskReport(instruction_id=actiondict['id'], action=action)


