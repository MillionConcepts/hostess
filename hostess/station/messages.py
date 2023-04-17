import random
import sys
import struct
from types import NoneType

import dill
from dustgoggles.func import gmap
from more_itertools import split_when, all_equal
import numpy as np

import hostess.station.proto.station_pb2 as pro
from hostess.station.proto_utils import make_timestamp


# TODO: optional base64 encoding for some channels
def byteorder():
    return "<" if sys.byteorder == "little" else ">"


def scalarchar(scalar):
    if not isinstance(scalar, (str, bytes, int, float, bool, NoneType)):
        raise TypeError(f"{type(scalar)} is not supported by scalarchar.")
    if isinstance(scalar, (str, bytes, NoneType)):
        repeat = len(scalar) if scalar is not None else 1
        return f"{repeat}s", type(scalar).__name__.lower()
    return np.min_scalar_type(scalar).char, None


def obj2scanf(obj):
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
    if fields.get("id") is None:
        fields["id"] = random.randint(int(1e7), int(1e8))
    action = pro.Action(description=description, **fields)
    if (action.WhichOneof("command") is None) and (description is None):
        raise TypeError("must pass a description or command message.")
    return action


def default_arg_packing(arguments):
    interp = []
    for k, v in arguments.items():
        obj = pack_arg(k, v)
        interp.append(obj)
    return interp


def pack_arg(name, obj):
    if isinstance(obj, (str, bytes, int, float)):
        scanf, chartype = obj2scanf(obj)
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
    func, module=None, arguments=None, context="thread", **action_fields
):
    if "name" not in action_fields:
        action_fields["name"] = func
    try:
        assert isinstance(arguments[0], pro.PythonObject)
        arguments = arguments
    except (AssertionError, KeyError):
        arguments = default_arg_packing(arguments)
    call = pro.FunctionCall(
        func=func, module=module, context=context, arguments=arguments
    )
    return make_action(**action_fields, functioncall=call)


def make_instruction(instructiontype, **kwargs):
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
