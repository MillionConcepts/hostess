"""utilities for interpreting and constructing hostess protobuf Messages."""

from __future__ import annotations

from ast import literal_eval
import datetime as dt
from functools import cached_property, cache
from itertools import accumulate
import json
from operator import add, attrgetter
import random
import struct
import sys
from types import MappingProxyType as MPt, NoneType
from typing import Optional, Any, Literal, Mapping, MutableMapping, Union

from cytoolz import groupby
import dill
from dustgoggles.func import gmap
from dustgoggles.structures import dig_for_values
from google.protobuf.internal.well_known_types import Duration, Timestamp
from google.protobuf.message import Message
from google.protobuf.pyext._message import (
    RepeatedCompositeContainer,
    RepeatedScalarContainer,
    ScalarMapContainer,
)
from more_itertools import split_when, all_equal
import numpy as np

from hostess.station.comm import make_comm
from hostess.station.proto import station_pb2 as pro
from hostess.station.proto_utils import (
    enum,
    make_duration,
    make_timestamp,
    proto_formatdict,
)
from hostess.utilities import mb, yprint


def byteorder() -> str:
    """
    format system byteorder for inclusion in a struct.Struct format string.

    Returns:
        "<" on little-endian platforms, ">" on big-endian platforms.
    """
    return "<" if sys.byteorder == "little" else ">"


def scalarchar(
    scalar: Union[str, bytes, int, float, list, tuple, bool, None]
) -> tuple[str, Optional[str]]:
    """
    Determine an appropriate format code and, if necessary, byte string type
    identifier for a 'scalar' object, to be used as a component of a
    struct.Struct format string.

    Args:
        scalar: object for which to determine code / identifier

    Returns:
        code: format code for `scalar` suitable for inclusion in struct string
        string_identifier: "none" if scalar is `None`; "str" if scalar is a
            `str`; "bytes" if scalar is a `bytes`; `None` otherwise.

    Raises:
        TypeError if `scalar` is not actually a scalar (as defined here).
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
    construct a struct / scanf format string for `obj`, along with a code for
    the 'string' type if it is `str`, `bytes`, or `NoneType` (struct strings
    represent all these types with 's', so an additional code is required for
    recipients to reconstruct them as the correct Python type).

    This function accepts most primitive Python types, as well as lists or
    tuples of primitive types. Does not accept sequences of mixed 'string'
    types.

    Note that this function is generally not useful for lists or tuples
    containing mixed data types or many distinct strings/bytestrings. It will
    in general be more efficient to serialize them some other way, because the
    struct string itself will often be long enough to negate the benefits of
    terse binary packing.

    Args:
        obj: object for which to construct format string

    Returns:
        format_string: format string for `obj`
        string_code: "none" if `obj` is `None` or a `list` / `tuple` of `None`;
            "str" if `obj` is a `str` or a `list` '/ `tuple` of `str`;
            "bytes" if scalar is a `bytes` or a `list` / `tuple` of `bytes`;
            `None` otherwise.

    Raises:
        TypeError if `obj2scanf()` does not know how to handle `obj`'s type.
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


def default_arg_packing(kwargs: dict[str, Any]) -> list[pro.PythonObject]:
    """
    convert a dict that represents kwargs for a function call into a list of
    pro.PythonObjects.

    Args:
        kwargs: `dict` containing keyword arguments for a function call, in the
            same format you would use if you were to locally execute
            `target_function(**kwargs)`

    Returns:
        list of pro.PythonObject Messages giving names and serialized values
            of `kwargs`
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
    Message. If `obj` is "scalar", serialize it using simple struct formatting;
    if it is a `np.ndarray` of non-object type, use its `ndarray.tobytes()`
    representation; otherwise, serialize it using `dill`. This is a good
    function for general-purpose object passing, and is used extensively in
    internal Node behaviors. However, it may in some cases be more efficient
    to implement serialization functions optimized for specific data formats.

    Args:
        obj: object to serialize as a `pro.PythonObject` Message.
        name: optional name for `obj` in Message; useful if you intend the
            recipient to pass `obj` as a keyword argument to a function.

    Returns:
        `obj` serialized as a `pro.PythonObject`.
    """
    if isinstance(obj, pro.PythonObject):
        return obj
    elif isinstance(obj, NoneType):
        (scanf, chartype), obj = obj2scanf(obj), b"\x00"
    elif isinstance(obj, (str, bytes, int, float)):
        scanf, chartype = obj2scanf(obj)
        if isinstance(obj, str):
            obj = obj.encode("utf-8")
    elif isinstance(obj, np.ndarray):
        dtype = str(obj.dtype)
        if dtype == 'object' or ", 'O', " in dtype:
            # object dtype does not have stable byte-level representation
            return pro.PythonObject(
                name=name, serialization="dill", value=dill.dumps(obj)
            )
        return pro.PythonObject(name=name, dtype=dtype, value=obj.tobytes())
    else:
        return pro.PythonObject(
            name=name, serialization="dill", value=dill.dumps(obj)
        )
    return pro.PythonObject(
        name=name,
        scanf=scanf,
        chartype=chartype,
        value=struct.pack(scanf, obj),
    )


# TODO: optional base64 encoding for some channels
def make_action(description: Optional[dict[str, str]] = None, **fields):
    """
    construct a default pro.Action message.

    Args:
        description: optional dict to use as "description" field of Action.
            For terse task descriptions to agents that may only need to hear
            a single name or number to know what to do.
        **fields: kwargs to interpret as fields of Action Message. Must
            include "call" if description is None.

    Returns:
        a pro.Action Message.

    Raises:
        TypeError if you specified neither a description or a call.
    """
    if fields.get("id") is None:
        fields["id"] = random.randint(int(1e7), int(1e8))
    action = pro.Action(description=description, **fields)
    if (action.WhichOneof("call") is None) and (description is None):
        raise TypeError("must pass a description or command message.")
    return action


def make_function_call_action(
    func: str,
    module: Optional[str] = None,
    kwargs: Union[list[pro.PythonObject], Mapping[str, Any]] = MPt({}),
    context: Literal["thread", "process", "detached"] = "thread",
    **action_fields,
) -> pro.Action:
    """
    make a pro.Action Message specifying a Python function call.
    """
    if "name" not in action_fields:
        action_fields["name"] = func
    try:
        # if kwargs is already a list of PythonObjects, don't try to repack
        assert isinstance(kwargs[0], pro.PythonObject)
        objects = kwargs
    except (AssertionError, KeyError, TypeError):
        objects = default_arg_packing(kwargs)
    call = pro.FunctionCall(
        func=func, module=module, context=context, arguments=objects
    )
    return make_action(**action_fields, functioncall=call)


def update_instruction_timestamp(instruction: pro.Instruction):
    instruction.MergeFrom(pro.Instruction(time=make_timestamp()))


def make_instruction(instructiontype: str, **kwargs) -> pro.Instruction:
    """
    Standardized factory function for Instruction Messages. This is generally
    the most convenient and reliable way to create an Instruction for a
    Station to send to a Delegate. `Station` uses it by default to create
    'configure' and 'shutdown'-type Instructions, and it is an essential
    component of most `InstructionFromInfo.instruction_maker` functions.

    Automatically adds a timestamp and a random id to the Instruction.

    Args:
        instructiontype: type of instruction to make, typically 'do',
            'configure', or 'stop'.
        kwargs: Message fields and values to include in Instruction.

    Returns:
        a hostess Instruction protobuf Message.
    """
    if kwargs.get("id") is None:
        kwargs["id"] = random.randint(int(1e7), int(1e8))
    instruction = pro.Instruction(
        time=make_timestamp(), type=instructiontype, **kwargs
    )
    if instruction.type == "do" and instruction.task is None:
        raise ValueError("must assign a task for a 'do' action.")
    return instruction


def unpack_obj(obj: pro.PythonObject) -> Any:
    """
    Default deserialization function for pro.PythonObject Messages. Used
    extensively in internal Node behaviors and by stock Actors. Good for
    general-purpose object passing, although it may in some cases be more
    efficient to implement fancier deserialization optimized for a specific
    application's data models or formats.

    Args:
        obj: hostess PythonObject Message.

    Returns:
        object deserialized from `obj`.
    """
    if enum(obj, "compression") not in ("nocompression", None):
        # TODO: handle inline compression
        raise NotImplementedError
    if enum(obj, "serialization") == "json":
        value = json.loads(obj.value)
    elif enum(obj, "serialization") == "dill":
        value = dill.loads(obj.value)
    elif obj.dtype:
        value = np.frombuffer(
            obj.value, dtype=np.dtype(literal_eval(obj.dtype))
        )
    elif obj.scanf:
        unpacked = struct.unpack(obj.scanf, obj.value)
        if any(isinstance(v, bytes) for v in unpacked):
            chartype = enum(obj, "chartype")
            if chartype == "str":
                unpacked = tuple(map(lambda s: s.decode("utf-8"), unpacked))
            elif chartype == "nonetype":
                unpacked = [None for _ in unpacked]
        value = unpacked if len(unpacked) > 1 else unpacked[0]
    else:
        value = obj.value
    return value


def task_msg(actiondict: dict, steps=None) -> pro.TaskReport:
    """
    construct a hostess TaskReport Message from an actiondict (a `dict` of
    the format produced by `make_actiondict()` and expected by `Delegates` as
    values of their `actions` attribute). Delegates call this function to
    help construct Updates to a Station describing the results of a completed
    task (whether successful or failed).

    Args:
        actiondict: dict containing data from and metadata about a completed
            task (see `handlers.make_actiondict()` for format).
        steps: Placeholder for 'pipeline' behavior. Not currently implemented;
            must always be None.

    Returns:
        A hostess TaskReport that can be used as the "completion"
            field of a hostess Update.
    """
    if steps is not None:
        raise NotImplementedError
    fields = {}
    if "steps" in actiondict.keys():
        raise NotImplementedError
    if actiondict["status"] != "running":
        fields["result"] = pack_obj(actiondict.get("result"))
    fields["time"] = dict2msg(actiondict, pro.ActionTime)
    fields["id"] = actiondict["id"]
    action = dict2msg(actiondict, pro.ActionReport)
    action.MergeFrom(pro.ActionReport(**fields))
    return pro.TaskReport(
        instruction_id=actiondict["instruction_id"], action=action
    )


class Msg:
    """
    Helper class for hostess protobuf Messages. Allows hostess classes to
    (usually) abstract away protobuf-specific qualities of Messages. Also
    Improves efficiency of internal  Node operations by caching encode/decode
    operations.

    Although Msg is not _actually_ immutable, it should be treated as if it
    were immutable due to its aggressive caches. If the 'content' of a Msg
    needs to change, you should always construct a new one rather than
    modifying it inplace.
    """

    def __init__(self, message: Message):
        """
        Args:
            message: protobuf Message, preferably a hostess-specific protobuf
                Message.
        """
        self.message, self.sent = message, False
        self.size = self.message.ByteSize()

    @cached_property
    def comm(self):
        """self.message serialized into a hostess comm."""
        return make_comm(self.message)

    @cache
    def unpack(self, field=None):
        """"""
        if field is None:
            return unpack_message(self.message)
        try:
            assert isinstance(
                element := dig_for_values(self.message, field), Message
            )
            return unpack_message(element)
        except (AttributeError, AssertionError):
            raise AttributeError(f"{field} not found in message")

    @cached_property
    def body(self):
        return self.unpack()

    # TODO, maybe: too expensive?
    @cache
    def __getattr__(self, attr):
        try:
            try:
                out = self.unpack(attr)
            except AttributeError:
                out = dig_for_values(self.body, attr)[0]
            return out
        except TypeError:
            raise AttributeError(f"Msg has no attribute '{attr}'")

    @cache
    def pprint(self, field=None):
        if field is None:
            return format_message(self.body)
        return format_message(getattr(self, field))

    @cache
    def display(self, field=None):
        if field is None:
            return yprint(self.body, maxlen=256)
        return yprint(getattr(self, field), maxlen=256)

    @cache
    def __str__(self):
        try:
            return self.pprint()
        except NotImplementedError:
            return self.display()

    def __getitem__(self, key):
        return self.__getattr__(key)

    def __repr__(self):
        return self.__str__()

    sent = False


class Mailbox:
    """manager class for lists of messages"""

    # TODO: improve efficiency with caching or something

    def __init__(self, messages: Optional[MutableMapping[int, Msg]] = None):
        messages = {} if messages is None else messages
        if not isinstance(messages, MutableMapping):
            raise TypeError
        self.messages = messages

    def _sizer(self):
        return accumulate(
            map(attrgetter("size"), tuple(self.messages.values())), add
        )

    def prune(self, max_mb: float = 256):
        for i, size in enumerate(self._sizer()):
            if mb(size) > max_mb:
                self.messages = self.messages[:i]
                break

    @staticmethod
    def maybe_construct_msg(thing: Union[dict, Message]):
        # 'outbox' case
        if isinstance(thing, Message):
            return Msg(thing)
        # 'edited Msg' case
        elif isinstance(thing, Msg):
            return thing
        # 'inbox' case
        return Msg(thing["content"]["body"])

    def __getitem__(self, key):
        return self.messages[key]

    def __setitem__(self, key, value):
        self.messages[key] = self.maybe_construct_msg(value)

    def append(self, item):
        if len(self.messages) == 0:
            nextplace = 0
        else:
            nextplace = max(self.messages.keys()) + 1
        self.messages[nextplace] = self.maybe_construct_msg(item)

    def __len__(self):
        return len(self.messages)

    def __iter__(self):
        return iter(self.messages.values())

    # TODO: we should probably partly cache this or something
    def sort(self) -> dict:
        try:
            # noinspection PyTypeChecker
            return groupby(lambda m: m.reason, tuple(self.messages.values()))
        except AttributeError:
            raise TypeError("This method is only used for Station inboxes.")

    def _get_completed(self):
        return self.sort().get("completion", [])

    def _get_heartbeats(self):
        return self.sort().get("heartbeat", [])

    def _get_wilco(self):
        return self.sort().get("wilco", [])

    def _get_info(self):
        return self.sort().get("info", [])

    info = property(_get_info)
    completed = property(_get_completed)
    heartbeats = property(_get_heartbeats)
    wilco = property(_get_wilco)


def unpack_message(msg: Union[Message, RepeatedCompositeContainer]):
    if isinstance(msg, RepeatedCompositeContainer):
        formatted = []
        for i in msg:
            try:
                formatted.append(unpack_message(i))
            except AttributeError:
                formatted.append(i)
        return formatted
    formatted = {}
    for k, v in proto_formatdict(msg).items():
        element = getattr(msg, k)
        # noinspection PySimplifyBooleanCheck
        if element is None:
            continue
        elif v == "ENUM":
            formatted[k] = enum(msg, k)
        elif isinstance(element, pro.PythonObject):
            if element.name == "":
                formatted[k] = unpack_obj(element)
            else:
                formatted[k] = {
                    "value": unpack_obj(element),
                    "name": element.name,
                }
        # they look like lists, but they're not!
        elif hasattr(element, "__len__") and (len(element) == 0):
            continue
        elif isinstance(element, ScalarMapContainer):
            formatted[k] = dict(element)
        elif isinstance(element, RepeatedScalarContainer):
            # noinspection PyTypeChecker
            formatted[k] = list(element)
        elif isinstance(element, (Timestamp, Duration)):
            formatted[k] = element.ToJsonString()
        elif ("ListFields" in dir(element)) and (element.ListFields() == []):
            continue
        elif isinstance(element, Union[Message, RepeatedCompositeContainer]):
            formatted[k] = unpack_message(element)
        else:
            formatted[k] = element
    return formatted


def _print_update(unpacked, maxlen=256):
    topline = (
        f"{unpacked['nodeid']['name']} - " f"PID {unpacked['nodeid']['pid']}"
    )
    if (iid := unpacked.get("instruction_id")) not in (None, 0):
        topline += f" - iid {iid}"
    lines = [topline, f"{unpacked['reason']}: {unpacked['time']}"]
    for key in ("completed", "info"):
        if key in unpacked.keys():
            lines.append(key)
            lines.append(yprint(unpacked[key], indent=2, maxlen=maxlen))
    return lines


def _print_state(unpacked, maxlen=256):
    topline = f"status {unpacked['status']}"
    lines = [topline]
    for key in ("config", "threads"):
        if key in unpacked.keys():
            lines.append(key)
            lines.append(yprint(unpacked[key], indent=2, maxlen=maxlen))
    return lines


def format_message(unpacked, maxlen=256):
    """
    default string formatter for unpacked message.
    TODO: more sophisticated behavior.
    """
    if "nodeid" in unpacked.keys():
        lines = _print_update(unpacked, maxlen)
    elif "loc" in unpacked.keys():
        lines = _print_state(unpacked, maxlen)
    else:
        raise NotImplementedError
    return "\n".join(lines)


def dict2msg(
    mapping,
    proto_class,
    mtypes=(dict, MPt),
    proto_module=pro,
    pack_objects=True,
) -> Message:
    """
    construct a protobuf from a dict, filtering any keys that are not fields
    of `proto_class` and recursively diving into nested dicts.
    """
    fdict, fields = proto_formatdict(proto_class), {}
    for k, v in mapping.items():
        if k not in fdict.keys():
            continue
        if isinstance(v, mtypes):
            fields[k] = dict2msg(v, getattr(proto_module, k))
        elif isinstance(v, dt.datetime):
            fields[k] = make_timestamp(v)
        elif isinstance(v, dt.timedelta):
            fields[k] = make_duration(v)
        # special behavior for PythonObject
        elif (
            isinstance(fdict[k], dict)
            and fdict[k].get("value") == "BYTES"
            and not isinstance(v, bytes)
            and pack_objects is True
        ):
            fields[k] = pack_obj(v)
        else:
            fields[k] = v
    return proto_class(**fields)
