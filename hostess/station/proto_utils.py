"""
utilities for dealing with the protobuf format. Not intended for
hostess-specific messages -- these are more generic utilities.
"""

import datetime as dt
from types import MappingProxyType as MPt
from typing import Optional, Union

import google.protobuf.json_format
from google.protobuf.descriptor import FieldDescriptor, Descriptor

# noinspection PyUnresolvedReferences
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import Message

# noinspection PyUnresolvedReferences
from google.protobuf.timestamp_pb2 import Timestamp

PROTO_TYPES = MPt(
    {
        getattr(FieldDescriptor, k): k.replace("TYPE_", "")
        for k in dir(FieldDescriptor)
        if k.startswith("TYPE")
    }
)
"""mapping from protobuf type codes to types"""
m2d = google.protobuf.json_format.MessageToDict
"""alias for google.protobuf.json_format.MessageToDict"""


def proto_formatdict(
    proto: Union[Message, Descriptor]
) -> dict[str, Union[dict, str]]:
    """
    return a (possibly nested) dict showing the legal fields of a protobuf
    message or message type.

    Args:
        proto: protobuf Message or Descriptor whose format to describe

    Returns:
        dict whose keys are field names and whose values are protobuf data
            types or nested dicts (representing child Messages) of the same
            format.
    """
    # i.e., it's a descriptor
    if hasattr(proto, "fields_by_name"):
        descriptor = proto
    else:
        descriptor = proto.DESCRIPTOR
    unpacked = {}
    for name, field in descriptor.fields_by_name.items():
        if (ptype := PROTO_TYPES[field.type]) != "MESSAGE":
            unpacked[name] = ptype
        else:
            # TODO: get enumeration values
            unpacked[name] = proto_formatdict(field.message_type)
    return unpacked


def make_duration(delta: Union[dt.timedelta, float]) -> Duration:
    """
    create a Duration Message from a float or a dt.timedelta object.

    Args:
        delta: total duration -- if a float, always represents seconds.

    Returns:
         a protobuf Duration Message specifying the same timespan as `delta`
    """
    duration = Duration()
    if isinstance(delta, float):
        duration.FromSeconds(delta)
    else:
        duration.FromTimedelta(delta)
    return duration


def make_timestamp(datetime: Optional[dt.datetime] = None) -> Timestamp:
    """
    create a Timestamp Message from either the current time or a dt.datetime
    object.

    Args:
        datetime: if None, make a Timestamp from the current time. if a
            dt.datetime, make a Timestamp from it.

    Returns:
        protobuf Timestamp Message.
    """
    timestamp = Timestamp()
    if datetime is None:
        timestamp.GetCurrentTime()
    else:
        timestamp.FromDatetime(datetime)
    return timestamp


def enum(message: Message, field: str) -> Union[str, int]:
    """
    get the string or int value of an enum field in a protobuf Message. (If
    you directly access the field with the Python API, you will get the enum
    key instead of its value, which is generally less useful.)

    Args:
        message: protobuf Message containing an enum field
        field: name of enum field

    Returns:
        enum value of `field`; None if `field` is not present in message
    """
    for desc in message.DESCRIPTOR.fields:
        if desc.name != field:
            continue
        try:
            return desc.enum_type.values_by_number[
                getattr(message, field)
            ].name
        except AttributeError:
            raise TypeError(f"{field} is not an enum")
    raise KeyError(f"{field} is not a field of message")
