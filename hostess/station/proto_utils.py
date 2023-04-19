import datetime as dt
from types import MappingProxyType as MPt
from typing import Union, Optional

import hostess.station.proto.station_pb2 as pro

import google.protobuf.json_format
from google.protobuf.descriptor import FieldDescriptor
# noinspection PyUnresolvedReferences
from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import Message
# noinspection PyUnresolvedReferences
from google.protobuf.timestamp_pb2 import Timestamp

PROTO_TYPES = MPt(
    {
        getattr(FieldDescriptor, k): k.replace("TYPE_", "")
        for k in dir(FieldDescriptor) if k.startswith("TYPE")
    }
)
# just an alias
m2d = google.protobuf.json_format.MessageToDict


def proto_formatdict(proto) -> dict[str, Union[dict, str]]:
    """
    return a (possibly nested) dict showing the legal fields of a protobuf
    message or message type.
    """
    # i.e., it's a descriptor
    if 'fields_by_name' in dir(proto):
        descriptor = proto
    else:
        descriptor = proto.DESCRIPTOR
    unpacked = {}
    for name, field in descriptor.fields_by_name.items():
        if (ptype := PROTO_TYPES[field.type]) != 'MESSAGE':
            unpacked[name] = ptype
        else:
            # TODO: get enumeration values
            unpacked[name] = proto_formatdict(field.message_type)
    return unpacked


def dict2msg(
    mapping,
    proto_class,
    mtypes=(dict, MPt),
    proto_module = pro
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
        else:
            fields[k] = v
    return proto_class(**fields)


def make_duration(delta: Union[dt.timedelta, float]) -> Duration:
    duration = Duration()
    if isinstance(delta, float):
        duration.FromSeconds(delta)
    else:
        duration.FromTimedelta(delta)
    return duration


def make_timestamp(datetime: Optional[dt.datetime] = None):
    timestamp = Timestamp()
    if datetime is None:
        timestamp.GetCurrentTime()
    else:
        timestamp.FromDatetime(datetime)
    return timestamp


def enum(message, field):
    """get the string value of an enum field in a message."""
    for desc, val in message.ListFields():
        if desc.enum_type is None:
            continue
        if desc.name != field:
            continue
        return desc.enum_type.values_by_number[val].name
