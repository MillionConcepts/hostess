import datetime as dt
import time
from types import MappingProxyType
from typing import Union

from google.protobuf.descriptor import FieldDescriptor
# noinspection PyUnresolvedReferences
from google.protobuf.duration_pb2 import Duration
# noinspection PyUnresolvedReferences
from google.protobuf.timestamp_pb2 import Timestamp

PROTO_TYPES = MappingProxyType(
    {
        getattr(FieldDescriptor, k): k.replace("TYPE_", "")
        for k in dir(FieldDescriptor) if k.startswith("TYPE")
    }
)


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


def timestamp2dt(proto):
    return dt.datetime.fromtimestamp(proto.seconds + proto.nanos)


def make_timestamp(moment=None):
    if moment is None:
        epoch = time.time()
    elif isinstance(moment, dt.datetime):
        epoch = moment.timestamp()
    elif isinstance(moment, str):
        epoch = dt.datetime.fromisoformat(moment).timestamp()
    elif isinstance(moment, float):
        epoch = moment
    else:
        raise TypeError
    seconds = round(epoch)
    nanos = round((epoch - seconds) * 1e9)
    return Timestamp(seconds=seconds, nanos=nanos)

