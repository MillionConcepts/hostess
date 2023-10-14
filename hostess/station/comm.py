from __future__ import annotations

import struct
from types import MappingProxyType as MPt
from typing import Union

from google.protobuf.message import Message, DecodeError

from hostess.station.proto import station_pb2 as hostess_proto
from hostess.station.proto_utils import m2d

# acknowledgement, end-of-message, start-of-header codes
HOSTESS_ACK = b"\06hostess"
HOSTESS_EOM = b"\03hostess"
HOSTESS_SOH = b"\01hostess"
# one-byte-wide codes for Message type of comm body.
# "none" means the comm body is not a serialized protobuf Message.
CODE_TO_MTYPE = MPt(
    {0: "none", 1: "Update", 2: "Instruction", 3: "PythonObject"}
)
MTYPE_TO_CODE = MPt({v: k for k, v in CODE_TO_MTYPE.items()})
HEADER_STRUCT = struct.Struct("<8sBL")
WRAPPER_SIZE = HEADER_STRUCT.size + len(HOSTESS_EOM)


def make_comm(body: Union[bytes, Message]) -> bytes:
    """
    create a hostess comm from a buffer or a protobuf Message.
    automatically attach header and footer.
    """
    if hasattr(body, "SerializePartialToString"):
        # i.e., it's a protobuf Message
        buf, mtype = body.SerializePartialToString(), body.__class__.__name__
    else:
        buf, mtype = body, "none"
    return (
        HEADER_STRUCT.pack(
            HOSTESS_SOH, MTYPE_TO_CODE[mtype], len(buf) + WRAPPER_SIZE
        )
        + buf
        + HOSTESS_EOM
    )


def read_header(buffer: bytes) -> dict[str, Union[str, bool, int]]:
    """attempt to read a hostess header from the first 13 bytes of `buffer`."""
    try:
        unpacked = HEADER_STRUCT.unpack(buffer[:13])
        assert buffer[:8] == HOSTESS_SOH
        try:
            mtype = CODE_TO_MTYPE[unpacked[1]]
        except KeyError:
            mtype = "invalid message type"
        return {"mtype": mtype, "length": unpacked[2]}
    except (struct.error, AssertionError):
        raise IOError("invalid hostess header")


def read_comm(
    buffer: bytes, unpack_proto: bool = False
) -> dict[str, Union[dict, bytes, Message, str]]:
    """
    read a hostess comm. if the header says the body is a protobuf, attempt to
    decode it as a hostess.station Message. if unpack_proto is True, convert
    it to a dict. return a dict containing the decoded header, the
    (possibly decoded) body, and any errors.
    """
    try:
        header = read_header(buffer[: HEADER_STRUCT.size])
    except IOError:
        return {"header": None, "body": buffer, "err": "header"}
    err, body = [], buffer[HEADER_STRUCT.size :]
    if body.endswith(HOSTESS_EOM):
        body = body[: -len(HOSTESS_EOM)]
    if len(buffer) != header["length"]:
        err.append("length")
    if header["mtype"] == "none":
        return {"header": header, "body": body, "err": ";".join(err)}
    try:
        # the value of the 'mtype' key should correspond to a hostess.station
        # protocol buffer class
        message_class = getattr(hostess_proto, header["mtype"])
        message: Message = message_class.FromString(body)
    except AttributeError:
        err.append("mtype")
        return {"header": header, "body": body, "err": ";".join(err)}
    except DecodeError:
        err.append("protobuf decode")
        return {"header": header, "body": body, "err": ";".join(err)}
    if unpack_proto is True:
        message = m2d(message)
    return {"header": header, "body": message, "err": ";".join(err)}
