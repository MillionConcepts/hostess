"""simple, robust protocol for messaging and serialized data transfer."""
from __future__ import annotations

import struct
from types import MappingProxyType as MPt
from typing import Union

from google.protobuf.message import Message, DecodeError

from hostess.station.proto import station_pb2 as hostess_proto
from hostess.station.proto_utils import m2d

HOSTESS_ACK = b"\06hostess"
"""hostess acknowledgement code"""
HOSTESS_EOM = b"\03hostess"
"""hostess end-of-message code"""
HOSTESS_SOH = b"\01hostess"
"""hostess start-of-header code"""
CODE_TO_MTYPE = MPt(
    {0: "none", 1: "Update", 2: "Instruction", 3: "PythonObject"}
)
"""
one-byte-wide codes for Message type of comm body. "none" means the comm body 
is not a serialized protobuf Message.
"""
MTYPE_TO_CODE = MPt({v: k for k, v in CODE_TO_MTYPE.items()})
HEADER_STRUCT = struct.Struct("<8sBL")
"""struct specification for hostess comm header."""
WRAPPER_SIZE = HEADER_STRUCT.size + len(HOSTESS_EOM)


def make_comm(body: Union[bytes, Message]) -> bytes:
    """
    create a hostess comm from a buffer or a protobuf Message.

    Args:
        body: byte string or hostess Message to use as comm body

    Returns:
        hostess comm as `bytes`
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
    """
    read a hostess header from the first 13 bytes of `buffer`.

    Args:
        buffer: a `bytes` buffer containing a hostess comm

    Returns:
        dict with keys:
            "mtype": name of body's hostess Message type as given in header;
                "none" if the header says the body is not a serialized Message
            "length": body length as given in header
    """
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
    read a hostess comm from a byte string. if the comm's header says its body
    contains a hostess Message protobuf, attempt to decode it as a Message.

    Args:
        buffer: `bytes` object comprising a hostess comm.
        unpack_proto: if True and the comm contains a protobuf, unpack it
            into a dictionary rather than returning a 'raw' Message.

    Returns:
        a dict containing the decoded header, the (possibly decoded) body,
        and any errors.
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
