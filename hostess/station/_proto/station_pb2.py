# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: station/_proto/station.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1cstation/_proto/station.proto\x12\x0fhostess_station\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\x8d\x01\n\nActionTime\x12)\n\x05start\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\'\n\x03\x65nd\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12+\n\x08\x64uration\x18\x03 \x01(\x0b\x32\x19.google.protobuf.Duration\"\xd5\x01\n\x06\x41\x63tion\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\'\n\x06status\x18\x02 \x01(\x0e\x32\x17.hostess_station.Status\x12+\n\x05level\x18\x03 \x01(\x0e\x32\x1c.hostess_station.ActionLevel\x12)\n\x04time\x18\x04 \x01(\x0b\x32\x1b.hostess_station.ActionTime\x12\x14\n\x0creturn_value\x18\x05 \x01(\t\x12\x13\n\x0breturn_type\x18\x06 \x01(\t\x12\x11\n\texit_code\x18\x07 \x01(\r\"\xa5\x01\n\nTaskReport\x12\x0f\n\x07task_id\x18\x01 \x01(\r\x12\x10\n\x08nodename\x18\x02 \x01(\t\x12\x10\n\x08hostname\x18\x03 \x01(\t\x12,\n\x08sendtime\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12(\n\x07\x61\x63tions\x18\x05 \x03(\x0b\x32\x17.hostess_station.Action\x12\n\n\x02ok\x18\x06 \x01(\x08\"\xf4\x01\n\x06Update\x12\x10\n\x08nodename\x18\x01 \x01(\t\x12\n\n\x02ok\x18\x02 \x01(\x08\x12(\n\x04time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\twait_time\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x10\n\x08hostname\x18\x05 \x01(\t\x12\x34\n\x0frunning_actions\x18\x06 \x01(\x0b\x32\x1b.hostess_station.TaskReport\x12\x16\n\x0etotal_messages\x18\x07 \x01(\r\x12\x14\n\x0cnew_messages\x18\x08 \x01(\r\"3\n\x0cPythonSerial\x12\x0c\n\x04\x62ody\x18\x01 \x01(\x0c\x12\x15\n\rserialization\x18\x02 \x01(\t\"\xa8\x01\n\x0ePythonArgument\x12\r\n\x05value\x18\x01 \x01(\x0c\x12\x35\n\rserialization\x18\x02 \x01(\x0e\x32\x1e.hostess_station.Serialization\x12\x0f\n\x07\x61rgname\x18\x03 \x01(\t\x12\x0c\n\x04type\x18\x04 \x01(\t\x12\x31\n\x0b\x63ompression\x18\x05 \x01(\x0e\x32\x1c.hostess_station.Compression\"\xcf\x01\n\nPythonCall\x12\x31\n\x08\x63\x61lltype\x18\x01 \x01(\x0e\x32\x1f.hostess_station.PythonCallType\x12\x0c\n\x04path\x18\x02 \x01(\t\x12\x0c\n\x04\x66unc\x18\x03 \x01(\t\x12\x32\n\targuments\x18\x04 \x03(\x0b\x32\x1f.hostess_station.PythonArgument\x12\x1a\n\x10interpreter_path\x18\x05 \x01(\tH\x00\x12\x13\n\tconda_env\x18\x06 \x01(\tH\x00\x42\r\n\x0binterpreter\"\xc8\x02\n\x0bInstruction\x12.\n\x04type\x18\x01 \x01(\x0e\x32 .hostess_station.InstructionType\x12\x0f\n\x07task_id\x18\x02 \x01(\r\x12:\n\x07message\x18\x03 \x03(\x0b\x32).hostess_station.Instruction.MessageEntry\x12\x0e\n\x04\x62lob\x18\x04 \x01(\x0cH\x00\x12\x14\n\nsystemcall\x18\x05 \x01(\tH\x00\x12\x31\n\npythoncall\x18\x06 \x01(\x0b\x32\x1b.hostess_station.PythonCallH\x00\x12(\n\x04time\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x1a.\n\x0cMessageEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\t\n\x07payload*!\n\x0b\x41\x63tionLevel\x12\x08\n\x04node\x10\x00\x12\x08\n\x04pipe\x10\x01*G\n\x06Status\x12\x0b\n\x07running\x10\x00\x12\x0b\n\x07success\x10\x01\x12\x0b\n\x07\x66\x61ilure\x10\x02\x12\t\n\x05\x63rash\x10\x03\x12\x0b\n\x07timeout\x10\x04*N\n\x0fInstructionType\x12\n\n\x06report\x10\x00\x12\x08\n\x04stop\x10\x01\x12\x08\n\x04wake\x10\x02\x12\t\n\x05sleep\x10\x03\x12\x06\n\x02\x64o\x10\x04\x12\x08\n\x04kill\x10\x05*M\n\rSerialization\x12\x10\n\x0cunserialized\x10\x00\x12\x13\n\x0fjson_serialized\x10\x01\x12\x15\n\x11pickle_serialized\x10\x02*!\n\x0b\x43ompression\x12\x08\n\x04none\x10\x00\x12\x08\n\x04gzip\x10\x01*(\n\x0ePythonCallType\x12\n\n\x06script\x10\x00\x12\n\n\x06module\x10\x01\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'station._proto.station_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _INSTRUCTION_MESSAGEENTRY._options = None
  _INSTRUCTION_MESSAGEENTRY._serialized_options = b'8\001'
  _ACTIONLEVEL._serialized_start=1654
  _ACTIONLEVEL._serialized_end=1687
  _STATUS._serialized_start=1689
  _STATUS._serialized_end=1760
  _INSTRUCTIONTYPE._serialized_start=1762
  _INSTRUCTIONTYPE._serialized_end=1840
  _SERIALIZATION._serialized_start=1842
  _SERIALIZATION._serialized_end=1919
  _COMPRESSION._serialized_start=1921
  _COMPRESSION._serialized_end=1954
  _PYTHONCALLTYPE._serialized_start=1956
  _PYTHONCALLTYPE._serialized_end=1996
  _ACTIONTIME._serialized_start=115
  _ACTIONTIME._serialized_end=256
  _ACTION._serialized_start=259
  _ACTION._serialized_end=472
  _TASKREPORT._serialized_start=475
  _TASKREPORT._serialized_end=640
  _UPDATE._serialized_start=643
  _UPDATE._serialized_end=887
  _PYTHONSERIAL._serialized_start=889
  _PYTHONSERIAL._serialized_end=940
  _PYTHONARGUMENT._serialized_start=943
  _PYTHONARGUMENT._serialized_end=1111
  _PYTHONCALL._serialized_start=1114
  _PYTHONCALL._serialized_end=1321
  _INSTRUCTION._serialized_start=1324
  _INSTRUCTION._serialized_end=1652
  _INSTRUCTION_MESSAGEENTRY._serialized_start=1595
  _INSTRUCTION_MESSAGEENTRY._serialized_end=1641
# @@protoc_insertion_point(module_scope)
