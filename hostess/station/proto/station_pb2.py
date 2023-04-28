# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: station/proto/station.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bstation/proto/station.proto\x12\x0fhostess_station\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"3\n\x0cPythonSerial\x12\x0c\n\x04\x62ody\x18\x01 \x01(\x0c\x12\x15\n\rserialization\x18\x02 \x01(\t\"\xdf\x01\n\x0cPythonObject\x12\r\n\x05value\x18\x01 \x01(\x0c\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x0f\n\x05scanf\x18\x03 \x01(\tH\x00\x12\x37\n\rserialization\x18\x04 \x01(\x0e\x32\x1e.hostess_station.SerializationH\x00\x12+\n\x08\x63hartype\x18\x05 \x01(\x0e\x32\x19.hostess_station.CharType\x12\x31\n\x0b\x63ompression\x18\x06 \x01(\x0e\x32\x1c.hostess_station.CompressionB\x08\n\x06\x66ormat\"\x8d\x01\n\nActionTime\x12)\n\x05start\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\'\n\x03\x65nd\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12+\n\x08\x64uration\x18\x03 \x01(\x0b\x32\x19.google.protobuf.Duration\"\x81\x02\n\x0c\x41\x63tionReport\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0b\n\x03pid\x18\x02 \x03(\r\x12\'\n\x06status\x18\x03 \x01(\x0e\x32\x17.hostess_station.Status\x12+\n\x05level\x18\x04 \x01(\x0e\x32\x1c.hostess_station.ActionLevel\x12)\n\x04time\x18\x05 \x01(\x0b\x32\x1b.hostess_station.ActionTime\x12-\n\x06result\x18\x06 \x01(\x0b\x32\x1d.hostess_station.PythonObject\x12\x13\n\x0breturn_type\x18\x07 \x01(\t\x12\x11\n\texit_code\x18\x08 \x01(\r\"\x81\x01\n\nTaskReport\x12\x16\n\x0einstruction_id\x18\x01 \x01(\r\x12,\n\x05steps\x18\x02 \x03(\x0b\x32\x1d.hostess_station.ActionReport\x12-\n\x06\x61\x63tion\x18\x03 \x01(\x0b\x32\x1d.hostess_station.ActionReport\"\xb3\x03\n\tNodeState\x12+\n\x06status\x18\x01 \x01(\x0e\x32\x1b.hostess_station.NodeStatus\x12%\n\x03loc\x18\x02 \x01(\x0e\x32\x18.hostess_station.NodeLoC\x12\x13\n\x0b\x63\x61n_receive\x18\x03 \x01(\x08\x12\x0c\n\x04\x62usy\x18\x04 \x01(\x08\x12\x38\n\x07threads\x18\x05 \x03(\x0b\x32\'.hostess_station.NodeState.ThreadsEntry\x12-\n\x06\x63onfig\x18\x06 \x01(\x0b\x32\x1d.hostess_station.PythonObject\x12\x42\n\x0cmessagecount\x18\x07 \x03(\x0b\x32,.hostess_station.NodeState.MessagecountEntry\x1a.\n\x0cThreadsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1aR\n\x11MessagecountEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.hostess_station.MessageCount:\x02\x38\x01\"?\n\x06NodeId\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0b\n\x03pid\x18\x03 \x01(\r\x12\x0c\n\x04host\x18\x04 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\t\"<\n\x0cMessageCount\x12\x16\n\x0etotal_messages\x18\x01 \x01(\r\x12\x14\n\x0cnew_messages\x18\x02 \x01(\r\"\xda\x02\n\x06Update\x12\'\n\x06nodeid\x18\x01 \x01(\x0b\x32\x17.hostess_station.NodeId\x12)\n\x05state\x18\x02 \x01(\x0b\x32\x1a.hostess_station.NodeState\x12(\n\x04time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12-\n\x06reason\x18\x04 \x01(\x0e\x32\x1d.hostess_station.UpdateReason\x12.\n\x07running\x18\x05 \x03(\x0b\x32\x1d.hostess_station.ActionReport\x12.\n\tcompleted\x18\x06 \x01(\x0b\x32\x1b.hostess_station.TaskReport\x12+\n\x04info\x18\x07 \x03(\x0b\x32\x1d.hostess_station.PythonObject\x12\x16\n\x0einstruction_id\x18\x08 \x01(\r\"\x8e\x01\n\x14PythonEncodedPayload\x12\x0c\n\x04\x62ody\x18\x01 \x01(\x0c\x12\x35\n\rserialization\x18\x02 \x01(\x0e\x32\x1e.hostess_station.Serialization\x12\x31\n\x0b\x63ompression\x18\x03 \x01(\x0e\x32\x1c.hostess_station.Compression\"\x8d\x01\n\x0c\x46unctionCall\x12\x0e\n\x06module\x18\x01 \x01(\t\x12\x0c\n\x04\x66unc\x18\x02 \x01(\t\x12\x30\n\targuments\x18\x03 \x03(\x0b\x32\x1d.hostess_station.PythonObject\x12-\n\x07\x63ontext\x18\x04 \x01(\x0e\x32\x1c.hostess_station.ExecContext\"\xd1\x01\n\nScriptCall\x12\x0e\n\x06module\x18\x01 \x01(\t\x12\x0c\n\x04\x66unc\x18\x02 \x01(\t\x12\x36\n\x07payload\x18\x03 \x01(\x0b\x32%.hostess_station.PythonEncodedPayload\x12\x1a\n\x10interpreter_path\x18\x04 \x01(\tH\x00\x12\x13\n\tconda_env\x18\x05 \x01(\tH\x00\x12-\n\x07\x63ontext\x18\x06 \x01(\x0e\x32\x1c.hostess_station.ExecContextB\r\n\x0binterpreter\"\xa5\x01\n\nSystemCall\x12\x0f\n\x07\x63ommand\x18\x01 \x01(\t\x12\x0f\n\x07payload\x18\x02 \x01(\x0c\x12\x31\n\x0b\x63ompression\x18\x03 \x01(\x0e\x32\x1c.hostess_station.Compression\x12\x13\n\x0binterpreter\x18\x04 \x01(\t\x12-\n\x07\x63ontext\x18\x05 \x01(\x0e\x32\x1c.hostess_station.ExecContext\"\xcd\x01\n\x08\x43odeBlob\x12\x0c\n\x04\x63ode\x18\x01 \x01(\x0c\x12\x11\n\tis_source\x18\x02 \x01(\x08\x12\x31\n\x0b\x63ompression\x18\x03 \x01(\x0e\x32\x1c.hostess_station.Compression\x12\x1a\n\x10interpreter_path\x18\x04 \x01(\tH\x00\x12\x13\n\tconda_env\x18\x05 \x01(\tH\x00\x12-\n\x07\x63ontext\x18\x06 \x01(\x0e\x32\x1c.hostess_station.ExecContextB\r\n\x0binterpreter\"\xfc\x02\n\x06\x41\x63tion\x12\n\n\x02id\x18\x01 \x01(\r\x12\x0c\n\x04name\x18\x02 \x01(\t\x12=\n\x0b\x64\x65scription\x18\x03 \x03(\x0b\x32(.hostess_station.Action.DescriptionEntry\x12\x31\n\nsystemcall\x18\x04 \x01(\x0b\x32\x1b.hostess_station.SystemCallH\x00\x12\x35\n\x0c\x66unctioncall\x18\x05 \x01(\x0b\x32\x1d.hostess_station.FunctionCallH\x00\x12\x31\n\nscriptcall\x18\x06 \x01(\x0b\x32\x1b.hostess_station.ScriptCallH\x00\x12/\n\nscriptblob\x18\x07 \x01(\x0b\x32\x19.hostess_station.CodeBlobH\x00\x12\x0c\n\x04wait\x18\x08 \x01(\x08\x1a\x32\n\x10\x44\x65scriptionEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\t\n\x07\x63ommand\"L\n\x08Pipeline\x12\n\n\x02id\x18\x01 \x01(\r\x12\x0c\n\x04name\x18\x02 \x01(\t\x12&\n\x05steps\x18\x03 \x03(\x0b\x32\x17.hostess_station.Action\"p\n\x0b\x43onfigParam\x12\x33\n\tparamtype\x18\x01 \x01(\x0e\x32 .hostess_station.ConfigParamType\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.hostess_station.PythonObject\"\xee\x02\n\x0bInstruction\x12.\n\x04type\x18\x01 \x01(\x0e\x32 .hostess_station.InstructionType\x12\n\n\x02id\x18\x02 \x01(\r\x12(\n\x04time\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12)\n\x06\x61\x63tion\x18\x04 \x01(\x0b\x32\x17.hostess_station.ActionH\x00\x12)\n\x04pipe\x18\x05 \x01(\x0b\x32\x19.hostess_station.PipelineH\x00\x12<\n\x08metadata\x18\x06 \x03(\x0b\x32*.hostess_station.Instruction.MetadataEntry\x12,\n\x06\x63onfig\x18\x07 \x03(\x0b\x32\x1c.hostess_station.ConfigParam\x1a/\n\rMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x06\n\x04task*O\n\rSerialization\x12\x18\n\x14unknownserialization\x10\x00\x12\x10\n\x0cunserialized\x10\x01\x12\x08\n\x04json\x10\x02\x12\x08\n\x04\x64ill\x10\x03*2\n\x0b\x43ompression\x12\x10\n\x0cuncompressed\x10\x00\x12\x08\n\x04gzip\x10\x01\x12\x07\n\x03lz4\x10\x02*C\n\x08\x43harType\x12\x15\n\x11unknownstringtype\x10\x00\x12\x07\n\x03str\x10\x01\x12\t\n\x05\x62ytes\x10\x02\x12\x0c\n\x08nonetype\x10\x03*;\n\x0b\x41\x63tionLevel\x12\x16\n\x12unknownactionlevel\x10\x00\x12\x08\n\x04pipe\x10\x01\x12\n\n\x06\x61\x63tion\x10\x02*Z\n\x06Status\x12\x11\n\runknownstatus\x10\x00\x12\x0b\n\x07running\x10\x01\x12\x0b\n\x07success\x10\x02\x12\x0b\n\x07\x66\x61ilure\x10\x03\x12\t\n\x05\x63rash\x10\x04\x12\x0b\n\x07timeout\x10\x05*`\n\nNodeStatus\x12\x12\n\x0eunknown_status\x10\x00\x12\x08\n\x04idle\x10\x01\x12\x0b\n\x07nominal\x10\x02\x12\x0c\n\x08\x64\x65graded\x10\x03\x12\x0c\n\x08shutdown\x10\x04\x12\x0b\n\x07\x63rashed\x10\x05*D\n\x07NodeLoC\x12\x0f\n\x0bunknown_loc\x10\x00\x12\x0b\n\x07primary\x10\x01\x12\r\n\tsecondary\x10\x02\x12\x0c\n\x08sleeping\x10\x03*\x83\x01\n\x0cUpdateReason\x12\r\n\tno_reason\x10\x00\x12\r\n\tscheduled\x10\x01\x12\x08\n\x04info\x10\x02\x12\r\n\trequested\x10\x03\x12\t\n\x05wilco\x10\x04\x12\x0f\n\x0b\x62\x61\x64_request\x10\x05\x12\x0b\n\x07\x65xiting\x10\x06\x12\x13\n\x0ftask_completion\x10\x07*n\n\x0fInstructionType\x12\x0f\n\x0bunknowninst\x10\x00\x12\n\n\x06report\x10\x01\x12\x08\n\x04stop\x10\x02\x12\x08\n\x04wake\x10\x03\x12\t\n\x05sleep\x10\x04\x12\x06\n\x02\x64o\x10\x05\x12\x08\n\x04kill\x10\x06\x12\r\n\tconfigure\x10\x07*H\n\x0b\x45xecContext\x12\x12\n\x0eunknowncontext\x10\x00\x12\n\n\x06thread\x10\x01\x12\x0b\n\x07process\x10\x02\x12\x0c\n\x08\x64\x65tached\x10\x03*H\n\x0f\x43onfigParamType\x12\x0f\n\x0bunknowntype\x10\x00\x12\x13\n\x0f\x63onfig_property\x10\x01\x12\x0f\n\x0b\x63onfig_dict\x10\x02\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'station.proto.station_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _NODESTATE_THREADSENTRY._options = None
  _NODESTATE_THREADSENTRY._serialized_options = b'8\001'
  _NODESTATE_MESSAGECOUNTENTRY._options = None
  _NODESTATE_MESSAGECOUNTENTRY._serialized_options = b'8\001'
  _ACTION_DESCRIPTIONENTRY._options = None
  _ACTION_DESCRIPTIONENTRY._serialized_options = b'8\001'
  _INSTRUCTION_METADATAENTRY._options = None
  _INSTRUCTION_METADATAENTRY._serialized_options = b'8\001'
  _SERIALIZATION._serialized_start=3663
  _SERIALIZATION._serialized_end=3742
  _COMPRESSION._serialized_start=3744
  _COMPRESSION._serialized_end=3794
  _CHARTYPE._serialized_start=3796
  _CHARTYPE._serialized_end=3863
  _ACTIONLEVEL._serialized_start=3865
  _ACTIONLEVEL._serialized_end=3924
  _STATUS._serialized_start=3926
  _STATUS._serialized_end=4016
  _NODESTATUS._serialized_start=4018
  _NODESTATUS._serialized_end=4114
  _NODELOC._serialized_start=4116
  _NODELOC._serialized_end=4184
  _UPDATEREASON._serialized_start=4187
  _UPDATEREASON._serialized_end=4318
  _INSTRUCTIONTYPE._serialized_start=4320
  _INSTRUCTIONTYPE._serialized_end=4430
  _EXECCONTEXT._serialized_start=4432
  _EXECCONTEXT._serialized_end=4504
  _CONFIGPARAMTYPE._serialized_start=4506
  _CONFIGPARAMTYPE._serialized_end=4578
  _PYTHONSERIAL._serialized_start=113
  _PYTHONSERIAL._serialized_end=164
  _PYTHONOBJECT._serialized_start=167
  _PYTHONOBJECT._serialized_end=390
  _ACTIONTIME._serialized_start=393
  _ACTIONTIME._serialized_end=534
  _ACTIONREPORT._serialized_start=537
  _ACTIONREPORT._serialized_end=794
  _TASKREPORT._serialized_start=797
  _TASKREPORT._serialized_end=926
  _NODESTATE._serialized_start=929
  _NODESTATE._serialized_end=1364
  _NODESTATE_THREADSENTRY._serialized_start=1234
  _NODESTATE_THREADSENTRY._serialized_end=1280
  _NODESTATE_MESSAGECOUNTENTRY._serialized_start=1282
  _NODESTATE_MESSAGECOUNTENTRY._serialized_end=1364
  _NODEID._serialized_start=1366
  _NODEID._serialized_end=1429
  _MESSAGECOUNT._serialized_start=1431
  _MESSAGECOUNT._serialized_end=1491
  _UPDATE._serialized_start=1494
  _UPDATE._serialized_end=1840
  _PYTHONENCODEDPAYLOAD._serialized_start=1843
  _PYTHONENCODEDPAYLOAD._serialized_end=1985
  _FUNCTIONCALL._serialized_start=1988
  _FUNCTIONCALL._serialized_end=2129
  _SCRIPTCALL._serialized_start=2132
  _SCRIPTCALL._serialized_end=2341
  _SYSTEMCALL._serialized_start=2344
  _SYSTEMCALL._serialized_end=2509
  _CODEBLOB._serialized_start=2512
  _CODEBLOB._serialized_end=2717
  _ACTION._serialized_start=2720
  _ACTION._serialized_end=3100
  _ACTION_DESCRIPTIONENTRY._serialized_start=3039
  _ACTION_DESCRIPTIONENTRY._serialized_end=3089
  _PIPELINE._serialized_start=3102
  _PIPELINE._serialized_end=3178
  _CONFIGPARAM._serialized_start=3180
  _CONFIGPARAM._serialized_end=3292
  _INSTRUCTION._serialized_start=3295
  _INSTRUCTION._serialized_end=3661
  _INSTRUCTION_METADATAENTRY._serialized_start=3606
  _INSTRUCTION_METADATAENTRY._serialized_end=3653
# @@protoc_insertion_point(module_scope)
