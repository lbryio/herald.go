# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: hub.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import result_pb2 as result__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\thub.proto\x12\x02pb\x1a\x0cresult.proto\"\x0e\n\x0c\x45mptyMessage\".\n\rServerMessage\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\t\"N\n\x0cHelloMessage\x12\x0c\n\x04port\x18\x01 \x01(\t\x12\x0c\n\x04host\x18\x02 \x01(\t\x12\"\n\x07servers\x18\x03 \x03(\x0b\x32\x11.pb.ServerMessage\"0\n\x0fInvertibleField\x12\x0e\n\x06invert\x18\x01 \x01(\x08\x12\r\n\x05value\x18\x02 \x03(\t\"\x1c\n\x0bStringValue\x12\r\n\x05value\x18\x01 \x01(\t\"\x1c\n\x0bStringArray\x12\r\n\x05value\x18\x01 \x03(\t\"\x1a\n\tBoolValue\x12\r\n\x05value\x18\x01 \x01(\x08\"\x1c\n\x0bUInt32Value\x12\r\n\x05value\x18\x01 \x01(\r\"j\n\nRangeField\x12\x1d\n\x02op\x18\x01 \x01(\x0e\x32\x11.pb.RangeField.Op\x12\r\n\x05value\x18\x02 \x03(\x05\".\n\x02Op\x12\x06\n\x02\x45Q\x10\x00\x12\x07\n\x03LTE\x10\x01\x12\x07\n\x03GTE\x10\x02\x12\x06\n\x02LT\x10\x03\x12\x06\n\x02GT\x10\x04\"\x8e\x0c\n\rSearchRequest\x12%\n\x08\x63laim_id\x18\x01 \x01(\x0b\x32\x13.pb.InvertibleField\x12\'\n\nchannel_id\x18\x02 \x01(\x0b\x32\x13.pb.InvertibleField\x12\x0c\n\x04text\x18\x03 \x01(\t\x12\r\n\x05limit\x18\x04 \x01(\x05\x12\x10\n\x08order_by\x18\x05 \x03(\t\x12\x0e\n\x06offset\x18\x06 \x01(\r\x12\x16\n\x0eis_controlling\x18\x07 \x01(\x08\x12\x1d\n\x15last_take_over_height\x18\x08 \x01(\t\x12\x12\n\nclaim_name\x18\t \x01(\t\x12\x17\n\x0fnormalized_name\x18\n \x01(\t\x12#\n\x0btx_position\x18\x0b \x03(\x0b\x32\x0e.pb.RangeField\x12\x1e\n\x06\x61mount\x18\x0c \x03(\x0b\x32\x0e.pb.RangeField\x12!\n\ttimestamp\x18\r \x03(\x0b\x32\x0e.pb.RangeField\x12*\n\x12\x63reation_timestamp\x18\x0e \x03(\x0b\x32\x0e.pb.RangeField\x12\x1e\n\x06height\x18\x0f \x03(\x0b\x32\x0e.pb.RangeField\x12\'\n\x0f\x63reation_height\x18\x10 \x03(\x0b\x32\x0e.pb.RangeField\x12)\n\x11\x61\x63tivation_height\x18\x11 \x03(\x0b\x32\x0e.pb.RangeField\x12)\n\x11\x65xpiration_height\x18\x12 \x03(\x0b\x32\x0e.pb.RangeField\x12$\n\x0crelease_time\x18\x13 \x03(\x0b\x32\x0e.pb.RangeField\x12\x11\n\tshort_url\x18\x14 \x01(\t\x12\x15\n\rcanonical_url\x18\x15 \x01(\t\x12\r\n\x05title\x18\x16 \x01(\t\x12\x0e\n\x06\x61uthor\x18\x17 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x18 \x01(\t\x12\x12\n\nclaim_type\x18\x19 \x03(\t\x12$\n\x0crepost_count\x18\x1a \x03(\x0b\x32\x0e.pb.RangeField\x12\x13\n\x0bstream_type\x18\x1b \x03(\t\x12\x12\n\nmedia_type\x18\x1c \x03(\t\x12\"\n\nfee_amount\x18\x1d \x03(\x0b\x32\x0e.pb.RangeField\x12\x14\n\x0c\x66\x65\x65_currency\x18\x1e \x01(\t\x12 \n\x08\x64uration\x18\x1f \x03(\x0b\x32\x0e.pb.RangeField\x12\x19\n\x11reposted_claim_id\x18  \x01(\t\x12#\n\x0b\x63\x65nsor_type\x18! \x03(\x0b\x32\x0e.pb.RangeField\x12\x19\n\x11\x63laims_in_channel\x18\" \x01(\t\x12)\n\x12is_signature_valid\x18$ \x01(\x0b\x32\r.pb.BoolValue\x12(\n\x10\x65\x66\x66\x65\x63tive_amount\x18% \x03(\x0b\x32\x0e.pb.RangeField\x12&\n\x0esupport_amount\x18& \x03(\x0b\x32\x0e.pb.RangeField\x12&\n\x0etrending_score\x18\' \x03(\x0b\x32\x0e.pb.RangeField\x12\r\n\x05tx_id\x18+ \x01(\t\x12 \n\x07tx_nout\x18, \x01(\x0b\x32\x0f.pb.UInt32Value\x12\x11\n\tsignature\x18- \x01(\t\x12\x18\n\x10signature_digest\x18. \x01(\t\x12\x18\n\x10public_key_bytes\x18/ \x01(\t\x12\x15\n\rpublic_key_id\x18\x30 \x01(\t\x12\x10\n\x08\x61ny_tags\x18\x31 \x03(\t\x12\x10\n\x08\x61ll_tags\x18\x32 \x03(\t\x12\x10\n\x08not_tags\x18\x33 \x03(\t\x12\x1d\n\x15has_channel_signature\x18\x34 \x01(\x08\x12!\n\nhas_source\x18\x35 \x01(\x0b\x32\r.pb.BoolValue\x12 \n\x18limit_claims_per_channel\x18\x36 \x01(\x05\x12\x15\n\rany_languages\x18\x37 \x03(\t\x12\x15\n\rall_languages\x18\x38 \x03(\t\x12\x19\n\x11remove_duplicates\x18\x39 \x01(\x08\x12\x11\n\tno_totals\x18: \x01(\x08\x12\x0f\n\x07sd_hash\x18; \x01(\t2\x9b\x04\n\x03Hub\x12*\n\x06Search\x12\x11.pb.SearchRequest\x1a\x0b.pb.Outputs\"\x00\x12+\n\x04Ping\x12\x10.pb.EmptyMessage\x1a\x0f.pb.StringValue\"\x00\x12-\n\x05Hello\x12\x10.pb.HelloMessage\x1a\x10.pb.HelloMessage\"\x00\x12/\n\x07\x41\x64\x64Peer\x12\x11.pb.ServerMessage\x1a\x0f.pb.StringValue\"\x00\x12\x35\n\rPeerSubscribe\x12\x11.pb.ServerMessage\x1a\x0f.pb.StringValue\"\x00\x12.\n\x07Version\x12\x10.pb.EmptyMessage\x1a\x0f.pb.StringValue\"\x00\x12/\n\x08\x46\x65\x61tures\x12\x10.pb.EmptyMessage\x1a\x0f.pb.StringValue\"\x00\x12\x30\n\tBroadcast\x12\x10.pb.EmptyMessage\x1a\x0f.pb.UInt32Value\"\x00\x12-\n\x06Height\x12\x10.pb.EmptyMessage\x1a\x0f.pb.UInt32Value\"\x00\x12\x37\n\x0fHeightSubscribe\x12\x0f.pb.UInt32Value\x1a\x0f.pb.UInt32Value\"\x00\x30\x01\x12)\n\x07Resolve\x12\x0f.pb.StringArray\x1a\x0b.pb.Outputs\"\x00\x42)Z\'github.com/lbryio/herald/protobuf/go/pbb\x06proto3')



_EMPTYMESSAGE = DESCRIPTOR.message_types_by_name['EmptyMessage']
_SERVERMESSAGE = DESCRIPTOR.message_types_by_name['ServerMessage']
_HELLOMESSAGE = DESCRIPTOR.message_types_by_name['HelloMessage']
_INVERTIBLEFIELD = DESCRIPTOR.message_types_by_name['InvertibleField']
_STRINGVALUE = DESCRIPTOR.message_types_by_name['StringValue']
_STRINGARRAY = DESCRIPTOR.message_types_by_name['StringArray']
_BOOLVALUE = DESCRIPTOR.message_types_by_name['BoolValue']
_UINT32VALUE = DESCRIPTOR.message_types_by_name['UInt32Value']
_RANGEFIELD = DESCRIPTOR.message_types_by_name['RangeField']
_SEARCHREQUEST = DESCRIPTOR.message_types_by_name['SearchRequest']
_RANGEFIELD_OP = _RANGEFIELD.enum_types_by_name['Op']
EmptyMessage = _reflection.GeneratedProtocolMessageType('EmptyMessage', (_message.Message,), {
  'DESCRIPTOR' : _EMPTYMESSAGE,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.EmptyMessage)
  })
_sym_db.RegisterMessage(EmptyMessage)

ServerMessage = _reflection.GeneratedProtocolMessageType('ServerMessage', (_message.Message,), {
  'DESCRIPTOR' : _SERVERMESSAGE,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.ServerMessage)
  })
_sym_db.RegisterMessage(ServerMessage)

HelloMessage = _reflection.GeneratedProtocolMessageType('HelloMessage', (_message.Message,), {
  'DESCRIPTOR' : _HELLOMESSAGE,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.HelloMessage)
  })
_sym_db.RegisterMessage(HelloMessage)

InvertibleField = _reflection.GeneratedProtocolMessageType('InvertibleField', (_message.Message,), {
  'DESCRIPTOR' : _INVERTIBLEFIELD,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.InvertibleField)
  })
_sym_db.RegisterMessage(InvertibleField)

StringValue = _reflection.GeneratedProtocolMessageType('StringValue', (_message.Message,), {
  'DESCRIPTOR' : _STRINGVALUE,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.StringValue)
  })
_sym_db.RegisterMessage(StringValue)

StringArray = _reflection.GeneratedProtocolMessageType('StringArray', (_message.Message,), {
  'DESCRIPTOR' : _STRINGARRAY,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.StringArray)
  })
_sym_db.RegisterMessage(StringArray)

BoolValue = _reflection.GeneratedProtocolMessageType('BoolValue', (_message.Message,), {
  'DESCRIPTOR' : _BOOLVALUE,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.BoolValue)
  })
_sym_db.RegisterMessage(BoolValue)

UInt32Value = _reflection.GeneratedProtocolMessageType('UInt32Value', (_message.Message,), {
  'DESCRIPTOR' : _UINT32VALUE,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.UInt32Value)
  })
_sym_db.RegisterMessage(UInt32Value)

RangeField = _reflection.GeneratedProtocolMessageType('RangeField', (_message.Message,), {
  'DESCRIPTOR' : _RANGEFIELD,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.RangeField)
  })
_sym_db.RegisterMessage(RangeField)

SearchRequest = _reflection.GeneratedProtocolMessageType('SearchRequest', (_message.Message,), {
  'DESCRIPTOR' : _SEARCHREQUEST,
  '__module__' : 'hub_pb2'
  # @@protoc_insertion_point(class_scope:pb.SearchRequest)
  })
_sym_db.RegisterMessage(SearchRequest)

_HUB = DESCRIPTOR.services_by_name['Hub']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\'github.com/lbryio/herald/protobuf/go/pb'
  _EMPTYMESSAGE._serialized_start=31
  _EMPTYMESSAGE._serialized_end=45
  _SERVERMESSAGE._serialized_start=47
  _SERVERMESSAGE._serialized_end=93
  _HELLOMESSAGE._serialized_start=95
  _HELLOMESSAGE._serialized_end=173
  _INVERTIBLEFIELD._serialized_start=175
  _INVERTIBLEFIELD._serialized_end=223
  _STRINGVALUE._serialized_start=225
  _STRINGVALUE._serialized_end=253
  _STRINGARRAY._serialized_start=255
  _STRINGARRAY._serialized_end=283
  _BOOLVALUE._serialized_start=285
  _BOOLVALUE._serialized_end=311
  _UINT32VALUE._serialized_start=313
  _UINT32VALUE._serialized_end=341
  _RANGEFIELD._serialized_start=343
  _RANGEFIELD._serialized_end=449
  _RANGEFIELD_OP._serialized_start=403
  _RANGEFIELD_OP._serialized_end=449
  _SEARCHREQUEST._serialized_start=452
  _SEARCHREQUEST._serialized_end=2002
  _HUB._serialized_start=2005
  _HUB._serialized_end=2544
# @@protoc_insertion_point(module_scope)
