# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: worker.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cworker.proto\x12\x06worker\":\n\x06Worker\x12\n\n\x02id\x18\x01 \x01(\t\x12$\n\x06status\x18\x02 \x01(\x0e\x32\x14.worker.WorkerStatus*R\n\x0cWorkerStatus\x12\x08\n\x04IDLE\x10\x00\x12\x0f\n\x0bIN_PROGRESS\x10\x01\x12\r\n\tCOMPLETED\x10\x02\x12\x0c\n\x08\x46INISHED\x10\x03\x12\n\n\x06\x46\x41ILED\x10\x04\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'worker_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_WORKERSTATUS']._serialized_start=84
  _globals['_WORKERSTATUS']._serialized_end=166
  _globals['_WORKER']._serialized_start=24
  _globals['_WORKER']._serialized_end=82
# @@protoc_insertion_point(module_scope)
