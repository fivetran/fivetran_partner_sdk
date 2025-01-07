# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: connector_sdk.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import common_pb2 as common__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x13\x63onnector_sdk.proto\x12\x0c\x66ivetran_sdk\x1a\x0c\x63ommon.proto\"\x8c\x01\n\rSchemaRequest\x12\x45\n\rconfiguration\x18\x01 \x03(\x0b\x32..fivetran_sdk.SchemaRequest.ConfigurationEntry\x1a\x34\n\x12\x43onfigurationEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xeb\x01\n\x0eSchemaResponse\x12\'\n\x1dschema_response_not_supported\x18\x01 \x01(\x08H\x00\x12/\n\x0bwith_schema\x18\x02 \x01(\x0b\x32\x18.fivetran_sdk.SchemaListH\x00\x12\x31\n\x0ewithout_schema\x18\x03 \x01(\x0b\x32\x17.fivetran_sdk.TableListH\x00\x12$\n\x17selection_not_supported\x18\x04 \x01(\x08H\x01\x88\x01\x01\x42\n\n\x08responseB\x1a\n\x18_selection_not_supported\"\xf3\x01\n\rUpdateRequest\x12\x45\n\rconfiguration\x18\x01 \x03(\x0b\x32..fivetran_sdk.UpdateRequest.ConfigurationEntry\x12/\n\tselection\x18\x02 \x01(\x0b\x32\x17.fivetran_sdk.SelectionH\x00\x88\x01\x01\x12\x17\n\nstate_json\x18\x03 \x01(\tH\x01\x88\x01\x01\x1a\x34\n\x12\x43onfigurationEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x0c\n\n_selectionB\r\n\x0b_state_json\"\x8b\x01\n\tSelection\x12:\n\x0ewithout_schema\x18\x01 \x01(\x0b\x32 .fivetran_sdk.TablesWithNoSchemaH\x00\x12\x35\n\x0bwith_schema\x18\x02 \x01(\x0b\x32\x1e.fivetran_sdk.TablesWithSchemaH\x00\x42\x0b\n\tselection\"^\n\x12TablesWithNoSchema\x12,\n\x06tables\x18\x01 \x03(\x0b\x32\x1c.fivetran_sdk.TableSelection\x12\x1a\n\x12include_new_tables\x18\x02 \x01(\x08\"_\n\x10TablesWithSchema\x12.\n\x07schemas\x18\x01 \x03(\x0b\x32\x1d.fivetran_sdk.SchemaSelection\x12\x1b\n\x13include_new_schemas\x18\x02 \x01(\x08\"\x82\x01\n\x0fSchemaSelection\x12\x10\n\x08included\x18\x01 \x01(\x08\x12\x13\n\x0bschema_name\x18\x02 \x01(\t\x12,\n\x06tables\x18\x03 \x03(\x0b\x32\x1c.fivetran_sdk.TableSelection\x12\x1a\n\x12include_new_tables\x18\x04 \x01(\x08\"\xbf\x01\n\x0eTableSelection\x12\x10\n\x08included\x18\x01 \x01(\x08\x12\x12\n\ntable_name\x18\x02 \x01(\t\x12:\n\x07\x63olumns\x18\x03 \x03(\x0b\x32).fivetran_sdk.TableSelection.ColumnsEntry\x12\x1b\n\x13include_new_columns\x18\x04 \x01(\x08\x1a.\n\x0c\x43olumnsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x08:\x02\x38\x01\"w\n\x0eUpdateResponse\x12+\n\tlog_entry\x18\x01 \x01(\x0b\x32\x16.fivetran_sdk.LogEntryH\x00\x12,\n\toperation\x18\x02 \x01(\x0b\x32\x17.fivetran_sdk.OperationH\x00\x42\n\n\x08response\"B\n\x08LogEntry\x12%\n\x05level\x18\x01 \x01(\x0e\x32\x16.fivetran_sdk.LogLevel\x12\x0f\n\x07message\x18\x02 \x01(\t\"\x9e\x01\n\tOperation\x12&\n\x06record\x18\x01 \x01(\x0b\x32\x14.fivetran_sdk.RecordH\x00\x12\x33\n\rschema_change\x18\x02 \x01(\x0b\x32\x1a.fivetran_sdk.SchemaChangeH\x00\x12.\n\ncheckpoint\x18\x03 \x01(\x0b\x32\x18.fivetran_sdk.CheckpointH\x00\x42\x04\n\x02op\"|\n\x0cSchemaChange\x12/\n\x0bwith_schema\x18\x01 \x01(\x0b\x32\x18.fivetran_sdk.SchemaListH\x00\x12\x31\n\x0ewithout_schema\x18\x02 \x01(\x0b\x32\x17.fivetran_sdk.TableListH\x00\x42\x08\n\x06\x63hange\"\xde\x01\n\x06Record\x12\x18\n\x0bschema_name\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x12\n\ntable_name\x18\x02 \x01(\t\x12\"\n\x04type\x18\x03 \x01(\x0e\x32\x14.fivetran_sdk.OpType\x12,\n\x04\x64\x61ta\x18\x04 \x03(\x0b\x32\x1e.fivetran_sdk.Record.DataEntry\x1a\x44\n\tDataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.fivetran_sdk.ValueType:\x02\x38\x01\x42\x0e\n\x0c_schema_name\" \n\nCheckpoint\x12\x12\n\nstate_json\x18\x01 \x01(\t*-\n\x08LogLevel\x12\x08\n\x04INFO\x10\x00\x12\x0b\n\x07WARNING\x10\x01\x12\n\n\x06SEVERE\x10\x02\x32\xc4\x02\n\tConnector\x12\x66\n\x11\x43onfigurationForm\x12&.fivetran_sdk.ConfigurationFormRequest\x1a\'.fivetran_sdk.ConfigurationFormResponse\"\x00\x12?\n\x04Test\x12\x19.fivetran_sdk.TestRequest\x1a\x1a.fivetran_sdk.TestResponse\"\x00\x12\x45\n\x06Schema\x12\x1b.fivetran_sdk.SchemaRequest\x1a\x1c.fivetran_sdk.SchemaResponse\"\x00\x12G\n\x06Update\x12\x1b.fivetran_sdk.UpdateRequest\x1a\x1c.fivetran_sdk.UpdateResponse\"\x00\x30\x01\x42\x1fH\x01P\x01Z\x19\x66ivetran.com/fivetran_sdkb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'connector_sdk_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'H\001P\001Z\031fivetran.com/fivetran_sdk'
  _globals['_SCHEMAREQUEST_CONFIGURATIONENTRY']._options = None
  _globals['_SCHEMAREQUEST_CONFIGURATIONENTRY']._serialized_options = b'8\001'
  _globals['_UPDATEREQUEST_CONFIGURATIONENTRY']._options = None
  _globals['_UPDATEREQUEST_CONFIGURATIONENTRY']._serialized_options = b'8\001'
  _globals['_TABLESELECTION_COLUMNSENTRY']._options = None
  _globals['_TABLESELECTION_COLUMNSENTRY']._serialized_options = b'8\001'
  _globals['_RECORD_DATAENTRY']._options = None
  _globals['_RECORD_DATAENTRY']._serialized_options = b'8\001'
  _globals['_LOGLEVEL']._serialized_start=2075
  _globals['_LOGLEVEL']._serialized_end=2120
  _globals['_SCHEMAREQUEST']._serialized_start=52
  _globals['_SCHEMAREQUEST']._serialized_end=192
  _globals['_SCHEMAREQUEST_CONFIGURATIONENTRY']._serialized_start=140
  _globals['_SCHEMAREQUEST_CONFIGURATIONENTRY']._serialized_end=192
  _globals['_SCHEMARESPONSE']._serialized_start=195
  _globals['_SCHEMARESPONSE']._serialized_end=430
  _globals['_UPDATEREQUEST']._serialized_start=433
  _globals['_UPDATEREQUEST']._serialized_end=676
  _globals['_UPDATEREQUEST_CONFIGURATIONENTRY']._serialized_start=140
  _globals['_UPDATEREQUEST_CONFIGURATIONENTRY']._serialized_end=192
  _globals['_SELECTION']._serialized_start=679
  _globals['_SELECTION']._serialized_end=818
  _globals['_TABLESWITHNOSCHEMA']._serialized_start=820
  _globals['_TABLESWITHNOSCHEMA']._serialized_end=914
  _globals['_TABLESWITHSCHEMA']._serialized_start=916
  _globals['_TABLESWITHSCHEMA']._serialized_end=1011
  _globals['_SCHEMASELECTION']._serialized_start=1014
  _globals['_SCHEMASELECTION']._serialized_end=1144
  _globals['_TABLESELECTION']._serialized_start=1147
  _globals['_TABLESELECTION']._serialized_end=1338
  _globals['_TABLESELECTION_COLUMNSENTRY']._serialized_start=1292
  _globals['_TABLESELECTION_COLUMNSENTRY']._serialized_end=1338
  _globals['_UPDATERESPONSE']._serialized_start=1340
  _globals['_UPDATERESPONSE']._serialized_end=1459
  _globals['_LOGENTRY']._serialized_start=1461
  _globals['_LOGENTRY']._serialized_end=1527
  _globals['_OPERATION']._serialized_start=1530
  _globals['_OPERATION']._serialized_end=1688
  _globals['_SCHEMACHANGE']._serialized_start=1690
  _globals['_SCHEMACHANGE']._serialized_end=1814
  _globals['_RECORD']._serialized_start=1817
  _globals['_RECORD']._serialized_end=2039
  _globals['_RECORD_DATAENTRY']._serialized_start=1955
  _globals['_RECORD_DATAENTRY']._serialized_end=2023
  _globals['_CHECKPOINT']._serialized_start=2041
  _globals['_CHECKPOINT']._serialized_end=2073
  _globals['_CONNECTOR']._serialized_start=2123
  _globals['_CONNECTOR']._serialized_end=2447
# @@protoc_insertion_point(module_scope)
