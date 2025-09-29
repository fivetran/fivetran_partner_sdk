from concurrent import futures
import grpc
import read_csv
import sys
import argparse
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_pb2
from sdk_pb2 import common_pb2
from sdk_pb2 import destination_sdk_pb2_grpc


INFO = "INFO"
WARNING = "WARNING"
SEVERE = "SEVERE"

class DestinationImpl(destination_sdk_pb2_grpc.DestinationConnectorServicer):
    table_map = {}

    def ConfigurationForm(self, request, context):
        log_message(INFO, "Fetching Configuration form")

        # Create the form fields
        form_fields = common_pb2.ConfigurationFormResponse(
            schema_selection_supported=True,
            table_selection_supported=True
        )

        # writerType field with dropdown
        writer_type = common_pb2.FormField(
            name="writerType",
            label="Writer Type",
            description="Choose the destination type",
            dropdown_field=common_pb2.DropdownField(dropdown_field=["Database", "File", "Cloud"]),
            default_value="Database"
        )

        # host field
        host = common_pb2.FormField(
            name="host",
            label="Host",
            text_field=common_pb2.TextField.PlainText,
            placeholder="your_host_details"
        )

        # port field
        port = common_pb2.FormField(
            name="port",
            label="Port",
            text_field=common_pb2.TextField.PlainText,
            placeholder="your_port_details"
        )

        # user field
        user = common_pb2.FormField(
            name="user",
            label="User",
            text_field=common_pb2.TextField.PlainText,
            placeholder="user_name"
        )

        # password field
        password = common_pb2.FormField(
            name="password",
            label="Password",
            text_field=common_pb2.TextField.Password,
            placeholder="your_password"
        )

        # database field
        database = common_pb2.FormField(
            name="database",
            label="Database",
            text_field=common_pb2.TextField.PlainText,
            placeholder="your_database_name"
        )

        # table field
        table = common_pb2.FormField(
            name="table",
            label="Table",
            text_field=common_pb2.TextField.PlainText,
            placeholder="your_table_name"
        )

        # filePath field
        file_path = common_pb2.FormField(
            name="filePath",
            label="File Path",
            text_field=common_pb2.TextField.PlainText,
            placeholder="your_file_path"
        )

        # region field with dropdown
        region = common_pb2.FormField(
            name="region",
            label="Cloud Region",
            description="Choose the cloud region",
            dropdown_field=common_pb2.DropdownField(dropdown_field=["Azure", "AWS", "Google Cloud"]),
            default_value="Azure"
        )

        # enableEncryption toggle field
        enable_encryption = common_pb2.FormField(
            name="enableEncryption",
            label="Enable Encryption?",
            description="To enable/disable encryption for data transfer",
            toggle_field=common_pb2.ToggleField()
        )

        # Define Visibility Conditions for Conditional Fields
        visibility_condition_for_cloud = common_pb2.VisibilityCondition(
            condition_field="writerType",
            string_value="Cloud"
        )

        visibility_condition_for_database = common_pb2.VisibilityCondition(
            condition_field="writerType",
            string_value="Database"
        )

        visibility_condition_for_file = common_pb2.VisibilityCondition(
            condition_field="writerType",
            string_value="File"
        )

        # List of conditional fields
        # Note: The 'name' and 'label' parameters in the FormField for conditional fields are not used.

        # Create conditional fields for Cloud
        conditional_fields_for_cloud = common_pb2.ConditionalFields(
            condition=visibility_condition_for_cloud,
            fields=[host, port, user, password, region]
        )

        # Create conditional fields for File
        conditional_fields_for_file = common_pb2.ConditionalFields(
            condition=visibility_condition_for_file,
            fields=[host, port, user, password, table, file_path]
        )

        # Create conditional fields for Database
        conditional_fields_for_database = common_pb2.ConditionalFields(
            condition=visibility_condition_for_database,
            fields=[host, port, user, password, database, table]
        )

        # Add conditional fields to the form
        conditional_field_for_cloud = common_pb2.FormField(
            name="conditional_field_for_cloud",
            label="Conditional field for cloud",
            conditional_fields=conditional_fields_for_cloud
        )

        conditional_field_for_file = common_pb2.FormField(
            name="conditional_field_for_file",
            label="Conditional field for File",
            conditional_fields=conditional_fields_for_file
        )

        conditional_field_for_database = common_pb2.FormField(
            name="conditional_field_for_database",
            label="Conditional field for Database",
            conditional_fields=conditional_fields_for_database
        )

        # Add all fields to the form response
        form_fields.fields.extend([
            writer_type,
            conditional_field_for_file,
            conditional_field_for_cloud,
            conditional_field_for_database,
            enable_encryption
        ])

        # Add tests to the form
        form_fields.tests.add(
            name="connect",
            label="Tests connection"
        )

        form_fields.tests.add(
            name="select",
            label="Tests selection"
        )

        return form_fields

    def Test(self, request, context):
        test_name = request.name
        log_message(INFO, "test name: " + test_name)
        return common_pb2.TestResponse(success=True)

    def CreateTable(self, request, context):
        print("[CreateTable] :" + str(request.schema_name) + " | " + str(request.table.name) + " | " + str(request.table.columns))
        DestinationImpl.table_map[request.table.name] = request.table
        return destination_sdk_pb2.CreateTableResponse(success=True)

    def AlterTable(self, request, context):
        res: destination_sdk_pb2.AlterTableResponse

        print("[AlterTable]: " + str(request.schema_name) + " | " + str(request.table.name) + " | " + str(request.table.columns))
        DestinationImpl.table_map[request.table.name] = request.table
        return destination_sdk_pb2.AlterTableResponse(success=True)

    def Truncate(self, request, context):
        print("[TruncateTable]: " + str(request.schema_name) + " | " + str(request.schema_name) + " | soft" + str(request.soft))
        return destination_sdk_pb2.TruncateResponse(success=True)

    def WriteBatch(self, request, context):
        for replace_file in request.replace_files:
            print("replace files: " + str(replace_file))
        for update_file in request.update_files:
            print("replace files: " + str(update_file))
        for delete_file in request.delete_files:
            print("delete files: " + str(delete_file))

        log_message(WARNING, "Data loading started for table " + request.table.name)
        for key, value in request.keys.items():
            print("----------------------------------------------------------------------------")
            print("Decrypting and printing file :" + str(key))
            print("----------------------------------------------------------------------------")
            read_csv.decrypt_file(key, value)
        log_message(INFO, "\nData loading completed for table " + request.table.name + "\n")

        res: destination_sdk_pb2.WriteBatchResponse = destination_sdk_pb2.WriteBatchResponse(success=True)
        return res

    def WriteHistoryBatch(self, request, context):
        '''
        Reference: https://github.com/fivetran/fivetran_sdk/blob/main/how-to-handle-history-mode-batch-files.md

        The `WriteHistoryBatch` method is used to write history mode-specific batch files to the destination.
        The incoming batch files are processed in the exact following order:
        1. `earliest_start_files`
        2. `replace_files`
        3. `update_files`
        4. `delete_files`

        1. **`earliest_start_files`**
           - Contains a single record per primary key with the earliest `_fivetran_start`.
           - Operations:
             - Delete overlapping records where `_fivetran_start` is greater than `earliest_fivetran_start`.
             - Update history mode-specific system columns (`fivetran_active` and `_fivetran_end`).

        2. **`update_files`**
           - Contains records with modified column values.
           - Process:
             - Modified columns are updated with new values.
             - Unmodified columns are populated with values from the last active record in the destination.
             - New records are inserted while maintaining history tracking.

        3. **`upsert_files`**
           - Contains records where all column values are modified.
           - Process:
             - Insert new records directly into the destination table.

        4. **`delete_files`**
           - Deactivates records in the destination table.
           - Process:
             - Set `_fivetran_active` to `FALSE`.
             - Update `_fivetran_end` to match the corresponding record’s end timestamp from the batch file.

        This structured processing ensures data consistency and historical tracking in the destination table.
        '''
        for earliest_start_file in request.earliest_start_files:
            print("earliest_start files: " + str(earliest_start_file))
        for replace_file in request.replace_files:
            print("replace files: " + str(replace_file))
        for update_file in request.update_files:
            print("replace files: " + str(update_file))
        for delete_file in request.delete_files:
            print("delete files: " + str(delete_file))

        log_message(WARNING, "Data loading started for table " + request.table.name)
        for key, value in request.keys.items():
            print("----------------------------------------------------------------------------")
            print("Decrypting and printing file :" + str(key))
            print("----------------------------------------------------------------------------")
            read_csv.decrypt_file(key, value)
        log_message(INFO, "\nData loading completed for table " + request.table.name + "\n")

        res: destination_sdk_pb2.WriteBatchResponse = destination_sdk_pb2.WriteBatchResponse(success=True)
        return res

    def DescribeTable(self, request, context):
        log_message(SEVERE, "Sample severe message: Completed fetching table info")
        if request.table_name not in DestinationImpl.table_map:
            return destination_sdk_pb2.DescribeTableResponse(not_found=True)
        else:
            return destination_sdk_pb2.DescribeTableResponse(not_found=False, table=DestinationImpl.table_map[request.table_name])

    def Migrate(self, request, context):
            """
            Handles schema migration operations introduced in the updated proto.

            MigrationDetails oneof operation cases:
              - drop
              - copy
              - rename
              - add
              - update_column_value
              - table_sync_mode_migration

            Each of these may itself contain a nested oneof (e.g. drop.entity, copy.entity, etc.)
            This implementation logs and performs simple in-memory metadata adjustments on table_map.
            Extend with real DDL / data operations for production use.

            The MigrateResponse has oneof 'response':
              success | unsupported | warning | task
            Here we demonstrate success/unsupported flows.
            """
            details = request.details
            schema = details.schema
            table = details.table

            operation_case = details.WhichOneof("operation")
            log_message(INFO, f"[Migrate] schema={schema} table={table} operation={operation_case}")

            # Default builder logic: we'll decide which field to set
            # The Python generated class will treat setting multiple oneof
            # members as overwriting previous, so set exactly one.
            response = None

            if operation_case == "drop":
                self._handle_drop(details.drop, schema, table)
                response = destination_sdk_pb2.MigrateResponse(success=True)

            elif operation_case == "copy":
                self._handle_copy(details.copy, schema, table)
                response = destination_sdk_pb2.MigrateResponse(success=True)

            elif operation_case == "rename":
                self._handle_rename(details.rename, schema, table)
                response = destination_sdk_pb2.MigrateResponse(success=True)

            elif operation_case == "add":
                self._handle_add(details.add, schema, table)
                response = destination_sdk_pb2.MigrateResponse(success=True)

            elif operation_case == "update_column_value":
                self._handle_update_column_value(details.update_column_value, schema, table)
                response = destination_sdk_pb2.MigrateResponse(success=True)

            elif operation_case == "table_sync_mode_migration":
                self._handle_table_sync_mode_migration(details.table_sync_mode_migration, schema, table)
                response = destination_sdk_pb2.MigrateResponse(success=True)

            else:
                log_message(WARNING, "[Migrate] Unsupported or missing operation")
                response = destination_sdk_pb2.MigrateResponse(unsupported=True)

            # Example to return a warning instead:
            # response = destination_sdk_pb2.MigrateResponse(
            #     warning=common_pb2.Warning(message="Non-critical migration issue")
            # )

            # Example to return a task for async processing:
            # response = destination_sdk_pb2.MigrateResponse(
            #     task=common_pb2.Task(id="background-migration-123")
            # )

            return response

        # ----------- Helper handlers for migration operations ----------- #

        def _handle_drop(self, drop_op, schema, table):
            entity_case = drop_op.WhichOneof("entity")
            if entity_case == "drop_table":
                log_message(INFO, f"[Migrate:Drop] Dropping table {schema}.{table}")
                DestinationImpl.table_map.pop(table, None)
            elif entity_case == "drop_column_in_history_mode":
                col = drop_op.drop_column_in_history_mode.column
                op_ts = drop_op.drop_column_in_history_mode.operation_timestamp
                log_message(INFO, f"[Migrate:DropColumnHistory] table={schema}.{table} column={col} op_ts={op_ts}")
                # Implement column removal logic from metadata if needed.
            else:
                log_message(WARNING, "[Migrate:Drop] No drop entity specified")

        def _handle_copy(self, copy_op, schema, table):
            entity_case = copy_op.WhichOneof("entity")
            if entity_case == "copy_table":
                cpy_table = copy_op.copy_table
                log_message(INFO, f"[Migrate:CopyTable] from={cpy_table.from_table} to={cpy_table.to_table} schema={schema}")
                # Copy metadata / data placeholder.
                if frm in DestinationImpl.table_map:
                    DestinationImpl.table_map[to] = DestinationImpl.table_map[frm]
            elif entity_case == "copy_column":
                cpy_col = copy_op.copy_column
                log_message(INFO, f"[Migrate:CopyColumn] table={schema}.{table} from_col={cpy_col.from_column} to_col={cpy_col.to_column}")
            elif entity_case == "copy_table_to_history_mode":
                frm = copy_op.copy_table_to_history_mode.from_table
                to = copy_op.copy_table_to_history_mode.to_table
                soft_deleted_col = copy_op.copy_table_to_history_mode.soft_deleted_column
                log_message(INFO, f"[Migrate:CopyTableToHistoryMode] from={frm} to={to} soft_deleted_column={soft_deleted_col}")
            else:
                log_message(WARNING, "[Migrate:Copy] No copy entity specified")

        def _handle_rename(self, rename_op, schema, table):
            entity_case = rename_op.WhichOneof("entity")
            if entity_case == "rename_table":
                frm = rename_op.rename_table.from_table
                to = rename_op.rename_table.to_table
                log_message(INFO, f"[Migrate:RenameTable] from={frm} to={to} schema={schema}")
                if frm in DestinationImpl.table_map:
                    tbl_meta = DestinationImpl.table_map.pop(frm)
                    # Adjust name inside the Table metadata if needed (proto Table likely has 'name' field)
                    tbl_meta = tbl_meta.__class__.FromString(tbl_meta.SerializeToString())
                    # If Table has a 'name' field we can rebuild:
                    if hasattr(tbl_meta, "name"):
                        tbl_meta.name = to
                    DestinationImpl.table_map[to] = tbl_meta
            elif entity_case == "rename_column":
                frm_col = rename_op.rename_column.from_column
                to_col = rename_op.rename_column.to_column
                log_message(INFO, f"[Migrate:RenameColumn] table={schema}.{table} from_col={frm_col} to_col={to_col}")
                # Adjust column metadata if tracking columns explicitly.
            else:
                log_message(WARNING, "[Migrate:Rename] No rename entity specified")

        def _handle_add(self, add_op, schema, table):
            entity_case = add_op.WhichOneof("entity")
            if entity_case == "add_column_in_history_mode":
                col = add_op.add_column_in_history_mode.column
                ctype = add_op.add_column_in_history_mode.column_type
                default = add_op.add_column_in_history_mode.default_value
                op_ts = add_op.add_column_in_history_mode.operation_timestamp
                log_message(INFO, f"[Migrate:AddColumnHistory] table={schema}.{table} column={col} type={ctype} default={default} op_ts={op_ts}")
            elif entity_case == "add_column_with_default_value":
                col = add_op.add_column_with_default_value.column
                ctype = add_op.add_column_with_default_value.column_type
                default = add_op.add_column_with_default_value.default_value
                log_message(INFO, f"[Migrate:AddColumnDefault] table={schema}.{table} column={col} type={ctype} default={default}")
            else:
                log_message(WARNING, "[Migrate:Add] No add entity specified")

        def _handle_update_column_value(self, upd, schema, table):
            col = upd.column
            value = upd.value
            log_message(INFO, f"[Migrate:UpdateColumnValue] table={schema}.{table} column={col} value={value}")
            # Placeholder: Iterate and update rows in storage layer.

        def _handle_table_sync_mode_migration(self, op, schema, table):
            sync_type = op.type
            soft_deleted_column = op.soft_deleted_column if op.HasField("soft_deleted_column") else "N/A"
            keep_deleted_rows = op.keep_deleted_rows if op.HasField("keep_deleted_rows") else False
            log_message(INFO, f"[Migrate:TableSyncModeMigration] table={schema}.{table} type={sync_type} soft_deleted_column={soft_deleted_column} keep_deleted_rows={keep_deleted_rows}")
def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_destination"}}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50052,
                        help="The server port")
    args = parser.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    server.add_insecure_port(f'[::]:{args.port}')
    destination_sdk_pb2_grpc.add_DestinationConnectorServicer_to_server(DestinationImpl(), server)
    server.start()
    print(f"Destination gRPC server started on port {args.port}...")
    server.wait_for_termination()
    print("Destination gRPC server terminated...")
