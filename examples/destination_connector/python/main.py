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

# Constants for system columns
FIVETRAN_START = "_fivetran_start"
FIVETRAN_END = "_fivetran_end"
FIVETRAN_ACTIVE = "_fivetran_active"

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
        Example implementation of the new Migrate RPC introduced for schema migration support.
        This method inspects which migration operation (oneof) was requested and logs / handles it.
        For demonstration, all recognized operations return success.

        :param request: The migration request containing details of the operation.
        :param context: gRPC context

        Note: This is just for demonstration, so no logic for migration is implemented
              rather different migration methods are just manipulating table_map to simulate
              the migration operations.
        """
        details = request.details
        schema = details.schema
        table = details.table

        operation_case = details.WhichOneof("operation")
        log_message(INFO, f"[Migrate] schema={schema} table={table} operation={operation_case}")

        response = None

        if operation_case == "drop":
            response = self._handle_drop(details.drop, schema, table)

        elif operation_case == "copy":
            response = self._handle_copy(details.copy, schema, table)

        elif operation_case == "rename":
            response = self._handle_rename(details.rename, schema, table)

        elif operation_case == "add":
            response = self._handle_add(details.add, schema, table)

        elif operation_case == "update_column_value":
            response = self._handle_update_column_value(details.update_column_value, schema, table)

        elif operation_case == "table_sync_mode_migration":
            response = self._handle_table_sync_mode_migration(details.table_sync_mode_migration, schema, table)

        else:
            log_message(WARNING, "[Migrate] Unsupported or missing operation")
            response = destination_sdk_pb2.MigrateResponse(unsupported=True)

        # Example: to return a warning instead:
        # response = destination_sdk_pb2.MigrateResponse(
        #     warning=common_pb2.Warning(message="Non-critical issue")
        # )

        # Example: to return a task instead (async pattern):
        # response = destination_sdk_pb2.MigrateResponse(
        #     task=common_pb2.Task(id="background-migration-123")
        # )

        # Example to return UNSUPPORTED status:
        # response = destination_sdk_pb2.MigrateResponse(unsupported=True)

        return response

    # ----------- Helper handlers for migration operations ----------- #

    def _handle_drop(self, drop_op, schema, table):
        """Handles drop operations (drop table, drop column in history mode)."""
        entity_case = drop_op.WhichOneof("entity")

        if entity_case == "drop_table":
            # table-map manipulation to simulate drop, replace with actual logic.
            DestinationImpl.table_map.pop(table, None)

            log_message(INFO, f"[Migrate:Drop] Dropping table {schema}.{table}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "drop_column_in_history_mode":
            # table-map manipulation to simulate drop column in history mode, replace with actual logic.
            dcol = drop_op.drop_column_in_history_mode
            table_obj = DestinationImpl.table_map.get(table)
            if table_obj:
                # Remove the specified column from the table
                columns_to_keep = [col for col in table_obj.columns if col.name != dcol.column]
                del table_obj.columns[:]
                table_obj.columns.extend(columns_to_keep)

            log_message(INFO, f"[Migrate:DropColumnHistory] table={schema}.{table} column={dcol.column} op_ts={dcol.operation_timestamp}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Drop] No drop entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def _handle_copy(self, copy_op, schema, table):
        """Handles copy operations (copy table, copy column, copy table to history mode)."""
        entity_case = copy_op.WhichOneof("entity")

        if entity_case == "copy_table":
            # table-map manipulation to simulate copy, replace with actual logic.
            ct = copy_op.copy_table
            if ct.from_table in DestinationImpl.table_map:
                DestinationImpl.table_map[ct.to_table] = DestinationImpl.table_map[ct.from_table]

            log_message(INFO, f"[Migrate:CopyTable] from={ct.from_table} to={ct.to_table} in schema={schema}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "copy_column":
            # table-map manipulation to simulate copy column, replace with actual logic.
            cc = copy_op.copy_column
            table_obj = DestinationImpl.table_map.get(table)
            if table_obj:
                for col in table_obj.columns:
                    if col.name == cc.from_column:
                        new_col = type(col)()
                        new_col.CopyFrom(col)
                        new_col.name = cc.to_column
                        table_obj.columns.add().CopyFrom(new_col)
                        break

            log_message(INFO, f"[Migrate:CopyColumn] table={schema}.{table} from_col={cc.from_column} to_col={cc.to_column}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "copy_table_to_history_mode":
            # table-map manipulation to simulate copy table to history mode, replace with actual logic.
            cth = copy_op.copy_table_to_history_mode
            if cth.from_table in DestinationImpl.table_map:
                from_table_obj = DestinationImpl.table_map[cth.from_table]
                new_table = self._create_table_copy(from_table_obj, cth.to_table)
                self._remove_column_from_table(new_table, cth.soft_deleted_column)
                self._add_history_mode_columns(new_table)
                DestinationImpl.table_map[cth.to_table] = new_table

            log_message(INFO, f"[Migrate:CopyTableToHistoryMode] from={cth.from_table} to={cth.to_table} soft_deleted_column={cth.soft_deleted_column}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Copy] No copy entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def _handle_rename(self, rename_op, schema, table):
        """Handles rename operations (rename table, rename column)."""
        entity_case = rename_op.WhichOneof("entity")

        if entity_case == "rename_table":
            # table-map manipulation to simulate rename, replace with actual logic.
            rt = rename_op.rename_table
            if rt.from_table in DestinationImpl.table_map:
                tbl = DestinationImpl.table_map.pop(rt.from_table)
                # Adjust name inside the Table metadata if needed
                tbl_copy = tbl.__class__.FromString(tbl.SerializeToString())
                if hasattr(tbl_copy, "name"):
                    tbl_copy.name = rt.to_table
                DestinationImpl.table_map[rt.to_table] = tbl_copy

            log_message(INFO, f"[Migrate:RenameTable] from={rt.from_table} to={rt.to_table} schema={schema}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "rename_column":
            # table-map manipulation to simulate rename column, replace with actual logic.
            rc = rename_op.rename_column
            table_obj = DestinationImpl.table_map.get(table)
            if table_obj:
                # Rename the column
                for col in table_obj.columns:
                    if col.name == rc.from_column:
                        col.name = rc.to_column
                        break

            log_message(INFO, f"[Migrate:RenameColumn] table={schema}.{table} from_col={rc.from_column} to_col={rc.to_column}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Rename] No rename entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def _handle_add(self, add_op, schema, table):
        """Handles add operations (add column in history mode, add column with default value)."""
        entity_case = add_op.WhichOneof("entity")

        if entity_case == "add_column_in_history_mode":
            # table-map manipulation to simulate add column in history mode, replace with actual logic.
            ach = add_op.add_column_in_history_mode
            table_obj = DestinationImpl.table_map.get(table)
            if table_obj:
                new_col = table_obj.columns.add()
                new_col.name = ach.column
                new_col.type = ach.column_type

            log_message(INFO, f"[Migrate:AddColumnHistory] table={schema}.{table} column={ach.column} type={ach.column_type} default={ach.default_value} op_ts={ach.operation_timestamp}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "add_column_with_default_value":
            # table-map manipulation to simulate add column with default value, replace with actual logic.
            acd = add_op.add_column_with_default_value
            table_obj = DestinationImpl.table_map.get(table)
            if table_obj:
                new_col = table_obj.columns.add()
                new_col.name = acd.column
                new_col.type = acd.column_type

            log_message(INFO, f"[Migrate:AddColumnDefault] table={schema}.{table} column={acd.column} type={acd.column_type} default={acd.default_value}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Add] No add entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def _handle_update_column_value(self, upd, schema, table):
        """Handles update column value operation."""
        # Placeholder: Update all existing rows' column value.

        log_message(INFO, f"[Migrate:UpdateColumnValue] table={schema}.{table} column={upd.column} value={upd.value}")
        return destination_sdk_pb2.MigrateResponse(success=True)

    def _handle_table_sync_mode_migration(self, op, schema, table):
        """Handles table sync mode migration operations."""
        table_obj = DestinationImpl.table_map.get(table)

        keep_deleted_rows = op.keep_deleted_rows if op.HasField("keep_deleted_rows") else False
        soft_deleted_column = op.soft_deleted_column if op.HasField("soft_deleted_column") else None

        # Determine the migration type and handle accordingly
        if op.type == destination_sdk_pb2.TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE:
            # table-map manipulation to simulate soft delete to live, replace with actual logic.
            table_copy = self._create_table_copy(table_obj, table_obj.name)
            self._remove_column_from_table(table_copy, soft_deleted_column)
            DestinationImpl.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from SOFT_DELETE to LIVE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY:
            # table-map manipulation to simulate soft delete to history, replace with actual logic.
            table_copy = self._create_table_copy(table_obj, table_obj.name)
            self._remove_column_from_table(table_copy, soft_deleted_column)
            self._add_history_mode_columns(table_copy)
            DestinationImpl.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from SOFT_DELETE to HISTORY")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.HISTORY_TO_SOFT_DELETE:
            # table-map manipulation to simulate history to soft delete, replace with actual logic.
            table_copy = self._create_table_copy(table_obj, table_obj.name)
            self._remove_history_mode_columns(table_copy)
            self._add_soft_delete_column(table_copy, soft_deleted_column)
            DestinationImpl.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from HISTORY to SOFT_DELETE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.HISTORY_TO_LIVE:
            # table-map manipulation to simulate history to live, replace with actual logic.
            table_copy = self._create_table_copy(table_obj, table_obj.name)
            self._remove_history_mode_columns(table_copy)
            DestinationImpl.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from HISTORY to LIVE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE:
            # table-map manipulation to simulate live to soft delete, replace with actual logic.
            table_copy = self._create_table_copy(table_obj, table_obj.name)
            self._add_soft_delete_column(table_copy, soft_deleted_column)
            DestinationImpl.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from LIVE to SOFT_DELETE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.LIVE_TO_HISTORY:
            # table-map manipulation to simulate live to history, replace with actual logic.
            table_copy = self._create_table_copy(table_obj, table_obj.name)
            self._add_history_mode_columns(table_copy)
            DestinationImpl.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from LIVE to HISTORY")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, f"[Migrate:TableSyncModeMigration] Unknown migration type for table={schema}.{table}")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    # ----------- Helper methods for table metadata operations ----------- #

    def _create_table_copy(self, table_obj, new_name):
        """Creates a copy of a table."""
        table_copy = table_obj.__class__.FromString(table_obj.SerializeToString())
        if hasattr(table_copy, "name"):
            table_copy.name = new_name
        return table_copy

    def _remove_column_from_table(self, table_obj, column_name):
        """Removes a column from a table."""
        if not column_name or not hasattr(table_obj, 'columns'):
            return
        # Create a new list of columns excluding the specified column
        columns_to_keep = [col for col in table_obj.columns if col.name != column_name]
        # Clear and repopulate
        del table_obj.columns[:]
        table_obj.columns.extend(columns_to_keep)

    def _remove_history_mode_columns(self, table_obj):
        """Removes history mode columns from a table."""
        if not hasattr(table_obj, 'columns'):
            return
        columns_to_keep = [
            col for col in table_obj.columns
            if col.name not in [FIVETRAN_START, FIVETRAN_END, FIVETRAN_ACTIVE]
        ]
        del table_obj.columns[:]
        table_obj.columns.extend(columns_to_keep)

    def _add_history_mode_columns(self, table_obj):
        """Adds history mode columns to a table."""
        if not hasattr(table_obj, 'columns'):
            return
        start_col = table_obj.columns.add()
        start_col.name = FIVETRAN_START

        end_col = table_obj.columns.add()
        end_col.name = FIVETRAN_END

        active_col = table_obj.columns.add()
        active_col.name = FIVETRAN_ACTIVE

    def _add_soft_delete_column(self, table_obj, column_name):
        """Adds a soft delete column to a table."""
        if not column_name or not hasattr(table_obj, 'columns'):
            return
        soft_del_col = table_obj.columns.add()
        soft_del_col.name = column_name

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
