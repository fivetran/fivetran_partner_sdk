from concurrent import futures
import grpc
import read_csv
import sys
import argparse
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_pb2
from sdk_pb2 import common_pb2
from sdk_pb2 import destination_sdk_pb2_grpc
from schema_migration_helper import SchemaMigrationHelper
from duckdb_helper import DuckDBHelper


INFO = "INFO"
WARNING = "WARNING"
SEVERE = "SEVERE"

class DestinationImpl(destination_sdk_pb2_grpc.DestinationConnectorServicer):
    # Initialize DuckDB with a file-based database for persistence
    # Use ":memory:" for in-memory database or "destination.db" for file-based
    db_helper = None
    default_schema = "fivetran_destination"

    def __init__(self):
        super().__init__()
        # Initialize DuckDB helper (use file-based database)
        if DestinationImpl.db_helper is None:
            DestinationImpl.db_helper = DuckDBHelper("destination.db")
        self.migration_helper = SchemaMigrationHelper(DestinationImpl.db_helper)

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

        # Add Descriptive Dropdown Field (Pooling Field)
        pooling_field = common_pb2.FormField(
            name="poolingStrategy",
            label="Connection Pooling Strategy",
            description="Select the pooling strategy for managing database connections.",
            required=True,
            descriptive_dropdown_fields=common_pb2.DescriptiveDropDownFields(
                descriptive_dropdown_field=[
                    common_pb2.DescriptiveDropDownField(
                        label="Basic Pooling",
                        value="basic_pooling",
                        description="Provides minimal connection reuse and low resource overhead."
                    ),
                    common_pb2.DescriptiveDropDownField(
                        label="Standard Pooling",
                        value="standard_pooling",
                        description="Balances connection reuse and performance for typical workloads."
                    ),
                    common_pb2.DescriptiveDropDownField(
                        label="Advanced Pooling",
                        value="advanced_pooling",
                        description="Uses intelligent algorithms to optimize performance for high concurrency and throughput."
                    ),
                ]
            ),
            default_value="standard_pooling"
        )

        # uploadFile upload field
        upload_file = common_pb2.FormField(
            name="uploadFile",
            label="Upload Configuration File",
            description="Upload a configuration file (e.g., JSON, YAML, or certificate)",
            upload_field=common_pb2.UploadField(
                allowed_file_type=[".json", ".yaml", ".yml", ".pem", ".crt"],
                max_file_size_bytes=1048576  # 1 MB
            )
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
            enable_encryption,
            pooling_field,
            upload_file
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
        schema_name = request.schema_name if request.schema_name else self.default_schema
        print("[CreateTable] :" + str(schema_name) + " | " + str(request.table.name) + " | " + str(request.table.columns))
        try:
            self.db_helper.create_table(schema_name, request.table)
            return destination_sdk_pb2.CreateTableResponse(success=True)
        except Exception as e:
            log_message(WARNING, f"CreateTable failed: {str(e)}")
            return destination_sdk_pb2.CreateTableResponse(success=False)

    def AlterTable(self, request, context):
        schema_name = request.schema_name if request.schema_name else self.default_schema
        drop_columns = request.drop_columns
        print(f"[AlterTable]: {schema_name} | {request.table.name} | drop_columns={drop_columns} | columns={request.table.columns}")

        try:
            # Get current table schema
            current_table = self.db_helper.describe_table(schema_name, request.table.name)

            if current_table is None:
                log_message(WARNING, f"Table {schema_name}.{request.table.name} does not exist")
                return destination_sdk_pb2.AlterTableResponse(success=False)

            # Compare current and requested columns
            current_column_names = {col.name for col in current_table.columns}
            requested_column_names = {col.name for col in request.table.columns}

            # Find columns to add (in request but not in current)
            new_columns = [col for col in request.table.columns if col.name not in current_column_names]

            # Find columns to drop (in current but not in request)
            columns_to_drop = [col.name for col in current_table.columns if col.name not in requested_column_names]

            # Add new columns
            for column in new_columns:
                self.db_helper.add_column(schema_name, request.table.name, column)
                log_message(INFO, f"Added column: {column.name} to {schema_name}.{request.table.name}")

            # Drop columns if drop_columns flag is true
            if drop_columns and columns_to_drop:
                for column_name in columns_to_drop:
                    self.db_helper.drop_column(schema_name, request.table.name, column_name)
                    log_message(INFO, f"Dropped column: {column_name} from {schema_name}.{request.table.name}")
            elif columns_to_drop:
                log_message(INFO, f"Skipping drop of {len(columns_to_drop)} columns (drop_columns=false): {columns_to_drop}")

            return destination_sdk_pb2.AlterTableResponse(success=True)
        except Exception as e:
            log_message(WARNING, f"AlterTable failed: {str(e)}")
            return destination_sdk_pb2.AlterTableResponse(success=False)

    def Truncate(self, request, context):
        schema_name = request.schema_name if request.schema_name else self.default_schema
        table_name = request.table_name
        print("[TruncateTable]: " + str(schema_name) + " | " + str(table_name) + " | soft=" + str(request.HasField("soft")))
        try:
            # Check if soft truncate is requested
            if request.HasField("soft"):
                # Soft truncate: mark rows as deleted instead of removing them
                deleted_column = request.soft.deleted_column
                log_message(INFO, f"Performing soft truncate on {schema_name}.{table_name} using column {deleted_column}")

                # Build UPDATE statement to mark all rows as deleted
                escaped_schema = self.db_helper.escape_identifier(schema_name)
                escaped_table = self.db_helper.escape_identifier(table_name)
                escaped_deleted_col = self.db_helper.escape_identifier(deleted_column)

                # Handle time-based truncate if synced_column and utc_delete_before are provided
                if request.synced_column and request.HasField("utc_delete_before"):
                    escaped_synced_col = self.db_helper.escape_identifier(request.synced_column)
                    delete_before_timestamp = request.utc_delete_before.ToDatetime()
                    sql = f'UPDATE "{escaped_schema}"."{escaped_table}" SET "{escaped_deleted_col}" = TRUE WHERE "{escaped_synced_col}" < ?'
                    self.db_helper.get_connection().execute(sql, [delete_before_timestamp])
                    log_message(INFO, f"Soft truncated rows where {request.synced_column} < {delete_before_timestamp}")
                else:
                    # Mark all rows as deleted
                    sql = f'UPDATE "{escaped_schema}"."{escaped_table}" SET "{escaped_deleted_col}" = TRUE'
                    self.db_helper.get_connection().execute(sql)
                    log_message(INFO, f"Soft truncated all rows in {schema_name}.{table_name}")
            else:
                # Hard truncate: remove all data from the table
                self.db_helper.truncate_table(schema_name, table_name)
                log_message(INFO, f"Hard truncated {schema_name}.{table_name}")

            return destination_sdk_pb2.TruncateResponse(success=True)
        except Exception as e:
            log_message(WARNING, f"Truncate failed: {str(e)}")
            return destination_sdk_pb2.TruncateResponse(success=False)

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
        schema_name = request.schema_name if request.schema_name else self.default_schema
        log_message(INFO, f"Completed fetching table info for {schema_name}.{request.table_name}")
        try:
            table = self.db_helper.describe_table(schema_name, request.table_name)
            if table is None:
                return destination_sdk_pb2.DescribeTableResponse(not_found=True)
            else:
                return destination_sdk_pb2.DescribeTableResponse(not_found=False, table=table)
        except Exception as e:
            log_message(WARNING, f"DescribeTable failed: {str(e)}")
            return destination_sdk_pb2.DescribeTableResponse(not_found=True)

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
            response = self.migration_helper.handle_drop(details.drop, schema, table)

        elif operation_case == "copy":
            response = self.migration_helper.handle_copy(details.copy, schema, table)

        elif operation_case == "rename":
            response = self.migration_helper.handle_rename(details.rename, schema, table)

        elif operation_case == "add":
            response = self.migration_helper.handle_add(details.add, schema, table)

        elif operation_case == "update_column_value":
            response = self.migration_helper.handle_update_column_value(details.update_column_value, schema, table)

        elif operation_case == "table_sync_mode_migration":
            response = self.migration_helper.handle_table_sync_mode_migration(details.table_sync_mode_migration, schema, table)

        else:
            log_message(WARNING, "[Migrate] Unsupported or missing operation")
            response = destination_sdk_pb2.MigrateResponse(unsupported=True)

        # Example: to return a warning instead:
        # response = destination_sdk_pb2.MigrateResponse(
        #     warning=common_pb2.Warning(message="Non-critical issue")
        # )

        # Example: to return a task instead (async pattern):
        # response = destination_sdk_pb2.MigrateResponse(
        #             task=common_pb2.Task(message="error-message")
        # )

        # Example to return UNSUPPORTED status:
        # response = destination_sdk_pb2.MigrateResponse(unsupported=True)

        return response

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
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nReceived shutdown signal...")
    finally:
        print("Shutting down server...")
        if DestinationImpl.db_helper:
            DestinationImpl.db_helper.close()
        server.stop(grace=5)
        print("Destination gRPC server terminated...")
