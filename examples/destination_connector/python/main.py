from concurrent import futures
import grpc
import read_csv
import sys
import argparse
import socket
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_pb2
from sdk_pb2 import common_pb2
from sdk_pb2 import destination_sdk_pb2_grpc
from schema_migration_helper import SchemaMigrationHelper
from duckdb_helper import DuckDBHelper
from table_operations_helper import TableOperationsHelper


INFO = "INFO"
WARNING = "WARNING"
SEVERE = "SEVERE"

class DestinationImpl(destination_sdk_pb2_grpc.DestinationConnectorServicer):
    # DuckDB helper for data persistence
    # To use in-memory storage instead, pass ":memory:" to DuckDBHelper
    db_helper = None
    default_schema = "fivetran_destination"

    def __init__(self):
        super().__init__()
        # Initialize DuckDB helper
        # To use in-memory storage instead, pass ":memory:" to DuckDBHelper
        if DestinationImpl.db_helper is None:
            DestinationImpl.db_helper = DuckDBHelper("destination.db")

        self.migration_helper = SchemaMigrationHelper(DestinationImpl.db_helper)
        self.table_operations_helper = TableOperationsHelper(DestinationImpl.db_helper)


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
        """
        Handle table creation.
        Implementation details are in table_operations_helper.py.
        """
        return self.table_operations_helper.create_table(request, self.default_schema)

    def AlterTable(self, request, context):
        """
        Handle table alterations (add columns, change types, modify primary keys, drop columns).
        Implementation details are in table_operations_helper.py.
        """
        return self.table_operations_helper.alter_table(request, request.schema_name, self.default_schema)

    def Truncate(self, request, context):
        """
        Handle table truncation (both hard and soft truncate).
        Implementation details are in table_operations_helper.py.
        """
        return self.table_operations_helper.truncate_table(request, self.default_schema)

    def WriteBatch(self, request, context):
        """
        Write batch data to the destination.

        This example implementation decrypts and prints batch files for demonstration purposes.
        For production use, implement your data loading logic here to process REPLACE, UPDATE,
        and DELETE files and write them to your destination.

        See: https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide/destination-connector-development-guide.md#writebatchrequest
        """
        for replace_file in request.replace_files:
            print("replace files: " + str(replace_file))
        for update_file in request.update_files:
            print("update files: " + str(update_file))
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
        This example implementation decrypts and prints history mode batch files for demonstration purposes.
        For production use, implement your data loading logic here to process history mode-specific batch files
        (earliest_start_files, replace_files, update_files, delete_files) and write them to your destination
        while maintaining history tracking with _fivetran_start, _fivetran_end, and _fivetran_active columns.

        See: https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide/destination-connector-development-guide.md#writehistorybatchrequest
        '''
        for earliest_start_file in request.earliest_start_files:
            print("earliest_start files: " + str(earliest_start_file))
        for replace_file in request.replace_files:
            print("replace files: " + str(replace_file))
        for update_file in request.update_files:
            print("update files: " + str(update_file))
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
        """
        Handle table description/metadata retrieval.
        Implementation details are in table_operations_helper.py.
        """
        return self.table_operations_helper.describe_table(request, self.default_schema)

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
    import json
    escaped_message = json.dumps(message)
    print(f'{{"level": "{level}", "message": {escaped_message}, "message-origin": "sdk_destination"}}')

def is_port_in_use(port):
    """Check if a port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('', port))
            return False
        except OSError:
            return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50052,
                        help="The server port")
    args = parser.parse_args()

    # Check if port is already in use BEFORE initializing database connection
    if is_port_in_use(args.port):
        raise RuntimeError(f"Port {args.port} is already in use. Another server may be running.")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    destination_sdk_pb2_grpc.add_DestinationConnectorServicer_to_server(DestinationImpl(), server)
    server.add_insecure_port(f'[::]:{args.port}')
    try:
        server.start()
        print(f"Destination gRPC server started on port {args.port}...")
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nReceived shutdown signal...")
    finally:
        print("Shutting down server...")
        # Stop server first with grace period to allow in-flight requests to complete
        server.stop(grace=5)
        # Close database connection after all requests have finished
        if DestinationImpl.db_helper:
            DestinationImpl.db_helper.close()
        print("Destination gRPC server terminated...")
