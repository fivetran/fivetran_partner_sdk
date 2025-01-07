from concurrent import futures
import grpc
import read_csv
import sys
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_v2_pb2
from sdk_pb2 import common_v2_pb2
from sdk_pb2 import destination_sdk_v2_pb2_grpc


INFO = "INFO"
WARNING = "WARNING"
SEVERE = "SEVERE"

class DestinationImpl(destination_sdk_v2_pb2_grpc.DestinationConnectorServicer):
    def ConfigurationForm(self, request, context):
        log_message(INFO, "Fetching Configuration form")

        # Create the form fields
        form_fields = common_v2_pb2.ConfigurationFormResponse(
            schema_selection_supported=True,
            table_selection_supported=True
        )

        # writerType field with dropdown
        writer_type = common_v2_pb2.FormField(
            name="writerType",
            label="Writer Type",
            description="Choose the destination type",
            dropdown_field=common_v2_pb2.DropdownField(dropdown_field=["Database", "File", "Cloud"]),
            default_value="Database"
        )

        # host field
        host = common_v2_pb2.FormField(
            name="host",
            label="Host",
            text_field=common_v2_pb2.TextField.PlainText,
            placeholder="your_host_details"
        )

        # port field
        port = common_v2_pb2.FormField(
            name="port",
            label="Port",
            text_field=common_v2_pb2.TextField.PlainText,
            placeholder="your_port_details"
        )

        # user field
        user = common_v2_pb2.FormField(
            name="user",
            label="User",
            text_field=common_v2_pb2.TextField.PlainText,
            placeholder="user_name"
        )

        # password field
        password = common_v2_pb2.FormField(
            name="password",
            label="Password",
            text_field=common_v2_pb2.TextField.Password,
            placeholder="your_password"
        )

        # database field
        database = common_v2_pb2.FormField(
            name="database",
            label="Database",
            text_field=common_v2_pb2.TextField.PlainText,
            placeholder="your_database_name"
        )

        # table field
        table = common_v2_pb2.FormField(
            name="table",
            label="Table",
            text_field=common_v2_pb2.TextField.PlainText,
            placeholder="your_table_name"
        )

        # filePath field
        file_path = common_v2_pb2.FormField(
            name="filePath",
            label="File Path",
            text_field=common_v2_pb2.TextField.PlainText,
            placeholder="your_file_path"
        )

        # region field with dropdown
        region = common_v2_pb2.FormField(
            name="region",
            label="Cloud Region",
            description="Choose the cloud region",
            dropdown_field=common_v2_pb2.DropdownField(dropdown_field=["Azure", "AWS", "Google Cloud"]),
            default_value="Azure"
        )

        # enableEncryption toggle field
        enable_encryption = common_v2_pb2.FormField(
            name="enableEncryption",
            label="Enable Encryption?",
            description="To enable/disable encryption for data transfer",
            toggle_field=common_v2_pb2.ToggleField()
        )

        # Define Visibility Conditions for Conditional Fields
        visibility_condition_for_cloud = common_v2_pb2.VisibilityCondition(
            condition_field="writerType",
            string_value="Cloud"
        )

        visibility_condition_for_database = common_v2_pb2.VisibilityCondition(
            condition_field="writerType",
            string_value="Database"
        )

        visibility_condition_for_file = common_v2_pb2.VisibilityCondition(
            condition_field="writerType",
            string_value="File"
        )

        # Create conditional fields for Cloud
        conditional_fields_for_cloud = common_v2_pb2.ConditionalFields(
            condition=visibility_condition_for_cloud,
            fields=[host, port, user, password, region]
        )

        # Create conditional fields for File
        conditional_fields_for_file = common_v2_pb2.ConditionalFields(
            condition=visibility_condition_for_file,
            fields=[host, port, user, password, table, file_path]
        )

        # Create conditional fields for Database
        conditional_fields_for_database = common_v2_pb2.ConditionalFields(
            condition=visibility_condition_for_database,
            fields=[host, port, user, password, database, table]
        )

        # Add conditional fields to the form
        conditional_field_for_cloud = common_v2_pb2.FormField(
            name="doesNotMatterCloud",
            label="It won't be used",
            conditional_fields=conditional_fields_for_cloud
        )

        conditional_field_for_file = common_v2_pb2.FormField(
            name="doesNotMatterFile",
            label="It won't be used",
            conditional_fields=conditional_fields_for_file
        )

        conditional_field_for_database = common_v2_pb2.FormField(
            name="doesNotMatterDatabase",
            label="It won't be used",
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
        return common_v2_pb2.TestResponse(success=True)

    def CreateTable(self, request, context):
        print("[CreateTable] :" + str(request.schema_name) + " | " + str(request.table.name) + " | " + str(request.table.columns))
        return destination_sdk_v2_pb2.CreateTableResponse(success=True)

    def AlterTable(self, request, context):
        res: destination_sdk_v2_pb2.AlterTableResponse

        print("[AlterTable]: " + str(request.schema_name) + " | " + str(request.table.name) + " | " + str(request.table.columns))
        return destination_sdk_v2_pb2.AlterTableResponse(success=True)

    def Truncate(self, request, context):
        print("[TruncateTable]: " + str(request.schema_name) + " | " + str(request.schema_name) + " | soft" + str(request.soft))
        return destination_sdk_v2_pb2.TruncateResponse(success=True)

    def WriteBatch(self, request, context):
        for replace_file in request.replace_files:
            print("replace files: " + str(replace_file))
        for update_file in request.update_files:
            print("replace files: " + str(update_file))
        for delete_file in request.delete_files:
            print("replace files: " + str(delete_file))

        log_message(WARNING, "Data loading started for table " + request.table.name)
        for key, value in request.keys.items():
            print("----------------------------------------------------------------------------")
            print("Decrypting and printing file :" + str(key))
            print("----------------------------------------------------------------------------")
            read_csv.decrypt_file(key, value)
        log_message(INFO, "\nData loading completed for table " + request.table.name + "\n")

        res: destination_sdk_v2_pb2.WriteBatchResponse = destination_sdk_v2_pb2.WriteBatchResponse(success=True)
        return res

    def DescribeTable(self, request, context):
        column1 = common_v2_pb2.Column(name="a1", type=common_v2_pb2.DataType.UNSPECIFIED, primary_key=True)
        column2 = common_v2_pb2.Column(name="a2", type=common_v2_pb2.DataType.DOUBLE)
        table: common_v2_pb2.Table = common_v2_pb2.Table(name=request.table_name, columns=[column1, column2])
        log_message(SEVERE, "Sample severe message: Completed fetching table info")
        return destination_sdk_v2_pb2.DescribeTableResponse(not_found=False, table=table)

def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_destination"}}')

if __name__ == '__main__':
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    server.add_insecure_port('[::]:50052')
    destination_sdk_v2_pb2_grpc.add_DestinationConnectorServicer_to_server(DestinationImpl(), server)
    server.start()
    print("Destination gRPC server started...")
    server.wait_for_termination()
    print("Destination gRPC server terminated...")
