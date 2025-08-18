import grpc
from concurrent import futures
import json
import sys
import argparse
sys.path.append('sdk_pb2')

from sdk_pb2 import connector_sdk_pb2_grpc
from sdk_pb2 import common_pb2
from sdk_pb2 import connector_sdk_pb2

INFO = "INFO"
WARNING = "WARNING"
SEVERE = "SEVERE"
MAX_BATCH_RECORDS = 100
MAX_BATCH_SIZE_IN_BYTES = 100 * 1024  # 100 KiB

class ConnectorService(connector_sdk_pb2_grpc.SourceConnectorServicer):
    def ConfigurationForm(self, request, context):
        log_message(INFO, "Fetching configuration form")
        form_fields = common_pb2.ConfigurationFormResponse(schema_selection_supported=True,
                                                           table_selection_supported=True)
        # Add the 'apiBaseURL' field
        form_fields.fields.add(
            name="apiBaseURL",
            label="API base URL",
            description="Enter the base URL for the API you're connecting to",
            required=True,
            text_field=common_pb2.TextField.PlainText,
            placeholder="api_base_url"
        )

        # Add the 'authenticationMethod' dropdown field
        form_fields.fields.add(
            name="authenticationMethod",
            label="Authentication Method",
            description="Choose the preferred authentication method to securely access the API",
            dropdown_field=common_pb2.DropdownField(dropdown_field=["OAuth2.0", "API Key", "Basic Auth", "None"]),
            default_value="None"
        )

        # Add the 'api_key' field (for API Key authentication method)
        api_key = common_pb2.FormField(
            name="api_key",
            label="API Key",
            text_field=common_pb2.TextField.Password,
            placeholder="your_api_key_here"
        )

        # Add the 'client_id' field (for OAuth authentication method)
        client_id = common_pb2.FormField(
            name="client_id",
            label="Client ID",
            text_field=common_pb2.TextField.Password,
            placeholder="your_client_id_here"
        )

        # Add the 'client_secret' field (for OAuth authentication method)
        client_secret = common_pb2.FormField(
            name="client_secret",
            label="Client Secret",
            text_field=common_pb2.TextField.Password,
            placeholder="your_client_secret_here"
        )

        # Add the 'userName' field (for Basic Auth authentication method)
        username = common_pb2.FormField(
            name="username",
            label="Username",
            text_field=common_pb2.TextField.PlainText,
            placeholder="your_username_here"
        )

        # Add the 'password' field (for Basic Auth authentication method)
        password = common_pb2.FormField(
            name="password",
            label="Password",
            text_field=common_pb2.TextField.Password,
            placeholder="your_password_here"
        )

        # Define the Visibility Conditions for Conditional Fields

        # For OAuth2.0 authentication
        visibility_condition_oauth = common_pb2.VisibilityCondition(
            condition_field="authenticationMethod",
            string_value="OAuth2.0"
        )

        # Create conditional fields for OAuth2.0
        conditional_oauth_fields = common_pb2.ConditionalFields(
            condition=visibility_condition_oauth,
            fields=[client_id, client_secret]
        )

        # Add conditional fields for OAuth2.0 to the form
        form_fields.fields.add(
            name="conditionalOAuthFields",
            label="OAuth2.0 Conditional Fields",
            conditional_fields=conditional_oauth_fields
        )

        # For API Key authentication
        visibility_condition_api_key = common_pb2.VisibilityCondition(
            condition_field="authenticationMethod",
            string_value="API Key"
        )

        # Create conditional fields for API Key
        conditional_api_key_fields = common_pb2.ConditionalFields(
            condition=visibility_condition_api_key,
            fields=[api_key]
        )

        # Add conditional fields for API Key to the form
        form_fields.fields.add(
            name="conditionalApiKeyFields",
            label="API Key Conditional Fields",
            conditional_fields=conditional_api_key_fields
        )

        # For Basic Auth authentication
        visibility_condition_basic_auth = common_pb2.VisibilityCondition(
            condition_field="authenticationMethod",
            string_value="Basic Auth"
        )

        # Create conditional fields for Basic Auth
        conditional_basic_auth_fields = common_pb2.ConditionalFields(
            condition=visibility_condition_basic_auth,
            fields=[username, password]
        )

        # Add conditional fields for Basic Auth to the form
        form_fields.fields.add(
            name="conditionalBasicAuthFields",
            label="Basic Auth Conditional Fields",
            conditional_fields=conditional_basic_auth_fields
        )

        # Add the 'apiVersion' dropdown field
        form_fields.fields.add(
            name="apiVersion",
            label="API Version",
            dropdown_field=common_pb2.DropdownField(dropdown_field=["v1", "v2", "v3"]),
            default_value="v2"
        )

        # Add the 'shouldEnableMetrics' toggle field
        form_fields.fields.add(
            name="shouldEnableMetrics",
            label="Enable Metrics?",
            toggle_field=common_pb2.ToggleField()
        )

        # Add the 'connect' and 'select' tests to the form
        form_fields.tests.add(
            name="connect",
            label="Tests connection"
        )

        form_fields.tests.add(
            name="select",
            label="Tests selection"
        )

        # Return or send the populated form
        return form_fields

    def Test(self, request, context):
        configuration = request.configuration
        # Name of the test to be run
        test_name = request.name

        log_message(INFO, "Test Name: " + str(test_name))
        
        return common_pb2.TestResponse(success=True)

    def Schema(self, request, context):
        table_list = common_pb2.TableList()
        t1 = table_list.tables.add(name="table1")
        t1.columns.add(name="a1", type=common_pb2.DataType.UNSPECIFIED, primary_key=True)
        t1.columns.add(name="a2", type=common_pb2.DataType.DOUBLE)

        t2 = table_list.tables.add(name="table2")
        t2.columns.add(name="b1", type=common_pb2.DataType.UNSPECIFIED, primary_key=True)
        t2.columns.add(name="b2", type=common_pb2.DataType.UNSPECIFIED)

        return connector_sdk_pb2.SchemaResponse(without_schema=table_list)

    def _make_string_value(self, value: str) -> common_pb2.ValueType:
        v = common_pb2.ValueType()
        v.string = value
        return v

    def _make_double_value(self, value: float) -> common_pb2.ValueType:
        v = common_pb2.ValueType()
        v.double = value
        return v

    def _build_record(self, table: str, record_type: int, data: dict) -> connector_sdk_pb2.Record:
        rec = connector_sdk_pb2.Record()
        rec.type = record_type
        rec.table_name = table
        for col, val in data.items():
            rec.data[col].CopyFrom(val)
        return rec

    def _records_byte_size(self, records_list):
        """Compute serialized size (bytes) of a Records batch via protobuf ByteSize()."""
        return connector_sdk_pb2.Records(records=records_list).ByteSize()

    def _flush_batch_if_not_empty(self, batch):
        """Yield a batched UpdateResponse if the batch is not empty, then clear it."""
        if batch:
            yield connector_sdk_pb2.UpdateResponse(
                records=connector_sdk_pb2.Records(records=batch)
            )
            batch.clear()

    def _initialize_state(self, request) -> dict:
        state_json = "{}"
        if request.HasField('state_json'):
            state_json = request.state_json
        state = json.loads(state_json or "{}")
        if state.get("cursor") is None:
            state["cursor"] = 0
        return state

    def _emit_batched_records(self, state: dict):
        """
        Batch records while enforcing BOTH:
          - <= MAX_BATCH_RECORDS per batch
          - <= MAX_BATCH_SIZE_IN_BYTES serialized size per batch
        """
        batch = []

        def can_append_without_exceeding_caps(candidate: connector_sdk_pb2.Record) -> bool:
            # Count cap
            if len(batch) + 1 > MAX_BATCH_RECORDS:
                return False
            # Byte-size cap (exact via proto ByteSize)
            return self._records_byte_size(batch + [candidate]) <= MAX_BATCH_SIZE_IN_BYTES

        def append_or_flush_then_append(candidate: connector_sdk_pb2.Record):
            """Append candidate or flush first; if still too big alone, emit individually."""
            nonlocal batch
            if can_append_without_exceeding_caps(candidate):
                batch.append(candidate)
                return

            # Flush current batch first
            yield from self._flush_batch_if_not_empty(batch)

            # If candidate still exceeds size cap by itself, send individually
            if self._records_byte_size([candidate]) > MAX_BATCH_SIZE_IN_BYTES:
                log_message(WARNING, "Single record exceeds 100KiB, emitting individually")
                yield connector_sdk_pb2.UpdateResponse(record=candidate)
            else:
                batch.append(candidate)

        # Demo data: UPSERTs for table1
        for t in range(0, 3):
            rec = self._build_record(
                table="table1",
                record_type=common_pb2.RecordType.UPSERT,
                data={
                    "a1": self._make_string_value(f"a-{t}"),
                    "a2": self._make_double_value(t * 0.234),
                },
            )
            state["cursor"] += 1
            yield from append_or_flush_then_append(rec)

        # Demo data: UPSERT for table2
        rec = self._build_record(
            table="table2",
            record_type=common_pb2.RecordType.UPSERT,
            data={
                "b1": self._make_string_value("b1"),
                "b2": self._make_string_value("ben"),
            },
        )
        state["cursor"] += 1
        yield from append_or_flush_then_append(rec)

        # Final flush of leftovers
        yield from self._flush_batch_if_not_empty(batch)

    def _emit_individual_records(self, state: dict):
        # UPDATE
        update_record = self._build_record(
            table="table1",
            record_type=common_pb2.RecordType.UPDATE,
            data={
                "a1": self._make_string_value("a-0"),
                "a2": self._make_double_value(110.234),
            },
        )
        yield connector_sdk_pb2.UpdateResponse(record=update_record)
        state["cursor"] += 1
        log_message(WARNING, "Emitted individual UPDATE record")

        # DELETE
        delete_record = self._build_record(
            table="table1",
            record_type=common_pb2.RecordType.DELETE,
            data={"a1": self._make_string_value("a-2")},
        )
        yield connector_sdk_pb2.UpdateResponse(record=delete_record)
        state["cursor"] += 1
        log_message(WARNING, "Emitted individual DELETE record")

    def _emit_checkpoint(self, state: dict):
        checkpoint = connector_sdk_pb2.Checkpoint()
        checkpoint.state_json = json.dumps(state)
        yield connector_sdk_pb2.UpdateResponse(checkpoint=checkpoint)

    def Update(self, request, context):
        """
        Main update handler that combines batched and individual record emissions.
        """
        log_message(WARNING, "Sync Start")
        state = self._initialize_state(request)

        yield from self._emit_batched_records(state)

        yield from self._emit_individual_records(state)

        yield from self._emit_checkpoint(state)

        log_message(SEVERE, "Completed Update with batched + individual records")


def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_connector"}}')


def start_server():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051,
                        help="The server port")
    args = parser.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    connector_sdk_pb2_grpc.add_SourceConnectorServicer_to_server(ConnectorService(), server)
    server.add_insecure_port(f'[::]:{args.port}')
    server.start()
    print(f"Server started on port {args.port}...")
    server.wait_for_termination()
    print("Server terminated.")


if __name__ == '__main__':
    print("Starting the server...")
    start_server()