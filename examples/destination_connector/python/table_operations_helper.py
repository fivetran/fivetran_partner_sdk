import sys
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_pb2
from sdk_pb2 import common_pb2
import json

INFO = "INFO"
WARNING = "WARNING"


class TableOperationsHelper:
    """Helper class for table operations including CreateTable, AlterTable, Truncate, and DescribeTable."""

    def __init__(self, db_helper):
        self.db_helper = db_helper

    def create_table(self, request, default_schema):
        """
        Handle CreateTable operation.

        Args:
            request: CreateTableRequest from Fivetran
            default_schema: Default schema name

        Returns:
            CreateTableResponse with success or failure
        """
        schema_name = request.schema_name if request.schema_name else default_schema
        print(f"[CreateTable]: {schema_name} | {request.table.name} | {request.table.columns}")
        try:
            self.db_helper.create_table(schema_name, request.table)
            return destination_sdk_pb2.CreateTableResponse(success=True)
        except Exception as e:
            log_message(WARNING, f"CreateTable failed: {str(e)}")
            return destination_sdk_pb2.CreateTableResponse(success=False)

    def describe_table(self, request, default_schema):
        """
        Handle DescribeTable operation.

        Args:
            request: DescribeTableRequest from Fivetran
            default_schema: Default schema name

        Returns:
            DescribeTableResponse with table metadata or not_found flag
        """
        schema_name = request.schema_name if request.schema_name else default_schema
        log_message(INFO, f"Fetching table info for {schema_name}.{request.table_name}")
        try:
            table = self.db_helper.describe_table(schema_name, request.table_name)
            if table is None:
                return destination_sdk_pb2.DescribeTableResponse(not_found=True)
            else:
                return destination_sdk_pb2.DescribeTableResponse(not_found=False, table=table)
        except Exception as e:
            log_message(WARNING, f"DescribeTable failed: {str(e)}")
            return destination_sdk_pb2.DescribeTableResponse(not_found=True)

    def columns_have_different_types(self, col1, col2):
        """
        Compare two columns to determine if they have different types.
        For DECIMAL types, also compares precision and scale.
        For STRING types, also compares string_byte_length.

        Args:
            col1: First column to compare
            col2: Second column to compare

        Returns:
            True if columns have different types, False otherwise
        """
        # If base types are different, they definitely have different types
        if col1.type != col2.type:
            return True

        # Special handling for DECIMAL - check precision/scale
        if col1.type == common_pb2.DataType.DECIMAL:
            # Check if both have decimal parameters
            col1_has_params = col1.HasField("params")
            col2_has_params = col2.HasField("params")

            # If only one has params, they're different
            if col1_has_params != col2_has_params:
                return True

            # If both have params, check if they both have decimal
            if col1_has_params and col2_has_params:
                col1_has_decimal = col1.params.HasField("decimal")
                col2_has_decimal = col2.params.HasField("decimal")

                if col1_has_decimal != col2_has_decimal:
                    return True

                # If both have decimal params, compare precision and scale
                if col1_has_decimal and col2_has_decimal:
                    return (col1.params.decimal.precision != col2.params.decimal.precision or
                            col1.params.decimal.scale != col2.params.decimal.scale)

        # Special handling for STRING - check string_byte_length
        # Important for destinations with VARCHAR size limits (e.g., VARCHAR(255))
        if col1.type == common_pb2.DataType.STRING:
            # Get byte lengths (0 if not set, meaning unlimited VARCHAR)
            col1_byte_length = col1.params.string_byte_length if col1.HasField("params") else 0
            col2_byte_length = col2.params.string_byte_length if col2.HasField("params") else 0

            # Compare byte lengths
            if col1_byte_length != col2_byte_length:
                return True

        return False

    def alter_table(self, request, schema_name, default_schema):
        """
        Handle AlterTable operation including column additions, type changes,
        column drops, and primary key changes.

        Args:
            request: AlterTableRequest from Fivetran
            schema_name: Schema name (or default if not specified)
            default_schema: Default schema name

        Returns:
            AlterTableResponse with success or failure
        """
        schema_name = schema_name if schema_name else default_schema
        drop_columns = request.drop_columns
        log_message(INFO, f"[AlterTable]: {schema_name} | {request.table.name} | drop_columns={drop_columns} | columns={request.table.columns}")

        try:
            # Get current table schema
            current_table = self.db_helper.describe_table(schema_name, request.table.name)

            if current_table is None:
                log_message(WARNING, f"Table {schema_name}.{request.table.name} does not exist")
                return destination_sdk_pb2.AlterTableResponse(success=False)

            # Compare current and requested columns
            current_column_names = {col.name for col in current_table.columns}
            requested_column_names = {col.name for col in request.table.columns}

            # Build maps for easy lookup
            current_columns_map = {col.name: col for col in current_table.columns}
            requested_columns_map = {col.name: col for col in request.table.columns}

            # Find columns to add (in request but not in current)
            new_columns = [col for col in request.table.columns if col.name not in current_column_names]

            # Find columns to drop (in current but not in request)
            columns_to_drop = [col.name for col in current_table.columns if col.name not in requested_column_names]

            # Find columns with type changes (in both but different types)
            columns_with_type_changes = []
            for col_name in current_column_names & requested_column_names:
                current_col = current_columns_map[col_name]
                requested_col = requested_columns_map[col_name]
                if self.columns_have_different_types(current_col, requested_col):
                    columns_with_type_changes.append((col_name, requested_col))

            # Wrap all ALTER TABLE operations in a transaction for atomicity
            with self.db_helper.transaction():
                # Add new columns
                for column in new_columns:
                    self.db_helper.add_column(schema_name, request.table.name, column)
                    log_message(INFO, f"Added column: {column.name} to {schema_name}.{request.table.name}")

                # Handle type changes using DuckDB's native ALTER COLUMN
                for col_name, new_col_def in columns_with_type_changes:
                    log_message(INFO, f"Changing type for column: {col_name} to {new_col_def.type}")

                    escaped_schema = self.db_helper.escape_identifier(schema_name)
                    escaped_table = self.db_helper.escape_identifier(request.table.name)
                    escaped_col = self.db_helper.escape_identifier(col_name)
                    sql_type = self.db_helper.map_datatype_to_sql(new_col_def.type, new_col_def)

                    # Use DuckDB's native ALTER COLUMN SET DATA TYPE
                    sql = f'ALTER TABLE "{escaped_schema}"."{escaped_table}" ALTER COLUMN "{escaped_col}" SET DATA TYPE {sql_type}'
                    self.db_helper.get_connection().execute(sql)

                    log_message(INFO, f"Type change completed for column: {col_name}")

                # Handle primary key changes
                self._handle_primary_key_changes(schema_name, request.table.name, current_table, request.table)

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

    def _handle_primary_key_changes(self, schema_name, table_name, current_table, requested_table):
        """Handle primary key constraint changes."""
        current_pk_columns = [col.name for col in current_table.columns if col.primary_key]
        requested_pk_columns = [col.name for col in requested_table.columns if col.primary_key]

        # Check if primary key changed
        if set(current_pk_columns) != set(requested_pk_columns):
            log_message(INFO, f"Primary key change detected: {current_pk_columns} -> {requested_pk_columns}")

            escaped_schema = self.db_helper.escape_identifier(schema_name)
            escaped_table = self.db_helper.escape_identifier(table_name)

            # Drop existing primary key constraint if it exists
            if current_pk_columns:
                # Try to drop the primary key constraint
                # Note: This assumes DuckDB's default naming convention (table_name + "_pkey").
                # For production, consider querying information_schema.table_constraints to get
                # the actual constraint name, especially if table names contain special characters.
                constraint_name = f"{table_name}_pkey"
                escaped_constraint = self.db_helper.escape_identifier(constraint_name)
                try:
                    sql = f'ALTER TABLE "{escaped_schema}"."{escaped_table}" DROP CONSTRAINT "{escaped_constraint}"'
                    self.db_helper.get_connection().execute(sql)
                    log_message(INFO, f"Dropped primary key constraint: {constraint_name}")
                except Exception as e:
                    # Constraint might not exist or have different name, log but continue
                    log_message(WARNING, f"Could not drop primary key constraint: {str(e)}")

            # Add new primary key constraint if requested
            if requested_pk_columns:
                pk_cols_str = ", ".join([f'"{self.db_helper.escape_identifier(col)}"' for col in requested_pk_columns])
                sql = f'ALTER TABLE "{escaped_schema}"."{escaped_table}" ADD PRIMARY KEY ({pk_cols_str})'
                self.db_helper.get_connection().execute(sql)
                log_message(INFO, f"Added primary key constraint on columns: {requested_pk_columns}")

    def truncate_table(self, request, default_schema):
        """
        Handle Truncate operation (both hard and soft truncate).

        Args:
            request: TruncateRequest from Fivetran
            default_schema: Default schema name

        Returns:
            TruncateResponse with success or failure
        """
        schema_name = request.schema_name if request.schema_name else default_schema
        table_name = request.table_name
        print(f"[TruncateTable]: {schema_name} | {table_name} | soft={request.HasField('soft')}")

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


def log_message(level, message):
    escaped_message = json.dumps(message)
    print(f'{{"level": "{level}", "message": {escaped_message}, "message-origin": "sdk_destination"}}')
