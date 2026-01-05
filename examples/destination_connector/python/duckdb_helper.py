import duckdb
import sys
sys.path.append('sdk_pb2')

from sdk_pb2 import common_pb2

INFO = "INFO"
WARNING = "WARNING"


class DuckDBHelper:
    """Helper class for DuckDB operations."""

    def __init__(self, db_path=""):
        """
        Initialize DuckDB connection.

        Args:
            db_path: Path to database file. If empty, creates in-memory database.
        """
        self.db_path = db_path if db_path else ":memory:"
        self._connection = duckdb.connect(self.db_path)
        log_message(INFO, f"Connected to DuckDB at: {self.db_path}")

    def get_connection(self):
        """Get the DuckDB connection."""
        return self._connection

    def escape_identifier(self, identifier):
        """
        Safely escape an identifier for use in SQL queries.
        Doubles any double-quotes in the identifier to escape them.

        This is a public method as it's needed by external callers
        (e.g., schema_migration_helper.py, main.py) to build SQL statements.

        Args:
            identifier: The identifier to escape

        Returns:
            Escaped identifier safe for use in quoted SQL identifiers
        """
        if identifier is None:
            raise ValueError("Identifier cannot be None")
        # Escape double quotes by doubling them
        return str(identifier).replace('"', '""')

    def close(self):
        """Close the DuckDB connection."""
        conn = getattr(self, "_connection", None)
        if not conn:
            return
        try:
            conn.close()
            log_message(INFO, "DuckDB connection closed")
        except Exception as e:
            # Log a warning but do not raise, to allow cleanup to continue
            log_message(WARNING, f"Error while closing DuckDB connection: {e}")
        finally:
            self._connection = None

    def table_exists(self, schema_name, table_name):
        """Check if a table exists in the given schema."""
        query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
        """
        result = self._connection.execute(query, [schema_name, table_name]).fetchone()
        return result[0] > 0

    def create_schema_if_not_exists(self, schema_name):
        """Create a schema if it doesn't exist."""
        sql = f'CREATE SCHEMA IF NOT EXISTS "{self.escape_identifier(schema_name)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Schema created or already exists: {schema_name}")

    def create_table(self, schema_name, table):
        """Create a table with the given schema."""
        self.create_schema_if_not_exists(schema_name)

        column_defs = []
        for column in table.columns:
            column_def = f'"{self.escape_identifier(column.name)}" {self.map_datatype_to_sql(column.type, column)}'
            column_defs.append(column_def)

        columns_str = ", ".join(column_defs)
        sql = f'CREATE TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table.name)}" ({columns_str})'

        self._connection.execute(sql)
        log_message(INFO, f"Table created: {schema_name}.{table.name}")

    def drop_table(self, schema_name, table_name):
        """Drop a table if it exists."""
        sql = f'DROP TABLE IF EXISTS "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table_name)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Table dropped: {schema_name}.{table_name}")

    def describe_table(self, schema_name, table_name):
        """Get table schema information."""
        if not self.table_exists(schema_name, table_name):
            return None

        query = """
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
        """

        result = self._connection.execute(query, [schema_name, table_name]).fetchall()

        # Build table object
        table_builder_columns = []

        for row in result:
            column_name = row[0]
            data_type = row[1]
            numeric_precision = row[2]
            numeric_scale = row[3]

            column_type = self._map_sql_type_to_datatype(data_type)
            column = common_pb2.Column(
                name=column_name,
                type=column_type
            )

            # For DECIMAL types, populate precision and scale
            if column_type == common_pb2.DataType.DECIMAL and numeric_precision is not None:
                column.params.decimal.precision = int(numeric_precision)
                column.params.decimal.scale = int(numeric_scale) if numeric_scale is not None else 0

            table_builder_columns.append(column)

        table_obj = common_pb2.Table(
            name=table_name,
            columns=table_builder_columns
        )

        return table_obj

    def add_column(self, schema_name, table_name, column):
        """Add a column to an existing table."""
        sql = f'ALTER TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table_name)}" ADD COLUMN "{self.escape_identifier(column.name)}" {self.map_datatype_to_sql(column.type, column)}'
        self._connection.execute(sql)
        log_message(INFO, f"Column added: {column.name} to {schema_name}.{table_name}")

    def drop_column(self, schema_name, table_name, column_name):
        """Drop a column from a table."""
        sql = f'ALTER TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table_name)}" DROP COLUMN "{self.escape_identifier(column_name)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Column dropped: {column_name} from {schema_name}.{table_name}")

    def rename_column(self, schema_name, table_name, old_name, new_name):
        """Rename a column."""
        sql = f'ALTER TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table_name)}" RENAME COLUMN "{self.escape_identifier(old_name)}" TO "{self.escape_identifier(new_name)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Column renamed: {old_name} to {new_name} in {schema_name}.{table_name}")

    def truncate_table(self, schema_name, table_name):
        """Truncate a table."""
        sql = f'TRUNCATE TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table_name)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Table truncated: {schema_name}.{table_name}")

    def rename_table(self, schema_name, old_name, new_name):
        """Rename a table."""
        sql = f'ALTER TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(old_name)}" RENAME TO "{self.escape_identifier(new_name)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Table renamed: {old_name} to {new_name} in {schema_name}")

    def copy_table(self, schema_name, from_table, to_table):
        """Copy a table structure and data."""
        sql = f'CREATE TABLE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(to_table)}" AS SELECT * FROM "{self.escape_identifier(schema_name)}"."{self.escape_identifier(from_table)}"'
        self._connection.execute(sql)
        log_message(INFO, f"Table copied: {from_table} to {to_table} in {schema_name}")

    def _normalize_value(self, value):
        """
        Normalize a Python value for safe binding into DuckDB.

        Allows None (NULL) and common primitive types directly. Falls back
        to string representation for unsupported types to avoid driver errors.
        """
        if value is None:
            return None
        if isinstance(value, (int, float, bool, str, bytes)):
            return value
        # Fallback: use string representation for other types
        return str(value)

    def update_column_value(self, schema_name, table_name, column_name, value):
        """Update all rows in a column with a specific value."""
        sql = f'UPDATE "{self.escape_identifier(schema_name)}"."{self.escape_identifier(table_name)}" SET "{self.escape_identifier(column_name)}" = ?'
        normalized_value = self._normalize_value(value)
        self._connection.execute(sql, [normalized_value])
        log_message(INFO, f"Column {column_name} updated in {schema_name}.{table_name}")

    def map_datatype_to_sql(self, datatype, column=None):
        """
        Map Fivetran DataType to SQL type.

        This is a public method as it's needed by external callers
        for type conversion operations.

        Args:
            datatype: The Fivetran DataType enum value
            column: Optional Column object containing additional type parameters (e.g., decimal precision/scale)

        Returns:
            SQL type string
        """
        # Handle DECIMAL specially - needs precision/scale from column definition
        if datatype == common_pb2.DataType.DECIMAL:
            if column and column.HasField("params") and column.params.HasField("decimal"):
                precision = column.params.decimal.precision
                scale = column.params.decimal.scale
                return f"DECIMAL({precision}, {scale})"
            else:
                # Default fallback when precision/scale not specified
                return "DECIMAL(38, 10)"

        type_mapping = {
            common_pb2.DataType.BOOLEAN: "BOOLEAN",
            common_pb2.DataType.SHORT: "SMALLINT",
            common_pb2.DataType.INT: "INTEGER",
            common_pb2.DataType.LONG: "BIGINT",
            common_pb2.DataType.FLOAT: "REAL",
            common_pb2.DataType.DOUBLE: "DOUBLE",
            common_pb2.DataType.NAIVE_DATE: "DATE",
            common_pb2.DataType.NAIVE_DATETIME: "TIMESTAMP",
            common_pb2.DataType.UTC_DATETIME: "TIMESTAMPTZ",
            common_pb2.DataType.NAIVE_TIME: "TIME",
            common_pb2.DataType.BINARY: "BLOB",
            common_pb2.DataType.STRING: "VARCHAR",
            common_pb2.DataType.JSON: "JSON",
            common_pb2.DataType.XML: "VARCHAR",
            common_pb2.DataType.UNSPECIFIED: "VARCHAR",
        }
        return type_mapping.get(datatype, "VARCHAR")

    def _map_sql_type_to_datatype(self, sql_type):
        """Map SQL type to Fivetran DataType."""
        sql_type = sql_type.upper()

        # Log the SQL type for debugging
        log_message(INFO, f"Mapping SQL type: {sql_type}")

        if "BOOL" in sql_type:
            return common_pb2.DataType.BOOLEAN
        if "SMALLINT" in sql_type or "INT2" in sql_type:
            return common_pb2.DataType.SHORT
        if "INTEGER" in sql_type or "INT4" in sql_type or "INT " in sql_type:
            return common_pb2.DataType.INT
        if "BIGINT" in sql_type or "INT8" in sql_type:
            return common_pb2.DataType.LONG
        if "DECIMAL" in sql_type or "NUMERIC" in sql_type:
            return common_pb2.DataType.DECIMAL
        # Check DOUBLE before FLOAT because "DOUBLE" contains the word but not "FLOAT"
        if "DOUBLE" in sql_type or "FLOAT8" in sql_type:
            return common_pb2.DataType.DOUBLE
        # Check for FLOAT - covers FLOAT, REAL, FLOAT4
        if "FLOAT" in sql_type or "REAL" in sql_type or "FLOAT4" in sql_type:
            return common_pb2.DataType.FLOAT
        if "DATE" in sql_type and "TIME" not in sql_type:
            return common_pb2.DataType.NAIVE_DATE
        if "TIMESTAMP" in sql_type and "TZ" in sql_type:
            return common_pb2.DataType.UTC_DATETIME
        if "TIMESTAMP" in sql_type:
            return common_pb2.DataType.NAIVE_DATETIME
        if "TIME" in sql_type:
            return common_pb2.DataType.NAIVE_TIME
        if "BLOB" in sql_type or "BYTEA" in sql_type:
            return common_pb2.DataType.BINARY
        if "JSON" in sql_type:
            return common_pb2.DataType.JSON

        # If no match found, log warning and return STRING as fallback
        log_message(WARNING, f"Unknown SQL type '{sql_type}', defaulting to STRING")
        return common_pb2.DataType.STRING


def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_destination"}}')
