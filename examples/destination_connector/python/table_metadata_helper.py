import sys
sys.path.append('sdk_pb2')

from sdk_pb2 import common_pb2

# Constants for system columns
FIVETRAN_START = "_fivetran_start"
FIVETRAN_END = "_fivetran_end"
FIVETRAN_ACTIVE = "_fivetran_active"

INFO = "INFO"
WARNING = "WARNING"


class TableMetadataHelper:
    """Helper class for table metadata operations"""

    @staticmethod
    def create_table_copy(table_obj, new_name):
        """Creates a copy of a table."""
        table_copy = table_obj.__class__.FromString(table_obj.SerializeToString())
        if hasattr(table_copy, "name"):
            table_copy.name = new_name
        return table_copy

    @staticmethod
    def remove_column_from_table(table_obj, column_name):
        """Removes a column from a table."""
        if not column_name or not hasattr(table_obj, 'columns'):
            return
        # Create a new list of columns excluding the specified column
        columns_to_keep = [col for col in table_obj.columns if col.name != column_name]
        # Clear and repopulate
        del table_obj.columns[:]
        table_obj.columns.extend(columns_to_keep)

    @staticmethod
    def add_history_mode_columns(table_obj):
        """Adds history mode columns to a table."""
        if not hasattr(table_obj, 'columns'):
            return
        start_col = table_obj.columns.add()
        start_col.name = FIVETRAN_START
        start_col.type = common_pb2.DataType.UTC_DATETIME

        end_col = table_obj.columns.add()
        end_col.name = FIVETRAN_END
        end_col.type = common_pb2.DataType.UTC_DATETIME

        active_col = table_obj.columns.add()
        active_col.name = FIVETRAN_ACTIVE
        active_col.type = common_pb2.DataType.BOOLEAN

    @staticmethod
    def add_history_mode_columns_to_db(db_helper, schema, table):
        """Adds history mode columns to a table in the database."""
        columns_to_add = [
            common_pb2.Column(name=FIVETRAN_START, type=common_pb2.DataType.UTC_DATETIME),
            common_pb2.Column(name=FIVETRAN_END, type=common_pb2.DataType.UTC_DATETIME),
            common_pb2.Column(name=FIVETRAN_ACTIVE, type=common_pb2.DataType.BOOLEAN)
        ]

        for column in columns_to_add:
            try:
                db_helper.add_column(schema, table, column)
            except Exception as e:
                log_message(WARNING, f"Failed to add column {column.name} to {schema}.{table}: {str(e)}")

    @staticmethod
    def remove_history_mode_columns_from_db(db_helper, schema, table):
        """Removes history mode columns from a table in the database."""
        columns_to_drop = [FIVETRAN_START, FIVETRAN_END, FIVETRAN_ACTIVE]

        for column in columns_to_drop:
            try:
                db_helper.drop_column(schema, table, column)
            except Exception as e:
                log_message(WARNING, f"Failed to drop column {column} from {schema}.{table}: {str(e)}")

    @staticmethod
    def add_soft_delete_column_to_db(db_helper, schema, table, column_name):
        """Adds a soft delete column to a table in the database."""
        if column_name:
            soft_del_col = common_pb2.Column(name=column_name, type=common_pb2.DataType.BOOLEAN)
            db_helper.add_column(schema, table, soft_del_col)


def log_message(level, message):
    import json
    log_entry = {"level": level, "message": json.dumps(message), "message-origin": "sdk_destination"}
    print(json.dumps(log_entry))
