import sys
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_pb2
from sdk_pb2 import common_pb2

# Constants for system columns
FIVETRAN_START = "_fivetran_start"
FIVETRAN_END = "_fivetran_end"
FIVETRAN_ACTIVE = "_fivetran_active"

INFO = "INFO"
WARNING = "WARNING"


class SchemaMigrationHelper:
    """Helper class for handling migration operations"""

    def __init__(self, db_helper):
        self.db_helper = db_helper

    def handle_drop(self, drop_op, schema, table):
        """Handles drop operations (drop table, drop column in history mode)."""
        entity_case = drop_op.WhichOneof("entity")

        try:
            if entity_case == "drop_table":
                self.db_helper.drop_table(schema, table)
                log_message(INFO, f"[Migrate:Drop] Dropping table {schema}.{table}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif entity_case == "drop_column_in_history_mode":
                drop_column = drop_op.drop_column_in_history_mode
                self.db_helper.drop_column(schema, table, drop_column.column)
                log_message(INFO, f"[Migrate:DropColumnHistory] table={schema}.{table} column={drop_column.column} op_ts={drop_column.operation_timestamp}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            else:
                log_message(WARNING, "[Migrate:Drop] No drop entity specified")
                return destination_sdk_pb2.MigrateResponse(unsupported=True)
        except Exception as e:
            log_message(WARNING, f"[Migrate:Drop] Failed: {str(e)}")
            return destination_sdk_pb2.MigrateResponse(success=False)

    def handle_copy(self, copy_op, schema, table):
        """Handles copy operations (copy table, copy column, copy table to history mode)."""
        entity_case = copy_op.WhichOneof("entity")

        try:
            if entity_case == "copy_table":
                copy_table = copy_op.copy_table
                self.db_helper.copy_table(schema, copy_table.from_table, copy_table.to_table)
                log_message(INFO, f"[Migrate:CopyTable] from={copy_table.from_table} to={copy_table.to_table} in schema={schema}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif entity_case == "copy_column":
                copy_column = copy_op.copy_column
                # Get table schema to find the column type
                table_obj = self.db_helper.describe_table(schema, table)
                if table_obj:
                    for col in table_obj.columns:
                        if col.name == copy_column.from_column:
                            # Add new column with same type
                            new_col = common_pb2.Column(name=copy_column.to_column, type=col.type)
                            self.db_helper.add_column(schema, table, new_col)
                            # Copy data from old column to new column using escaped identifiers
                            escaped_schema = self.db_helper._escape_identifier(schema)
                            escaped_table = self.db_helper._escape_identifier(table)
                            escaped_to_col = self.db_helper._escape_identifier(copy_column.to_column)
                            escaped_from_col = self.db_helper._escape_identifier(copy_column.from_column)
                            sql = f'UPDATE "{escaped_schema}"."{escaped_table}" SET "{escaped_to_col}" = "{escaped_from_col}"'
                            self.db_helper.connection.execute(sql)
                            break

                log_message(INFO, f"[Migrate:CopyColumn] table={schema}.{table} from_col={copy_column.from_column} to_col={copy_column.to_column}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif entity_case == "copy_table_to_history_mode":
                copy_table_history_mode = copy_op.copy_table_to_history_mode
                from_table_obj = self.db_helper.describe_table(schema, copy_table_history_mode.from_table)

                if from_table_obj:
                    # Create new table metadata without soft delete column and with history columns
                    new_table = TableMetadataHelper.create_table_copy(from_table_obj, copy_table_history_mode.to_table)
                    TableMetadataHelper.remove_column_from_table(new_table, copy_table_history_mode.soft_deleted_column)
                    TableMetadataHelper.add_history_mode_columns(new_table)

                    # Create the new table in DuckDB
                    self.db_helper.create_table(schema, new_table)

                    # Copy data (excluding soft deleted column) with escaped identifiers
                    columns_to_copy = [col.name for col in new_table.columns
                                      if col.name not in [FIVETRAN_START, FIVETRAN_END, FIVETRAN_ACTIVE]]
                    columns_str = ", ".join([f'"{self.db_helper._escape_identifier(col)}"' for col in columns_to_copy])
                    escaped_schema = self.db_helper._escape_identifier(schema)
                    escaped_to_table = self.db_helper._escape_identifier(copy_table_history_mode.to_table)
                    escaped_from_table = self.db_helper._escape_identifier(copy_table_history_mode.from_table)
                    sql = f'INSERT INTO "{escaped_schema}"."{escaped_to_table}" ({columns_str}) SELECT {columns_str} FROM "{escaped_schema}"."{escaped_from_table}"'
                    self.db_helper.connection.execute(sql)

                log_message(INFO, f"[Migrate:CopyTableToHistoryMode] from={copy_table_history_mode.from_table} to={copy_table_history_mode.to_table} soft_deleted_column={copy_table_history_mode.soft_deleted_column}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            else:
                log_message(WARNING, "[Migrate:Copy] No copy entity specified")
                return destination_sdk_pb2.MigrateResponse(unsupported=True)
        except Exception as e:
            log_message(WARNING, f"[Migrate:Copy] Failed: {str(e)}")
            return destination_sdk_pb2.MigrateResponse(success=False)

    def handle_rename(self, rename_op, schema, table):
        """Handles rename operations (rename table, rename column)."""
        entity_case = rename_op.WhichOneof("entity")

        try:
            if entity_case == "rename_table":
                rt = rename_op.rename_table
                self.db_helper.rename_table(schema, rt.from_table, rt.to_table)
                log_message(INFO, f"[Migrate:RenameTable] from={rt.from_table} to={rt.to_table} schema={schema}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif entity_case == "rename_column":
                rename_column = rename_op.rename_column
                self.db_helper.rename_column(schema, table, rename_column.from_column, rename_column.to_column)
                log_message(INFO, f"[Migrate:RenameColumn] table={schema}.{table} from_col={rename_column.from_column} to_col={rename_column.to_column}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            else:
                log_message(WARNING, "[Migrate:Rename] No rename entity specified")
                return destination_sdk_pb2.MigrateResponse(unsupported=True)
        except Exception as e:
            log_message(WARNING, f"[Migrate:Rename] Failed: {str(e)}")
            return destination_sdk_pb2.MigrateResponse(success=False)

    def handle_add(self, add_op, schema, table):
        """Handles add operations (add column in history mode, add column with default value)."""
        entity_case = add_op.WhichOneof("entity")

        try:
            if entity_case == "add_column_in_history_mode":
                add_col_history_mode = add_op.add_column_in_history_mode
                new_col = common_pb2.Column(
                    name=add_col_history_mode.column,
                    type=add_col_history_mode.column_type
                )
                self.db_helper.add_column(schema, table, new_col)

                # If default value is provided, update existing rows
                if add_col_history_mode.HasField("default_value"):
                    self.db_helper.update_column_value(schema, table, add_col_history_mode.column, add_col_history_mode.default_value)

                log_message(INFO, f"[Migrate:AddColumnHistory] table={schema}.{table} column={add_col_history_mode.column} type={add_col_history_mode.column_type} default={add_col_history_mode.default_value} op_ts={add_col_history_mode.operation_timestamp}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif entity_case == "add_column_with_default_value":
                add_col_default_with_value = add_op.add_column_with_default_value
                new_col = common_pb2.Column(
                    name=add_col_default_with_value.column,
                    type=add_col_default_with_value.column_type
                )
                self.db_helper.add_column(schema, table, new_col)

                # Update existing rows with default value
                if add_col_default_with_value.HasField("default_value"):
                    self.db_helper.update_column_value(schema, table, add_col_default_with_value.column, add_col_default_with_value.default_value)

                log_message(INFO, f"[Migrate:AddColumnDefault] table={schema}.{table} column={add_col_default_with_value.column} type={add_col_default_with_value.column_type} default={add_col_default_with_value.default_value}")
                return destination_sdk_pb2.MigrateResponse(success=True)

            else:
                log_message(WARNING, "[Migrate:Add] No add entity specified")
                return destination_sdk_pb2.MigrateResponse(unsupported=True)
        except Exception as e:
            log_message(WARNING, f"[Migrate:Add] Failed: {str(e)}")
            return destination_sdk_pb2.MigrateResponse(success=False)

    def handle_update_column_value(self, upd, schema, table):
        """Handles update column value operation."""
        try:
            self.db_helper.update_column_value(schema, table, upd.column, upd.value)
            log_message(INFO, f"[Migrate:UpdateColumnValue] table={schema}.{table} column={upd.column} value={upd.value}")
            return destination_sdk_pb2.MigrateResponse(success=True)
        except Exception as e:
            log_message(WARNING, f"[Migrate:UpdateColumnValue] Failed: {str(e)}")
            return destination_sdk_pb2.MigrateResponse(success=False)

    def handle_table_sync_mode_migration(self, op, schema, table):
        """Handles table sync mode migration operations."""
        soft_deleted_column = op.soft_deleted_column if op.HasField("soft_deleted_column") else None

        try:
            # Determine the migration type and handle accordingly
            if op.type == destination_sdk_pb2.TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE:
                # Remove soft delete column
                if soft_deleted_column:
                    self.db_helper.drop_column(schema, table, soft_deleted_column)
                log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from SOFT_DELETE to LIVE")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY:
                # Remove soft delete column and add history mode columns
                if soft_deleted_column:
                    self.db_helper.drop_column(schema, table, soft_deleted_column)
                TableMetadataHelper.add_history_mode_columns_to_db(self.db_helper, schema, table)
                log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from SOFT_DELETE to HISTORY")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.HISTORY_TO_SOFT_DELETE:
                # Remove history mode columns and add soft delete column
                TableMetadataHelper.remove_history_mode_columns_from_db(self.db_helper, schema, table)
                if soft_deleted_column:
                    TableMetadataHelper.add_soft_delete_column_to_db(self.db_helper, schema, table, soft_deleted_column)
                log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from HISTORY to SOFT_DELETE")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.HISTORY_TO_LIVE:
                # Remove history mode columns
                TableMetadataHelper.remove_history_mode_columns_from_db(self.db_helper, schema, table)
                log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from HISTORY to LIVE")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE:
                # Add soft delete column
                if soft_deleted_column:
                    TableMetadataHelper.add_soft_delete_column_to_db(self.db_helper, schema, table, soft_deleted_column)
                log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from LIVE to SOFT_DELETE")
                return destination_sdk_pb2.MigrateResponse(success=True)

            elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.LIVE_TO_HISTORY:
                # Add history mode columns
                TableMetadataHelper.add_history_mode_columns_to_db(self.db_helper, schema, table)
                log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from LIVE to HISTORY")
                return destination_sdk_pb2.MigrateResponse(success=True)

            else:
                log_message(WARNING, f"[Migrate:TableSyncModeMigration] Unknown migration type for table={schema}.{table}")
                return destination_sdk_pb2.MigrateResponse(unsupported=True)
        except Exception as e:
            log_message(WARNING, f"[Migrate:TableSyncModeMigration] Failed: {str(e)}")
            return destination_sdk_pb2.MigrateResponse(success=False)


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
    def remove_history_mode_columns(table_obj):
        """Removes history mode columns from a table."""
        if not hasattr(table_obj, 'columns'):
            return
        columns_to_keep = [
            col for col in table_obj.columns
            if col.name not in [FIVETRAN_START, FIVETRAN_END, FIVETRAN_ACTIVE]
        ]
        del table_obj.columns[:]
        table_obj.columns.extend(columns_to_keep)

    @staticmethod
    def add_history_mode_columns(table_obj):
        """Adds history mode columns to a table."""
        if not hasattr(table_obj, 'columns'):
            return
        start_col = table_obj.columns.add()
        start_col.name = FIVETRAN_START
        start_col.type =common_pb2.DataType.UTC_DATETIME

        end_col = table_obj.columns.add()
        end_col.name = FIVETRAN_END
        end_col.type = common_pb2.DataType.UTC_DATETIME

        active_col = table_obj.columns.add()
        active_col.name = FIVETRAN_ACTIVE
        active_col.type = common_pb2.DataType.BOOLEAN

    @staticmethod
    def add_soft_delete_column(table_obj, column_name):
        """Adds a soft delete column to a table."""
        if not column_name or not hasattr(table_obj, 'columns'):
            return
        soft_del_col = table_obj.columns.add()
        soft_del_col.name = column_name
        soft_del_col.type = common_pb2.DataType.BOOLEAN

    @staticmethod
    def add_history_mode_columns_to_db(db_helper, schema, table):
        """Adds history mode columns to a table in the database."""
        start_col = common_pb2.Column(name=FIVETRAN_START, type=common_pb2.DataType.UTC_DATETIME)
        end_col = common_pb2.Column(name=FIVETRAN_END, type=common_pb2.DataType.UTC_DATETIME)
        active_col = common_pb2.Column(name=FIVETRAN_ACTIVE, type=common_pb2.DataType.BOOLEAN)

        db_helper.add_column(schema, table, start_col)
        db_helper.add_column(schema, table, end_col)
        db_helper.add_column(schema, table, active_col)

    @staticmethod
    def remove_history_mode_columns_from_db(db_helper, schema, table):
        """Removes history mode columns from a table in the database."""
        try:
            db_helper.drop_column(schema, table, FIVETRAN_START)
        except Exception as e:
            log_message(WARNING, f"Failed to drop column {FIVETRAN_START}: {str(e)}")
        try:
            db_helper.drop_column(schema, table, FIVETRAN_END)
        except Exception as e:
            log_message(WARNING, f"Failed to drop column {FIVETRAN_END}: {str(e)}")
        try:
            db_helper.drop_column(schema, table, FIVETRAN_ACTIVE)
        except Exception as e:
            log_message(WARNING, f"Failed to drop column {FIVETRAN_ACTIVE}: {str(e)}")

    @staticmethod
    def add_soft_delete_column_to_db(db_helper, schema, table, column_name):
        """Adds a soft delete column to a table in the database."""
        if column_name:
            soft_del_col = common_pb2.Column(name=column_name, type=common_pb2.DataType.BOOLEAN)
            db_helper.add_column(schema, table, soft_del_col)


def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_destination"}}')
