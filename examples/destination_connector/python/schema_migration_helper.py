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

    def __init__(self, table_map):
        self.table_map = table_map

    def handle_drop(self, drop_op, schema, table):
        """Handles drop operations (drop table, drop column in history mode)."""
        entity_case = drop_op.WhichOneof("entity")

        if entity_case == "drop_table":
            # table-map manipulation to simulate drop, replace with actual logic.
            self.table_map.pop(table, None)

            log_message(INFO, f"[Migrate:Drop] Dropping table {schema}.{table}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "drop_column_in_history_mode":
            # IMPORTANT: In a real implementation, DO NOT physically drop the column from the table.
            # Instead, keep the column and set it to NULL for new records to maintain history.
            # This simplified example removes it from metadata for simulation purposes only.
            #
            # Real implementation should:
            # 1. Insert new rows with the column set to NULL and operation_timestamp
            # 2. Update previous active records' _fivetran_end and _fivetran_active
            # See schema-migration-helper-service.md for full implementation details.
            drop_column = drop_op.drop_column_in_history_mode
            table_obj = self.table_map.get(table)
            if table_obj:
                # Note: This removes column from metadata for example purposes only - don't do this in production
                columns_to_keep = [col for col in table_obj.columns if col.name != drop_column.column]
                del table_obj.columns[:]
                table_obj.columns.extend(columns_to_keep)

            log_message(INFO, f"[Migrate:DropColumnHistory] table={schema}.{table} column={drop_column.column} op_ts={drop_column.operation_timestamp}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Drop] No drop entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def handle_copy(self, copy_op, schema, table):
        """Handles copy operations (copy table, copy column, copy table to history mode)."""
        entity_case = copy_op.WhichOneof("entity")

        if entity_case == "copy_table":
            # table-map manipulation to simulate copy, replace with actual logic.
            copy_table = copy_op.copy_table
            if copy_table.from_table in self.table_map:
                self.table_map[copy_table.to_table] = self.table_map[copy_table.from_table]

            log_message(INFO, f"[Migrate:CopyTable] from={copy_table.from_table} to={copy_table.to_table} in schema={schema}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "copy_column":
            # table-map manipulation to simulate copy column, replace with actual logic.
            copy_column = copy_op.copy_column
            table_obj = self.table_map.get(table)
            if table_obj:
                for col in table_obj.columns:
                    if col.name == copy_column.from_column:
                        new_col = type(col)()
                        new_col.CopyFrom(col)
                        new_col.name = copy_column.to_column
                        table_obj.columns.add().CopyFrom(new_col)
                        break

            log_message(INFO, f"[Migrate:CopyColumn] table={schema}.{table} from_col={copy_column.from_column} to_col={copy_column.to_column}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "copy_table_to_history_mode":
            # table-map manipulation to simulate copy table to history mode, replace with actual logic.
            copy_table_history_mode = copy_op.copy_table_to_history_mode
            if copy_table_history_mode.from_table in self.table_map:
                from_table_obj = self.table_map[copy_table_history_mode.from_table]
                new_table = TableMetadataHelper.create_table_copy(from_table_obj, copy_table_history_mode.to_table)
                TableMetadataHelper.remove_column_from_table(new_table, copy_table_history_mode.soft_deleted_column)
                TableMetadataHelper.add_history_mode_columns(new_table)
                self.table_map[copy_table_history_mode.to_table] = new_table

            log_message(INFO, f"[Migrate:CopyTableToHistoryMode] from={copy_table_history_mode.from_table} to={copy_table_history_mode.to_table} soft_deleted_column={copy_table_history_mode.soft_deleted_column}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Copy] No copy entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def handle_rename(self, rename_op, schema, table):
        """Handles rename operations (rename table, rename column)."""
        entity_case = rename_op.WhichOneof("entity")

        if entity_case == "rename_table":
            # table-map manipulation to simulate rename, replace with actual logic.
            rt = rename_op.rename_table
            if rt.from_table in self.table_map:
                tbl = self.table_map.pop(rt.from_table)
                # Adjust name inside the Table metadata if needed
                tbl_copy = tbl.__class__.FromString(tbl.SerializeToString())
                if hasattr(tbl_copy, "name"):
                    tbl_copy.name = rt.to_table
                self.table_map[rt.to_table] = tbl_copy

            log_message(INFO, f"[Migrate:RenameTable] from={rt.from_table} to={rt.to_table} schema={schema}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "rename_column":
            # table-map manipulation to simulate rename column, replace with actual logic.
            rename_column = rename_op.rename_column
            table_obj = self.table_map.get(table)
            if table_obj:
                # Rename the column
                for col in table_obj.columns:
                    if col.name == rename_column.from_column:
                        col.name = rename_column.to_column
                        break

            log_message(INFO, f"[Migrate:RenameColumn] table={schema}.{table} from_col={rename_column.from_column} to_col={rename_column.to_column}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Rename] No rename entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def handle_add(self, add_op, schema, table):
        """Handles add operations (add column in history mode, add column with default value)."""
        entity_case = add_op.WhichOneof("entity")

        if entity_case == "add_column_in_history_mode":
            # table-map manipulation to simulate add column in history mode, replace with actual logic.
            add_col_history_mode = add_op.add_column_in_history_mode
            table_obj = self.table_map.get(table)
            if table_obj:
                new_col = table_obj.columns.add()
                new_col.name = add_col_history_mode.column
                new_col.type = add_col_history_mode.column_type

            log_message(INFO, f"[Migrate:AddColumnHistory] table={schema}.{table} column={add_col_history_mode.column} type={add_col_history_mode.column_type} default={add_col_history_mode.default_value} op_ts={add_col_history_mode.operation_timestamp}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif entity_case == "add_column_with_default_value":
            # table-map manipulation to simulate add column with default value, replace with actual logic.
            add_col_default_with_value = add_op.add_column_with_default_value
            table_obj = self.table_map.get(table)
            if table_obj:
                new_col = table_obj.columns.add()
                new_col.name = add_col_default_with_value.column
                new_col.type = add_col_default_with_value.column_type

            log_message(INFO, f"[Migrate:AddColumnDefault] table={schema}.{table} column={add_col_default_with_value.column} type={add_col_default_with_value.column_type} default={add_col_default_with_value.default_value}")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, "[Migrate:Add] No add entity specified")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)

    def handle_update_column_value(self, upd, schema, table):
        """Handles update column value operation."""
        # Placeholder: Update all existing rows' column value.

        log_message(INFO, f"[Migrate:UpdateColumnValue] table={schema}.{table} column={upd.column} value={upd.value}")
        return destination_sdk_pb2.MigrateResponse(success=True)

    def handle_table_sync_mode_migration(self, op, schema, table):
        """Handles table sync mode migration operations."""
        table_obj = self.table_map.get(table)

        soft_deleted_column = op.soft_deleted_column if op.HasField("soft_deleted_column") else None

        # Determine the migration type and handle accordingly
        if op.type == destination_sdk_pb2.TableSyncModeMigrationType.SOFT_DELETE_TO_LIVE:
            # table-map manipulation to simulate soft delete to live, replace with actual logic.
            table_copy = TableMetadataHelper.create_table_copy(table_obj, table_obj.name)
            TableMetadataHelper.remove_column_from_table(table_copy, soft_deleted_column)
            self.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from SOFT_DELETE to LIVE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.SOFT_DELETE_TO_HISTORY:
            # table-map manipulation to simulate soft delete to history, replace with actual logic.
            table_copy = TableMetadataHelper.create_table_copy(table_obj, table_obj.name)
            TableMetadataHelper.remove_column_from_table(table_copy, soft_deleted_column)
            TableMetadataHelper.add_history_mode_columns(table_copy)
            self.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from SOFT_DELETE to HISTORY")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.HISTORY_TO_SOFT_DELETE:
            # table-map manipulation to simulate history to soft delete, replace with actual logic.
            table_copy = TableMetadataHelper.create_table_copy(table_obj, table_obj.name)
            TableMetadataHelper.remove_history_mode_columns(table_copy)
            TableMetadataHelper.add_soft_delete_column(table_copy, soft_deleted_column)
            self.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from HISTORY to SOFT_DELETE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.HISTORY_TO_LIVE:
            # table-map manipulation to simulate history to live, replace with actual logic.
            table_copy = TableMetadataHelper.create_table_copy(table_obj, table_obj.name)
            TableMetadataHelper.remove_history_mode_columns(table_copy)
            self.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from HISTORY to LIVE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.LIVE_TO_SOFT_DELETE:
            # table-map manipulation to simulate live to soft delete, replace with actual logic.
            table_copy = TableMetadataHelper.create_table_copy(table_obj, table_obj.name)
            TableMetadataHelper.add_soft_delete_column(table_copy, soft_deleted_column)
            self.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from LIVE to SOFT_DELETE")
            return destination_sdk_pb2.MigrateResponse(success=True)

        elif op.type == destination_sdk_pb2.TableSyncModeMigrationType.LIVE_TO_HISTORY:
            # table-map manipulation to simulate live to history, replace with actual logic.
            table_copy = TableMetadataHelper.create_table_copy(table_obj, table_obj.name)
            TableMetadataHelper.add_history_mode_columns(table_copy)
            self.table_map[table] = table_copy

            log_message(INFO, f"[Migrate:TableSyncModeMigration] Migrating table={schema}.{table} from LIVE to HISTORY")
            return destination_sdk_pb2.MigrateResponse(success=True)

        else:
            log_message(WARNING, f"[Migrate:TableSyncModeMigration] Unknown migration type for table={schema}.{table}")
            return destination_sdk_pb2.MigrateResponse(unsupported=True)


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


def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_destination"}}')
