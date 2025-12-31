package destination;

import fivetran_sdk.v2.*;

import java.util.Map;
import java.util.logging.Logger;

public class SchemaMigrationHelper {
    private static final Logger logger = Logger.getLogger(SchemaMigrationHelper.class.getName());
    private final Map<String, Table> tableMap;
    private final TableMetadataHelper metadataHelper;

    public SchemaMigrationHelper(Map<String, Table> tableMap) {
        this.tableMap = tableMap;
        this.metadataHelper = new TableMetadataHelper();
    }

    public MigrateResponse.Builder handleDrop(DropOperation dropOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        switch (dropOp.getEntityCase()) {
            case DROP_TABLE:
                // table-map manipulation to simulate drop, replace with actual logic.
                tableMap.remove(table);

                logger.info(String.format("[Migrate:Drop] Dropping table %s.%s", schema, table));
                respBuilder.setSuccess(true);
                break;
            case DROP_COLUMN_IN_HISTORY_MODE:
                // IMPORTANT: DO NOT physically drop the column from the table.
                // The column must remain in the table structure to preserve historical data.
                //
                // Real implementation should:
                // 1. Insert new rows with the column set to NULL and operation_timestamp
                // 2. Update previous active records' _fivetran_end and _fivetran_active
                // See schema-migration-helper-service.md for full implementation details.
                DropColumnInHistoryMode dropColumn = dropOp.getDropColumnInHistoryMode();
                // The column remains in the table - no metadata changes needed

                logger.info(String.format("[Migrate:DropColumnHistory] table=%s.%s column=%s op_ts=%s",
                        schema, table, dropColumn.getColumn(), dropColumn.getOperationTimestamp()));
                respBuilder.setSuccess(true);
                break;
            case ENTITY_NOT_SET:
            default:
                logger.warning("[Migrate:Drop] No drop entity specified");
                respBuilder.setUnsupported(true);
        }
        return respBuilder;
    }

    public MigrateResponse.Builder handleCopy(CopyOperation copyOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        switch (copyOp.getEntityCase()) {
            case COPY_TABLE:
                // table-map manipulation to simulate copy, replace with actual logic.
                CopyTable copyTable = copyOp.getCopyTable();
                tableMap.put(copyTable.getToTable(), tableMap.get(copyTable.getFromTable()));

                logger.info(String.format("[Migrate:CopyTable] from=%s to=%s in schema=%s",
                        copyTable.getFromTable(), copyTable.getToTable(), schema));
                respBuilder.setSuccess(true);
                break;
            case COPY_COLUMN:
                // table-map manipulation to simulate copy column, replace with actual logic.
                CopyColumn copyColumn = copyOp.getCopyColumn();
                Table tableObj = tableMap.get(table);
                for (Column col : tableObj.getColumnsList()) {
                    if (col.getName().equals(copyColumn.getFromColumn())) {
                        Column newCol = col.toBuilder().setName(copyColumn.getToColumn()).build();
                        Table updatedTable = tableObj.toBuilder().addColumns(newCol).build();
                        tableMap.put(table, updatedTable);
                        break;
                    }
                }

                logger.info(String.format("[Migrate:CopyColumn] table=%s.%s from_col=%s to_col=%s",
                        schema, table, copyColumn.getFromColumn(), copyColumn.getToColumn()));
                respBuilder.setSuccess(true);
                break;
            case COPY_TABLE_TO_HISTORY_MODE:
                // table-map manipulation to simulate copy table to history mode, replace with actual logic.
                CopyTableToHistoryMode copyTableToHistoryMode = copyOp.getCopyTableToHistoryMode();
                String softDeletedColumn = copyTableToHistoryMode.getSoftDeletedColumn();
                String toTable = copyTableToHistoryMode.getToTable();
                String fromTable = copyTableToHistoryMode.getFromTable();
                Table.Builder newTable = Table.newBuilder()
                        .setName(toTable)
                        .addAllColumns(tableMap.get(fromTable).getColumnsList());
                metadataHelper.removeColumnFromBuilder(newTable, tableMap.get(fromTable), softDeletedColumn);
                metadataHelper.addHistoryModeColumns(newTable);
                tableMap.put(toTable, newTable.build());

                logger.info(String.format("[Migrate:CopyTableToHistoryMode] from=%s to=%s soft_deleted_column=%s",
                        copyTableToHistoryMode.getFromTable(), copyTableToHistoryMode.getToTable(), copyTableToHistoryMode.getSoftDeletedColumn()));
                respBuilder.setSuccess(true);
                break;
            case ENTITY_NOT_SET:
            default:
                logger.warning("[Migrate:Copy] No copy entity specified");
                respBuilder.setUnsupported(true);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleRename(RenameOperation renameOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        switch (renameOp.getEntityCase()) {
            case RENAME_TABLE:
                // table-map manipulation to simulate rename, replace with actual logic.
                RenameTable renameTable = renameOp.getRenameTable();
                Table tbl = tableMap.remove(renameTable.getFromTable());
                tableMap.put(renameTable.getToTable(), tbl.toBuilder().setName(renameTable.getToTable()).build());

                logger.info(String.format("[Migrate:RenameTable] from=%s to=%s schema=%s",
                        renameTable.getFromTable(), renameTable.getToTable(), schema));
                respBuilder.setSuccess(true);
                break;
            case RENAME_COLUMN:
                // table-map manipulation to simulate rename column, replace with actual logic.
                RenameColumn renameColumn = renameOp.getRenameColumn();
                Table tableObj = tableMap.get(table);
                Table.Builder updatedTableBuilder = metadataHelper.rebuildTableWithRenamedColumn(tableObj,
                        renameColumn.getFromColumn(), renameColumn.getToColumn());
                metadataHelper.updateTableWithModifiedColumns(tableMap, table, updatedTableBuilder);

                logger.info(String.format("[Migrate:RenameColumn] table=%s.%s from_col=%s to_col=%s",
                        schema, table, renameColumn.getFromColumn(), renameColumn.getToColumn()));
                respBuilder.setSuccess(true);
                break;
            case ENTITY_NOT_SET:
            default:
                logger.warning("[Migrate:Rename] No rename entity specified");
                respBuilder.setUnsupported(true);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleAdd(AddOperation addOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        switch (addOp.getEntityCase()) {
            case ADD_COLUMN_IN_HISTORY_MODE: {
                // table-map manipulation to simulate add column in history mode, replace with actual logic.
                AddColumnInHistoryMode addColumnInHistoryMode = addOp.getAddColumnInHistoryMode();
                Column newCol = Column.newBuilder()
                        .setName(addColumnInHistoryMode.getColumn())
                        .setType(addColumnInHistoryMode.getColumnType())
                        .build();
                metadataHelper.addColumnToTable(tableMap, table, newCol);

                logger.info(String.format("[Migrate:AddColumnHistory] table=%s.%s column=%s type=%s default=%s op_ts=%s",
                        schema, table, addColumnInHistoryMode.getColumn(), addColumnInHistoryMode.getColumnType(), addColumnInHistoryMode.getDefaultValue(), addColumnInHistoryMode.getOperationTimestamp()));
                respBuilder.setSuccess(true);
                break;
            }
            case ADD_COLUMN_WITH_DEFAULT_VALUE: {
                // table-map manipulation to simulate add column with default value, replace with actual logic.
                AddColumnWithDefaultValue addColumnWithDefaultValue = addOp.getAddColumnWithDefaultValue();
                Column newCol = Column.newBuilder()
                        .setName(addColumnWithDefaultValue.getColumn())
                        .setType(addColumnWithDefaultValue.getColumnType())
                        .build();
                metadataHelper.addColumnToTable(tableMap, table, newCol);

                logger.info(String.format("[Migrate:AddColumnDefault] table=%s.%s column=%s type=%s default=%s",
                        schema, table, addColumnWithDefaultValue.getColumn(), addColumnWithDefaultValue.getColumnType(), addColumnWithDefaultValue.getDefaultValue()));
                respBuilder.setSuccess(true);
                break;
            }
            case ENTITY_NOT_SET:
            default:
                logger.warning("[Migrate:Add] No add entity specified");
                respBuilder.setUnsupported(true);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleUpdateColumnValue(UpdateColumnValueOperation upd, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();
        // Placeholder: Update all existing rows' column value.

        logger.info(String.format("[Migrate:UpdateColumnValue] table=%s.%s column=%s value=%s",
                schema, table, upd.getColumn(), upd.getValue()));
        respBuilder.setSuccess(true);

        return respBuilder;
    }

    public MigrateResponse.Builder handleTableSyncModeMigration(TableSyncModeMigrationOperation op, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        Table tableObj = tableMap.get(table);
        String softDeletedColumn = op.hasSoftDeletedColumn() ? op.getSoftDeletedColumn() : null;
        Table.Builder builder = tableObj.toBuilder();

        switch (op.getType()) {
            case SOFT_DELETE_TO_LIVE:
                // table-map manipulation to simulate soft delete to live, replace with actual logic.
                metadataHelper.removeColumnFromBuilder(builder, tableObj, softDeletedColumn);

                logger.info(String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from SOFT_DELETE to LIVE", schema, table));
                respBuilder.setSuccess(true);
                break;
            case SOFT_DELETE_TO_HISTORY:
                // table-map manipulation to simulate soft delete to history, replace with actual logic.
                metadataHelper.removeColumnFromBuilder(builder, tableObj, softDeletedColumn);
                metadataHelper.addHistoryModeColumns(builder);

                logger.info(String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from SOFT_DELETE to HISTORY", schema, table));
                respBuilder.setSuccess(true);
                break;
            case HISTORY_TO_SOFT_DELETE:
                // table-map manipulation to simulate history to soft delete, replace with actual logic.
                metadataHelper.removeHistoryModeColumns(builder, tableObj);
                metadataHelper.addSoftDeleteColumn(builder, softDeletedColumn);

                logger.info(String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from HISTORY to SOFT_DELETE", schema, table));
                respBuilder.setSuccess(true);
                break;
            case HISTORY_TO_LIVE:
                // table-map manipulation to simulate history to live, replace with actual logic.
                metadataHelper.removeHistoryModeColumns(builder, tableObj);

                logger.info(String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from HISTORY to LIVE", schema, table));
                respBuilder.setSuccess(true);
                break;
            case LIVE_TO_SOFT_DELETE:
                // table-map manipulation to simulate live to soft delete, replace with actual logic.
                metadataHelper.addSoftDeleteColumn(builder, softDeletedColumn);

                logger.info(String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from LIVE to SOFT_DELETE", schema, table));
                respBuilder.setSuccess(true);
                break;
            case LIVE_TO_HISTORY:
                // table-map manipulation to simulate live to history, replace with actual logic.
                metadataHelper.addHistoryModeColumns(builder);

                logger.info(String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from LIVE to HISTORY", schema, table));
                respBuilder.setSuccess(true);
                break;
            default:
                logger.warning(String.format("[Migrate:TableSyncModeMigration] Unknown migration type for table=%s.%s", schema, table));
                respBuilder.setUnsupported(true);
                return respBuilder;
        }

        tableMap.put(table, builder.build());
        return respBuilder;
    }

    public class TableMetadataHelper {
        // Constants for system columns
        private static final String FIVETRAN_START = "_fivetran_start";
        private static final String FIVETRAN_END = "_fivetran_end";
        private static final String FIVETRAN_ACTIVE = "_fivetran_active";

        public void removeColumnFromBuilder(Table.Builder builder, Table tableObj, String columnName) {
            if (columnName == null) return;
            builder.clearColumns();
            for (Column col : tableObj.getColumnsList()) {
                if (!col.getName().equals(columnName)) {
                    builder.addColumns(col);
                }
            }
        }

        public void removeHistoryModeColumns(Table.Builder builder, Table tableObj) {
            builder.clearColumns();
            for (Column col : tableObj.getColumnsList()) {
                String name = col.getName();
                if (!name.equals(FIVETRAN_START) && !name.equals(FIVETRAN_END) && !name.equals(FIVETRAN_ACTIVE)) {
                    builder.addColumns(col);
                }
            }
        }

        public void addHistoryModeColumns(Table.Builder builder) {
            builder.addColumns(Column.newBuilder().setName(FIVETRAN_START).setType(DataType.NAIVE_DATETIME).build());
            builder.addColumns(Column.newBuilder().setName(FIVETRAN_END).setType(DataType.NAIVE_DATETIME).build());
            builder.addColumns(Column.newBuilder().setName(FIVETRAN_ACTIVE).setType(DataType.BOOLEAN).build());
        }

        public void addSoftDeleteColumn(Table.Builder builder, String columnName) {
            if (columnName != null) {
                builder.addColumns(Column.newBuilder().setName(columnName).setType(DataType.BOOLEAN).build());
            }
        }

        public void updateTableWithModifiedColumns(Map<String, Table> tableMap, String tableName, Table.Builder updatedTableBuilder) {
            tableMap.put(tableName, updatedTableBuilder.build());
        }

        public Table.Builder rebuildTableWithoutColumn(Table tableObj, String columnName) {
            Table.Builder updatedTableBuilder = tableObj.toBuilder();
            updatedTableBuilder.clearColumns();
            for (Column col : tableObj.getColumnsList()) {
                if (!col.getName().equals(columnName)) {
                    updatedTableBuilder.addColumns(col);
                }
            }
            return updatedTableBuilder;
        }

        public Table.Builder rebuildTableWithRenamedColumn(Table tableObj, String fromColumn, String toColumn) {
            Table.Builder updatedTableBuilder = tableObj.toBuilder();
            updatedTableBuilder.clearColumns();
            for (Column col : tableObj.getColumnsList()) {
                if (col.getName().equals(fromColumn)) {
                    updatedTableBuilder.addColumns(col.toBuilder().setName(toColumn).build());
                } else {
                    updatedTableBuilder.addColumns(col);
                }
            }
            return updatedTableBuilder;
        }

        public void addColumnToTable(Map<String, Table> tableMap, String tableName, Column column) {
            Table tableObj = tableMap.get(tableName);
            if (tableObj != null) {
                Table updatedTable = tableObj.toBuilder().addColumns(column).build();
                tableMap.put(tableName, updatedTable);
            }
        }
    }
}



