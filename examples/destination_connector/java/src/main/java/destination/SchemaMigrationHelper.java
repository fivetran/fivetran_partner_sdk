package destination;

import com.google.gson.Gson;
import fivetran_sdk.v2.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Helper class for handling migration operations using DuckDB.
 */
public class SchemaMigrationHelper {
    private static final String INFO = "INFO";
    private static final String WARNING = "WARNING";

    private final DuckDBHelper dbHelper;

    public SchemaMigrationHelper(DuckDBHelper dbHelper) {
        this.dbHelper = dbHelper;
    }

    public MigrateResponse.Builder handleDrop(DropOperation dropOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        try {
            switch (dropOp.getEntityCase()) {
                case DROP_TABLE:
                    dbHelper.dropTable(schema, table);
                    logMessage(INFO, String.format("[Migrate:Drop] Dropping table %s.%s", schema, table));
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
                    logMessage(INFO, String.format("[Migrate:DropColumnHistory] table=%s.%s column=%s op_ts=%s - Column preserved for history",
                            schema, table, dropColumn.getColumn(), dropColumn.getOperationTimestamp()));
                    respBuilder.setSuccess(true);
                    break;

                case ENTITY_NOT_SET:
                default:
                    logMessage(WARNING, "[Migrate:Drop] No drop entity specified");
                    respBuilder.setUnsupported(true);
            }
        } catch (Exception e) {
            logMessage(WARNING, "[Migrate:Drop] Failed: " + e.getMessage());
            respBuilder.setSuccess(false);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleCopy(CopyOperation copyOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        try {
            switch (copyOp.getEntityCase()) {
                case COPY_TABLE:
                    CopyTable copyTable = copyOp.getCopyTable();
                    dbHelper.copyTable(schema, copyTable.getFromTable(), copyTable.getToTable());
                    logMessage(INFO, String.format("[Migrate:CopyTable] from=%s to=%s in schema=%s",
                            copyTable.getFromTable(), copyTable.getToTable(), schema));
                    respBuilder.setSuccess(true);
                    break;

                case COPY_COLUMN:
                    CopyColumn copyColumn = copyOp.getCopyColumn();
                    // Get table schema to find the column type
                    Table tableObj = dbHelper.describeTable(schema, table);
                    if (tableObj == null) {
                        logMessage(WARNING, String.format("[Migrate:CopyColumn] Table %s.%s does not exist", schema, table));
                        respBuilder.setSuccess(false);
                        break;
                    }

                    boolean columnFound = false;
                    for (Column col : tableObj.getColumnsList()) {
                        if (col.getName().equals(copyColumn.getFromColumn())) {
                            // Wrap column copy in transaction (add column + copy data)
                            dbHelper.beginTransaction();
                            try {
                                // Add new column with same type and params (preserves DECIMAL precision/scale, VARCHAR length, etc.)
                                Column newCol = col.toBuilder().setName(copyColumn.getToColumn()).build();
                                dbHelper.addColumn(schema, table, newCol);

                                // Copy data from old column to new column using escaped identifiers
                                String escapedSchema = dbHelper.escapeIdentifier(schema);
                                String escapedTable = dbHelper.escapeIdentifier(table);
                                String escapedToCol = dbHelper.escapeIdentifier(copyColumn.getToColumn());
                                String escapedFromCol = dbHelper.escapeIdentifier(copyColumn.getFromColumn());
                                String sql = String.format("UPDATE \"%s\".\"%s\" SET \"%s\" = \"%s\"",
                                        escapedSchema, escapedTable, escapedToCol, escapedFromCol);
                                try (Statement stmt = dbHelper.getConnection().createStatement()) {
                                    stmt.execute(sql);
                                }

                                dbHelper.commitTransaction();
                                columnFound = true;
                            } catch (Exception e) {
                                dbHelper.rollbackTransaction();
                                throw e;
                            }
                            break;
                        }
                    }

                    if (!columnFound) {
                        logMessage(WARNING, String.format("[Migrate:CopyColumn] Column %s not found in %s.%s",
                                copyColumn.getFromColumn(), schema, table));
                        respBuilder.setSuccess(false);
                        break;
                    }

                    logMessage(INFO, String.format("[Migrate:CopyColumn] table=%s.%s from_col=%s to_col=%s",
                            schema, table, copyColumn.getFromColumn(), copyColumn.getToColumn()));
                    respBuilder.setSuccess(true);
                    break;

                case COPY_TABLE_TO_HISTORY_MODE:
                    CopyTableToHistoryMode copyTableToHistoryMode = copyOp.getCopyTableToHistoryMode();
                    Table fromTableObj = dbHelper.describeTable(schema, copyTableToHistoryMode.getFromTable());

                    if (fromTableObj == null) {
                        logMessage(WARNING, String.format("[Migrate:CopyTableToHistoryMode] Source table %s.%s does not exist",
                                schema, copyTableToHistoryMode.getFromTable()));
                        respBuilder.setSuccess(false);
                        break;
                    }

                    // Create new table metadata without soft delete column and with history columns
                    Table newTable = TableMetadataHelper.createTableCopy(fromTableObj, copyTableToHistoryMode.getToTable());
                    newTable = TableMetadataHelper.removeColumnFromTable(newTable, copyTableToHistoryMode.getSoftDeletedColumn());
                    newTable = TableMetadataHelper.addHistoryModeColumns(newTable);

                    // Wrap table creation and data copy in transaction
                    dbHelper.beginTransaction();
                    try {
                        // Create the new table in DuckDB
                        dbHelper.createTable(schema, newTable);

                        // Copy data (excluding soft deleted column and history columns) with escaped identifiers
                        List<String> columnsToCopy = new ArrayList<>();
                        Set<String> historyColumns = new HashSet<>(Arrays.asList(
                                TableMetadataHelper.FIVETRAN_START,
                                TableMetadataHelper.FIVETRAN_END,
                                TableMetadataHelper.FIVETRAN_ACTIVE));

                        for (Column col : newTable.getColumnsList()) {
                            if (!historyColumns.contains(col.getName())) {
                                columnsToCopy.add(col.getName());
                            }
                        }

                        List<String> escapedColumns = new ArrayList<>();
                        for (String col : columnsToCopy) {
                            escapedColumns.add("\"" + dbHelper.escapeIdentifier(col) + "\"");
                        }
                        String columnsStr = String.join(", ", escapedColumns);

                        String escapedSchema = dbHelper.escapeIdentifier(schema);
                        String escapedToTable = dbHelper.escapeIdentifier(copyTableToHistoryMode.getToTable());
                        String escapedFromTable = dbHelper.escapeIdentifier(copyTableToHistoryMode.getFromTable());
                        String sql = String.format("INSERT INTO \"%s\".\"%s\" (%s) SELECT %s FROM \"%s\".\"%s\"",
                                escapedSchema, escapedToTable, columnsStr, columnsStr, escapedSchema, escapedFromTable);
                        try (Statement stmt = dbHelper.getConnection().createStatement()) {
                            stmt.execute(sql);
                        }

                        dbHelper.commitTransaction();
                    } catch (Exception e) {
                        dbHelper.rollbackTransaction();
                        throw e;
                    }

                    logMessage(INFO, String.format("[Migrate:CopyTableToHistoryMode] from=%s to=%s soft_deleted_column=%s",
                            copyTableToHistoryMode.getFromTable(), copyTableToHistoryMode.getToTable(),
                            copyTableToHistoryMode.getSoftDeletedColumn()));
                    respBuilder.setSuccess(true);
                    break;

                case ENTITY_NOT_SET:
                default:
                    logMessage(WARNING, "[Migrate:Copy] No copy entity specified");
                    respBuilder.setUnsupported(true);
            }
        } catch (Exception e) {
            logMessage(WARNING, "[Migrate:Copy] Failed: " + e.getMessage());
            respBuilder.setSuccess(false);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleRename(RenameOperation renameOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        try {
            switch (renameOp.getEntityCase()) {
                case RENAME_TABLE:
                    RenameTable renameTable = renameOp.getRenameTable();
                    dbHelper.renameTable(schema, renameTable.getFromTable(), renameTable.getToTable());
                    logMessage(INFO, String.format("[Migrate:RenameTable] from=%s to=%s schema=%s",
                            renameTable.getFromTable(), renameTable.getToTable(), schema));
                    respBuilder.setSuccess(true);
                    break;

                case RENAME_COLUMN:
                    RenameColumn renameColumn = renameOp.getRenameColumn();
                    dbHelper.renameColumn(schema, table, renameColumn.getFromColumn(), renameColumn.getToColumn());
                    logMessage(INFO, String.format("[Migrate:RenameColumn] table=%s.%s from_col=%s to_col=%s",
                            schema, table, renameColumn.getFromColumn(), renameColumn.getToColumn()));
                    respBuilder.setSuccess(true);
                    break;

                case ENTITY_NOT_SET:
                default:
                    logMessage(WARNING, "[Migrate:Rename] No rename entity specified");
                    respBuilder.setUnsupported(true);
            }
        } catch (Exception e) {
            logMessage(WARNING, "[Migrate:Rename] Failed: " + e.getMessage());
            respBuilder.setSuccess(false);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleAdd(AddOperation addOp, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        try {
            switch (addOp.getEntityCase()) {
                case ADD_COLUMN_IN_HISTORY_MODE: {
                    AddColumnInHistoryMode addColHistoryMode = addOp.getAddColumnInHistoryMode();
                    Column newCol = Column.newBuilder()
                            .setName(addColHistoryMode.getColumn())
                            .setType(addColHistoryMode.getColumnType())
                            .build();

                    // Wrap add column + optional update in transaction
                    dbHelper.beginTransaction();
                    try {
                        dbHelper.addColumn(schema, table, newCol);

                        // If default value is provided, update existing rows
                        if (addColHistoryMode.getDefaultValue() != null && !addColHistoryMode.getDefaultValue().isEmpty()) {
                            dbHelper.updateColumnValue(schema, table, addColHistoryMode.getColumn(),
                                    addColHistoryMode.getDefaultValue());
                        }

                        dbHelper.commitTransaction();
                    } catch (Exception e) {
                        dbHelper.rollbackTransaction();
                        throw e;
                    }

                    logMessage(INFO, String.format("[Migrate:AddColumnHistory] table=%s.%s column=%s type=%s default=%s op_ts=%s",
                            schema, table, addColHistoryMode.getColumn(), addColHistoryMode.getColumnType(),
                            addColHistoryMode.getDefaultValue(), addColHistoryMode.getOperationTimestamp()));
                    respBuilder.setSuccess(true);
                    break;
                }

                case ADD_COLUMN_WITH_DEFAULT_VALUE: {
                    AddColumnWithDefaultValue addColDefaultWithValue = addOp.getAddColumnWithDefaultValue();
                    Column newCol = Column.newBuilder()
                            .setName(addColDefaultWithValue.getColumn())
                            .setType(addColDefaultWithValue.getColumnType())
                            .build();

                    // Wrap add column + optional update in transaction
                    dbHelper.beginTransaction();
                    try {
                        dbHelper.addColumn(schema, table, newCol);

                        // Update existing rows with default value
                        if (addColDefaultWithValue.getDefaultValue() != null && !addColDefaultWithValue.getDefaultValue().isEmpty()) {
                            dbHelper.updateColumnValue(schema, table, addColDefaultWithValue.getColumn(),
                                    addColDefaultWithValue.getDefaultValue());
                        }

                        dbHelper.commitTransaction();
                    } catch (Exception e) {
                        dbHelper.rollbackTransaction();
                        throw e;
                    }

                    logMessage(INFO, String.format("[Migrate:AddColumnDefault] table=%s.%s column=%s type=%s default=%s",
                            schema, table, addColDefaultWithValue.getColumn(), addColDefaultWithValue.getColumnType(),
                            addColDefaultWithValue.getDefaultValue()));
                    respBuilder.setSuccess(true);
                    break;
                }

                case ENTITY_NOT_SET:
                default:
                    logMessage(WARNING, "[Migrate:Add] No add entity specified");
                    respBuilder.setUnsupported(true);
            }
        } catch (Exception e) {
            logMessage(WARNING, "[Migrate:Add] Failed: " + e.getMessage());
            respBuilder.setSuccess(false);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleUpdateColumnValue(UpdateColumnValueOperation upd, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();

        try {
            dbHelper.updateColumnValue(schema, table, upd.getColumn(), upd.getValue());
            logMessage(INFO, String.format("[Migrate:UpdateColumnValue] table=%s.%s column=%s value=%s",
                    schema, table, upd.getColumn(), upd.getValue()));
            respBuilder.setSuccess(true);
        } catch (Exception e) {
            logMessage(WARNING, "[Migrate:UpdateColumnValue] Failed: " + e.getMessage());
            respBuilder.setSuccess(false);
        }

        return respBuilder;
    }

    public MigrateResponse.Builder handleTableSyncModeMigration(TableSyncModeMigrationOperation op, String schema, String table) {
        MigrateResponse.Builder respBuilder = MigrateResponse.newBuilder();
        String opSoftDeletedColumn = op.getSoftDeletedColumn();
        String softDeletedColumn = (opSoftDeletedColumn != null && !opSoftDeletedColumn.isEmpty()) ? opSoftDeletedColumn : null;

        try {
            switch (op.getType()) {
                case SOFT_DELETE_TO_LIVE:
                    logMessage(WARNING, "[Migrate:TableSyncModeMigration] Migration from SOFT_DELETE to LIVE is not supported");
                    respBuilder.setSuccess(false);
                    break;

                case SOFT_DELETE_TO_HISTORY:
                    // Wrap drop column + add history columns in transaction
                    dbHelper.beginTransaction();
                    try {
                        // Remove soft delete column and add history mode columns
                        if (softDeletedColumn != null) {
                            dbHelper.dropColumn(schema, table, softDeletedColumn);
                        }
                        TableMetadataHelper.addHistoryModeColumnsToDb(dbHelper, schema, table);
                        dbHelper.commitTransaction();
                    } catch (Exception e) {
                        dbHelper.rollbackTransaction();
                        throw e;
                    }
                    logMessage(INFO, String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from SOFT_DELETE to HISTORY",
                            schema, table));
                    respBuilder.setSuccess(true);
                    break;

                case HISTORY_TO_SOFT_DELETE:
                    // Wrap remove history columns + add soft delete column in transaction
                    dbHelper.beginTransaction();
                    try {
                        // Remove history mode columns and add soft delete column
                        TableMetadataHelper.removeHistoryModeColumnsFromDb(dbHelper, schema, table);
                        if (softDeletedColumn != null) {
                            TableMetadataHelper.addSoftDeleteColumnToDb(dbHelper, schema, table, softDeletedColumn);
                        }
                        dbHelper.commitTransaction();
                    } catch (Exception e) {
                        dbHelper.rollbackTransaction();
                        throw e;
                    }
                    logMessage(INFO, String.format("[Migrate:TableSyncModeMigration] Migrating table=%s.%s from HISTORY to SOFT_DELETE",
                            schema, table));
                    respBuilder.setSuccess(true);
                    break;

                case HISTORY_TO_LIVE:
                    logMessage(WARNING, "[Migrate:TableSyncModeMigration] Migration from HISTORY to LIVE is not supported");
                    respBuilder.setSuccess(false);
                    break;

                case LIVE_TO_SOFT_DELETE:
                    logMessage(WARNING, "[Migrate:TableSyncModeMigration] Migration from LIVE to SOFT_DELETE is not supported");
                    respBuilder.setSuccess(false);
                    break;

                case LIVE_TO_HISTORY:
                    logMessage(WARNING, "[Migrate:TableSyncModeMigration] Migration from LIVE to HISTORY is not supported");
                    respBuilder.setSuccess(false);
                    break;

                default:
                    logMessage(WARNING, String.format("[Migrate:TableSyncModeMigration] Unknown migration type for table=%s.%s",
                            schema, table));
                    respBuilder.setUnsupported(true);
            }
        } catch (Exception e) {
            logMessage(WARNING, "[Migrate:TableSyncModeMigration] Failed: " + e.getMessage());
            respBuilder.setSuccess(false);
        }

        return respBuilder;
    }

    /**
     * Log a structured message.
     */
    private void logMessage(String level, String message) {
        Gson gson = new Gson();
        Map<String, String> logEntry = new HashMap<>();
        logEntry.put("level", level);
        logEntry.put("message", message);
        logEntry.put("message-origin", "sdk_destination");
        System.out.println(gson.toJson(logEntry));
    }
}
