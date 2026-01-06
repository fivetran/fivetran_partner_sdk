package destination;

import com.google.gson.Gson;
import fivetran_sdk.v2.*;

import java.sql.*;
import java.util.*;

/**
 * Helper class for table operations including CreateTable, AlterTable, Truncate, and DescribeTable.
 */
public class TableOperationsHelper {
    private static final String INFO = "INFO";
    private static final String WARNING = "WARNING";

    private final DuckDBHelper dbHelper;

    public TableOperationsHelper(DuckDBHelper dbHelper) {
        this.dbHelper = dbHelper;
    }

    /**
     * Handle CreateTable operation.
     */
    public CreateTableResponse createTable(CreateTableRequest request, String defaultSchema) {
        String schemaName = (!request.getSchemaName().isEmpty()) ? request.getSchemaName() : defaultSchema;
        System.out.println(String.format("[CreateTable]: %s | %s | %s",
            schemaName, request.getTable().getName(), request.getTable().getColumnsList()));

        try {
            dbHelper.createTable(schemaName, request.getTable());
            return CreateTableResponse.newBuilder().setSuccess(true).build();
        } catch (Exception e) {
            logMessage(WARNING, "CreateTable failed: " + e.getMessage());
            return CreateTableResponse.newBuilder().setSuccess(false).build();
        }
    }

    /**
     * Handle DescribeTable operation.
     */
    public DescribeTableResponse describeTable(DescribeTableRequest request, String defaultSchema) {
        String schemaName = (!request.getSchemaName().isEmpty()) ? request.getSchemaName() : defaultSchema;
        logMessage(INFO, "Fetching table info for " + schemaName + "." + request.getTableName());

        try {
            Table table = dbHelper.describeTable(schemaName, request.getTableName());
            if (table == null) {
                return DescribeTableResponse.newBuilder().setNotFound(true).build();
            } else {
                return DescribeTableResponse.newBuilder().setNotFound(false).setTable(table).build();
            }
        } catch (Exception e) {
            logMessage(WARNING, "DescribeTable failed: " + e.getMessage());
            return DescribeTableResponse.newBuilder().setNotFound(true).build();
        }
    }

    /**
     * Compare two columns to determine if they have different types.
     * For DECIMAL types, also compares precision and scale.
     * For STRING types, also compares string_byte_length.
     */
    private boolean columnsHaveDifferentTypes(Column col1, Column col2) {
        // If base types are different, they definitely have different types
        if (col1.getType() != col2.getType()) {
            return true;
        }

        // Special handling for DECIMAL - check precision/scale
        if (col1.getType() == DataType.DECIMAL) {
            boolean col1HasParams = col1.hasParams();
            boolean col2HasParams = col2.hasParams();

            // If only one has params, they're different
            if (col1HasParams != col2HasParams) {
                return true;
            }

            // If both have params, check if they both have decimal
            if (col1HasParams && col2HasParams) {
                boolean col1HasDecimal = col1.getParams().hasDecimal();
                boolean col2HasDecimal = col2.getParams().hasDecimal();

                if (col1HasDecimal != col2HasDecimal) {
                    return true;
                }

                // If both have decimal params, compare precision and scale
                if (col1HasDecimal && col2HasDecimal) {
                    return col1.getParams().getDecimal().getPrecision() != col2.getParams().getDecimal().getPrecision() ||
                           col1.getParams().getDecimal().getScale() != col2.getParams().getDecimal().getScale();
                }
            }
        }

        // Special handling for STRING - check string_byte_length
        if (col1.getType() == DataType.STRING) {
            int col1ByteLength = col1.hasParams() ? col1.getParams().getStringByteLength() : 0;
            int col2ByteLength = col2.hasParams() ? col2.getParams().getStringByteLength() : 0;

            if (col1ByteLength != col2ByteLength) {
                return true;
            }
        }

        return false;
    }

    /**
     * Handle AlterTable operation including column additions, type changes,
     * column drops, and primary key changes.
     */
    public AlterTableResponse alterTable(AlterTableRequest request, String schemaName, String defaultSchema) {
        schemaName = (schemaName != null && !schemaName.isEmpty()) ? schemaName : defaultSchema;
        boolean dropColumns = request.getDropColumns();
        logMessage(INFO, String.format("[AlterTable]: %s | %s | drop_columns=%s | columns=%s",
            schemaName, request.getTable().getName(), dropColumns, request.getTable().getColumnsList()));

        try {
            // Get current table schema
            Table currentTable = dbHelper.describeTable(schemaName, request.getTable().getName());

            if (currentTable == null) {
                logMessage(WARNING, "Table " + schemaName + "." + request.getTable().getName() + " does not exist");
                return AlterTableResponse.newBuilder().setSuccess(false).build();
            }

            // Compare current and requested columns
            Set<String> currentColumnNames = new HashSet<>();
            Map<String, Column> currentColumnsMap = new HashMap<>();
            for (Column col : currentTable.getColumnsList()) {
                currentColumnNames.add(col.getName());
                currentColumnsMap.put(col.getName(), col);
            }

            Set<String> requestedColumnNames = new HashSet<>();
            Map<String, Column> requestedColumnsMap = new HashMap<>();
            for (Column col : request.getTable().getColumnsList()) {
                requestedColumnNames.add(col.getName());
                requestedColumnsMap.put(col.getName(), col);
            }

            // Find columns to add (in request but not in current)
            List<Column> newColumns = new ArrayList<>();
            for (Column col : request.getTable().getColumnsList()) {
                if (!currentColumnNames.contains(col.getName())) {
                    newColumns.add(col);
                }
            }

            // Find columns to drop (in current but not in request)
            List<String> columnsToDrop = new ArrayList<>();
            for (Column col : currentTable.getColumnsList()) {
                if (!requestedColumnNames.contains(col.getName())) {
                    columnsToDrop.add(col.getName());
                }
            }

            // Find columns with type changes (in both but different types)
            List<Map.Entry<String, Column>> columnsWithTypeChanges = new ArrayList<>();
            Set<String> commonColumns = new HashSet<>(currentColumnNames);
            commonColumns.retainAll(requestedColumnNames);
            for (String colName : commonColumns) {
                Column currentCol = currentColumnsMap.get(colName);
                Column requestedCol = requestedColumnsMap.get(colName);
                if (columnsHaveDifferentTypes(currentCol, requestedCol)) {
                    columnsWithTypeChanges.add(new AbstractMap.SimpleEntry<>(colName, requestedCol));
                }
            }

            // Wrap all ALTER TABLE operations in a transaction for atomicity
            dbHelper.beginTransaction();
            try {
                // Add new columns
                for (Column column : newColumns) {
                    dbHelper.addColumn(schemaName, request.getTable().getName(), column);
                    logMessage(INFO, "Added column: " + column.getName() + " to " + schemaName + "." + request.getTable().getName());
                }

                // Handle type changes using DuckDB's native ALTER COLUMN
                for (Map.Entry<String, Column> entry : columnsWithTypeChanges) {
                    String colName = entry.getKey();
                    Column newColDef = entry.getValue();
                    logMessage(INFO, "Changing type for column: " + colName + " to " + newColDef.getType());

                    String escapedSchema = dbHelper.escapeIdentifier(schemaName);
                    String escapedTable = dbHelper.escapeIdentifier(request.getTable().getName());
                    String escapedCol = dbHelper.escapeIdentifier(colName);
                    String sqlType = dbHelper.mapDatatypeToSql(newColDef.getType(), newColDef);

                    // Use DuckDB's native ALTER COLUMN SET DATA TYPE
                    String sql = String.format("ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" SET DATA TYPE %s",
                        escapedSchema, escapedTable, escapedCol, sqlType);
                    try (Statement stmt = dbHelper.getConnection().createStatement()) {
                        stmt.execute(sql);
                    }

                    logMessage(INFO, "Type change completed for column: " + colName);
                }

                // Handle primary key changes
                handlePrimaryKeyChanges(schemaName, request.getTable().getName(), currentTable, request.getTable());

                // Drop columns if drop_columns flag is true
                if (dropColumns && !columnsToDrop.isEmpty()) {
                    for (String columnName : columnsToDrop) {
                        dbHelper.dropColumn(schemaName, request.getTable().getName(), columnName);
                        logMessage(INFO, "Dropped column: " + columnName + " from " + schemaName + "." + request.getTable().getName());
                    }
                } else if (!columnsToDrop.isEmpty()) {
                    logMessage(INFO, "Skipping drop of " + columnsToDrop.size() + " columns (drop_columns=false): " + columnsToDrop);
                }

                dbHelper.commitTransaction();
                return AlterTableResponse.newBuilder().setSuccess(true).build();
            } catch (Exception e) {
                dbHelper.rollbackTransaction();
                throw e;
            }
        } catch (Exception e) {
            logMessage(WARNING, "AlterTable failed: " + e.getMessage());
            return AlterTableResponse.newBuilder().setSuccess(false).build();
        }
    }

    /**
     * Handle primary key constraint changes.
     */
    private void handlePrimaryKeyChanges(String schemaName, String tableName, Table currentTable, Table requestedTable) throws SQLException {
        List<String> currentPkColumns = new ArrayList<>();
        for (Column col : currentTable.getColumnsList()) {
            if (col.getPrimaryKey()) {
                currentPkColumns.add(col.getName());
            }
        }

        List<String> requestedPkColumns = new ArrayList<>();
        for (Column col : requestedTable.getColumnsList()) {
            if (col.getPrimaryKey()) {
                requestedPkColumns.add(col.getName());
            }
        }

        // Check if primary key changed
        if (!new HashSet<>(currentPkColumns).equals(new HashSet<>(requestedPkColumns))) {
            logMessage(INFO, "Primary key change detected: " + currentPkColumns + " -> " + requestedPkColumns);

            String escapedSchema = dbHelper.escapeIdentifier(schemaName);
            String escapedTable = dbHelper.escapeIdentifier(tableName);

            // Drop existing primary key constraint if it exists
            if (!currentPkColumns.isEmpty()) {
                String constraintName = tableName + "_pkey";
                String escapedConstraint = dbHelper.escapeIdentifier(constraintName);
                try {
                    String sql = String.format("ALTER TABLE \"%s\".\"%s\" DROP CONSTRAINT \"%s\"",
                        escapedSchema, escapedTable, escapedConstraint);
                    try (Statement stmt = dbHelper.getConnection().createStatement()) {
                        stmt.execute(sql);
                        logMessage(INFO, "Dropped primary key constraint: " + constraintName);
                    }
                } catch (SQLException e) {
                    logMessage(WARNING, "Could not drop primary key constraint: " + e.getMessage());
                }
            }

            // Add new primary key constraint if requested
            if (!requestedPkColumns.isEmpty()) {
                List<String> escapedPkCols = new ArrayList<>();
                for (String col : requestedPkColumns) {
                    escapedPkCols.add("\"" + dbHelper.escapeIdentifier(col) + "\"");
                }
                String pkColsStr = String.join(", ", escapedPkCols);
                String sql = String.format("ALTER TABLE \"%s\".\"%s\" ADD PRIMARY KEY (%s)",
                    escapedSchema, escapedTable, pkColsStr);
                try (Statement stmt = dbHelper.getConnection().createStatement()) {
                    stmt.execute(sql);
                    logMessage(INFO, "Added primary key constraint on columns: " + requestedPkColumns);
                }
            }
        }
    }

    /**
     * Handle Truncate operation (both hard and soft truncate).
     */
    public TruncateResponse truncateTable(TruncateRequest request, String defaultSchema) {
        String schemaName = (!request.getSchemaName().isEmpty()) ? request.getSchemaName() : defaultSchema;
        String tableName = request.getTableName();
        System.out.println(String.format("[TruncateTable]: %s | %s | soft=%s",
            schemaName, tableName, request.hasSoft()));

        try {
            // Check if soft truncate is requested
            if (request.hasSoft()) {
                // Soft truncate: mark rows as deleted instead of removing them
                String deletedColumn = request.getSoft().getDeletedColumn();
                logMessage(INFO, "Performing soft truncate on " + schemaName + "." + tableName + " using column " + deletedColumn);

                // Build UPDATE statement to mark all rows as deleted
                String escapedSchema = dbHelper.escapeIdentifier(schemaName);
                String escapedTable = dbHelper.escapeIdentifier(tableName);
                String escapedDeletedCol = dbHelper.escapeIdentifier(deletedColumn);

                // Handle time-based truncate if synced_column and utc_delete_before are provided
                if (!request.getSyncedColumn().isEmpty() && request.hasUtcDeleteBefore()) {
                    String escapedSyncedCol = dbHelper.escapeIdentifier(request.getSyncedColumn());
                    Timestamp deleteBeforeTimestamp = new Timestamp(request.getUtcDeleteBefore().getSeconds() * 1000);
                    String sql = String.format("UPDATE \"%s\".\"%s\" SET \"%s\" = TRUE WHERE \"%s\" < ?",
                        escapedSchema, escapedTable, escapedDeletedCol, escapedSyncedCol);
                    try (PreparedStatement stmt = dbHelper.getConnection().prepareStatement(sql)) {
                        stmt.setTimestamp(1, deleteBeforeTimestamp);
                        stmt.executeUpdate();
                        logMessage(INFO, "Soft truncated rows where " + request.getSyncedColumn() + " < " + deleteBeforeTimestamp);
                    }
                } else {
                    // Mark all rows as deleted
                    String sql = String.format("UPDATE \"%s\".\"%s\" SET \"%s\" = TRUE",
                        escapedSchema, escapedTable, escapedDeletedCol);
                    try (Statement stmt = dbHelper.getConnection().createStatement()) {
                        stmt.executeUpdate(sql);
                        logMessage(INFO, "Soft truncated all rows in " + schemaName + "." + tableName);
                    }
                }
            } else {
                // Hard truncate: remove all data from the table
                dbHelper.truncateTable(schemaName, tableName);
                logMessage(INFO, "Hard truncated " + schemaName + "." + tableName);
            }

            return TruncateResponse.newBuilder().setSuccess(true).build();
        } catch (Exception e) {
            logMessage(WARNING, "Truncate failed: " + e.getMessage());
            return TruncateResponse.newBuilder().setSuccess(false).build();
        }
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
