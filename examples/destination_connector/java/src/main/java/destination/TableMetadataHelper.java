package destination;

import com.google.gson.Gson;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.Table;

import java.sql.SQLException;
import java.util.*;

/**
 * Helper class for table metadata operations.
 */
public class TableMetadataHelper {
    private static final String INFO = "INFO";
    private static final String WARNING = "WARNING";

    // Constants for system columns
    public static final String FIVETRAN_START = "_fivetran_start";
    public static final String FIVETRAN_END = "_fivetran_end";
    public static final String FIVETRAN_ACTIVE = "_fivetran_active";

    /**
     * Creates a copy of a table.
     */
    public static Table createTableCopy(Table tableObj, String newName) {
        return Table.newBuilder(tableObj)
            .setName(newName)
            .build();
    }

    /**
     * Removes a column from a table.
     */
    public static Table removeColumnFromTable(Table tableObj, String columnName) {
        if (columnName == null || columnName.isEmpty()) {
            return tableObj;
        }

        Table.Builder builder = Table.newBuilder(tableObj);
        builder.clearColumns();

        for (Column col : tableObj.getColumnsList()) {
            if (!col.getName().equals(columnName)) {
                builder.addColumns(col);
            }
        }

        return builder.build();
    }

    /**
     * Adds history mode columns to a table.
     */
    public static Table addHistoryModeColumns(Table tableObj) {
        Table.Builder builder = Table.newBuilder(tableObj);

        Column startCol = Column.newBuilder()
            .setName(FIVETRAN_START)
            .setType(DataType.UTC_DATETIME)
            .build();

        Column endCol = Column.newBuilder()
            .setName(FIVETRAN_END)
            .setType(DataType.UTC_DATETIME)
            .build();

        Column activeCol = Column.newBuilder()
            .setName(FIVETRAN_ACTIVE)
            .setType(DataType.BOOLEAN)
            .build();

        builder.addColumns(startCol);
        builder.addColumns(endCol);
        builder.addColumns(activeCol);

        return builder.build();
    }

    /**
     * Adds history mode columns to a table in the database.
     */
    public static void addHistoryModeColumnsToDb(DuckDBHelper dbHelper, String schema, String table) {
        List<Column> columnsToAdd = Arrays.asList(
            Column.newBuilder().setName(FIVETRAN_START).setType(DataType.UTC_DATETIME).build(),
            Column.newBuilder().setName(FIVETRAN_END).setType(DataType.UTC_DATETIME).build(),
            Column.newBuilder().setName(FIVETRAN_ACTIVE).setType(DataType.BOOLEAN).build()
        );

        for (Column column : columnsToAdd) {
            try {
                dbHelper.addColumn(schema, table, column);
            } catch (SQLException e) {
                logMessage(WARNING, "Failed to add column " + column.getName() + " to " + schema + "." + table + ": " + e.getMessage());
            }
        }
    }

    /**
     * Removes history mode columns from a table in the database.
     */
    public static void removeHistoryModeColumnsFromDb(DuckDBHelper dbHelper, String schema, String table) {
        List<String> columnsToDrop = Arrays.asList(FIVETRAN_START, FIVETRAN_END, FIVETRAN_ACTIVE);

        for (String column : columnsToDrop) {
            try {
                dbHelper.dropColumn(schema, table, column);
            } catch (SQLException e) {
                logMessage(WARNING, "Failed to drop column " + column + " from " + schema + "." + table + ": " + e.getMessage());
            }
        }
    }

    /**
     * Removes history mode columns from a table.
     */
    public static Table removeHistoryModeColumns(Table tableObj) {
        Table.Builder builder = Table.newBuilder(tableObj);
        builder.clearColumns();

        Set<String> historyColumns = new HashSet<>(Arrays.asList(FIVETRAN_START, FIVETRAN_END, FIVETRAN_ACTIVE));

        for (Column col : tableObj.getColumnsList()) {
            if (!historyColumns.contains(col.getName())) {
                builder.addColumns(col);
            }
        }

        return builder.build();
    }

    /**
     * Adds a soft delete column to a table.
     */
    public static Table addSoftDeleteColumn(Table tableObj, String columnName) {
        if (columnName == null || columnName.isEmpty()) {
            return tableObj;
        }

        Column softDelCol = Column.newBuilder()
            .setName(columnName)
            .setType(DataType.BOOLEAN)
            .build();

        return Table.newBuilder(tableObj)
            .addColumns(softDelCol)
            .build();
    }

    /**
     * Adds a soft delete column to a table in the database.
     */
    public static void addSoftDeleteColumnToDb(DuckDBHelper dbHelper, String schema, String table, String columnName) {
        if (columnName != null && !columnName.isEmpty()) {
            Column softDelCol = Column.newBuilder()
                .setName(columnName)
                .setType(DataType.BOOLEAN)
                .build();
            try {
                dbHelper.addColumn(schema, table, softDelCol);
            } catch (SQLException e) {
                logMessage(WARNING, "Failed to add soft delete column " + columnName + " to " + schema + "." + table + ": " + e.getMessage());
            }
        }
    }

    /**
     * Log a structured message.
     */
    private static void logMessage(String level, String message) {
        Gson gson = new Gson();
        Map<String, String> logEntry = new HashMap<>();
        logEntry.put("level", level);
        logEntry.put("message", message);
        logEntry.put("message-origin", "sdk_destination");
        System.out.println(gson.toJson(logEntry));
    }
}
