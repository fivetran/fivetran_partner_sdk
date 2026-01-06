package destination;

import com.google.gson.Gson;
import fivetran_sdk.v2.Column;
import fivetran_sdk.v2.DataType;
import fivetran_sdk.v2.DataTypeParams;
import fivetran_sdk.v2.DecimalParams;
import fivetran_sdk.v2.Table;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for DuckDB operations.
 * Provides database connection management and SQL operations for the destination connector.
 */
public class DuckDBHelper implements AutoCloseable {
    private static final String INFO = "INFO";
    private static final String WARNING = "WARNING";

    private final String dbPath;
    private Connection connection;

    /**
     * Initialize DuckDB connection.
     *
     * @param dbPath Path to database file. Use ":memory:" for in-memory database.
     */
    public DuckDBHelper(String dbPath) {
        this.dbPath = (dbPath == null || dbPath.isEmpty()) ? ":memory:" : dbPath;
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            this.connection = DriverManager.getConnection("jdbc:duckdb:" + this.dbPath);
            logMessage(INFO, "Connected to DuckDB at: " + this.dbPath);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failed to connect to DuckDB: " + e.getMessage(), e);
        }
    }

    /**
     * Get the DuckDB connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Begin a transaction.
     */
    public void beginTransaction() throws SQLException {
        connection.setAutoCommit(false);
    }

    /**
     * Commit a transaction.
     */
    public void commitTransaction() throws SQLException {
        connection.commit();
        connection.setAutoCommit(true);
        logMessage(INFO, "Transaction committed successfully");
    }

    /**
     * Rollback a transaction.
     */
    public void rollbackTransaction() {
        try {
            connection.rollback();
            connection.setAutoCommit(true);
            logMessage(WARNING, "Transaction rolled back");
        } catch (SQLException e) {
            logMessage(WARNING, "Error during rollback: " + e.getMessage());
        }
    }

    /**
     * Safely escape an identifier for use in SQL queries.
     * Doubles any double-quotes in the identifier to escape them.
     *
     * IMPORTANT: Always use the escaped identifier within double quotes in SQL:
     *
     * Correct usage:
     *     String escapedName = escapeIdentifier(tableName);
     *     String sql = String.format("SELECT * FROM \"%s\"", escapedName);  // Note the quotes!
     *
     * @param identifier The identifier to escape
     * @return Escaped identifier safe for use in quoted SQL identifiers
     */
    public String escapeIdentifier(String identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier cannot be null");
        }
        return identifier.replace("\"", "\"\"");
    }

    /**
     * Close the DuckDB connection.
     * Forces a checkpoint to ensure all data is written to disk before closing.
     */
    @Override
    public void close() {
        if (connection != null) {
            try {
                // Force a checkpoint to write all WAL data to the main database file
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("CHECKPOINT");
                    logMessage(INFO, "DuckDB checkpoint completed");
                } catch (SQLException e) {
                    logMessage(WARNING, "Error during checkpoint: " + e.getMessage());
                }

                connection.close();
                logMessage(INFO, "DuckDB connection closed");
            } catch (SQLException e) {
                logMessage(WARNING, "Error while closing DuckDB connection: " + e.getMessage());
            } finally {
                connection = null;
            }
        }
    }

    /**
     * Check if a table exists in the given schema.
     */
    public boolean tableExists(String schemaName, String tableName) throws SQLException {
        String query = "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt("count") > 0;
            }
        }
    }

    /**
     * Create a schema if it doesn't exist.
     */
    public void createSchemaIfNotExists(String schemaName) throws SQLException {
        String sql = String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", escapeIdentifier(schemaName));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Schema created or already exists: " + schemaName);
        }
    }

    /**
     * Create a table with the given schema.
     */
    public void createTable(String schemaName, Table table) throws SQLException {
        createSchemaIfNotExists(schemaName);

        List<String> columnDefs = new ArrayList<>();
        for (Column column : table.getColumnsList()) {
            String columnDef = String.format("\"%s\" %s",
                escapeIdentifier(column.getName()),
                mapDatatypeToSql(column.getType(), column));
            columnDefs.add(columnDef);
        }

        String columnsStr = String.join(", ", columnDefs);
        String sql = String.format("CREATE TABLE \"%s\".\"%s\" (%s)",
            escapeIdentifier(schemaName),
            escapeIdentifier(table.getName()),
            columnsStr);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Table created: " + schemaName + "." + table.getName());
        }
    }

    /**
     * Drop a table if it exists.
     */
    public void dropTable(String schemaName, String tableName) throws SQLException {
        String sql = String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"",
            escapeIdentifier(schemaName),
            escapeIdentifier(tableName));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Table dropped: " + schemaName + "." + tableName);
        }
    }

    /**
     * Get table schema information.
     */
    public Table describeTable(String schemaName, String tableName) throws SQLException {
        if (!tableExists(schemaName, tableName)) {
            return null;
        }

        String query = "SELECT column_name, data_type, numeric_precision, numeric_scale, character_maximum_length " +
                      "FROM information_schema.columns " +
                      "WHERE table_schema = ? AND table_name = ? " +
                      "ORDER BY ordinal_position";

        List<Column> columns = new ArrayList<>();

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    Integer numericPrecision = rs.getObject("numeric_precision", Integer.class);
                    Integer numericScale = rs.getObject("numeric_scale", Integer.class);
                    Integer characterMaxLength = rs.getObject("character_maximum_length", Integer.class);

                    DataType columnType = mapSqlTypeToDatatype(dataType);
                    Column.Builder columnBuilder = Column.newBuilder()
                        .setName(columnName)
                        .setType(columnType);

                    // For DECIMAL types, populate precision and scale
                    if (columnType == DataType.DECIMAL && numericPrecision != null) {
                        DecimalParams decimalParams = DecimalParams.newBuilder()
                            .setPrecision(numericPrecision)
                            .setScale(numericScale != null ? numericScale : 0)
                            .build();
                        columnBuilder.setParams(DataTypeParams.newBuilder()
                            .setDecimal(decimalParams)
                            .build());
                    }

                    // For STRING types, populate string_byte_length if available
                    if (columnType == DataType.STRING && characterMaxLength != null) {
                        columnBuilder.setParams(DataTypeParams.newBuilder()
                            .setStringByteLength(characterMaxLength)
                            .build());
                    }

                    columns.add(columnBuilder.build());
                }
            }
        }

        return Table.newBuilder()
            .setName(tableName)
            .addAllColumns(columns)
            .build();
    }

    /**
     * Add a column to an existing table.
     */
    public void addColumn(String schemaName, String tableName, Column column) throws SQLException {
        String sql = String.format("ALTER TABLE \"%s\".\"%s\" ADD COLUMN \"%s\" %s",
            escapeIdentifier(schemaName),
            escapeIdentifier(tableName),
            escapeIdentifier(column.getName()),
            mapDatatypeToSql(column.getType(), column));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Column added: " + column.getName() + " to " + schemaName + "." + tableName);
        }
    }

    /**
     * Drop a column from a table.
     */
    public void dropColumn(String schemaName, String tableName, String columnName) throws SQLException {
        String sql = String.format("ALTER TABLE \"%s\".\"%s\" DROP COLUMN \"%s\"",
            escapeIdentifier(schemaName),
            escapeIdentifier(tableName),
            escapeIdentifier(columnName));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Column dropped: " + columnName + " from " + schemaName + "." + tableName);
        }
    }

    /**
     * Rename a column.
     */
    public void renameColumn(String schemaName, String tableName, String oldName, String newName) throws SQLException {
        String sql = String.format("ALTER TABLE \"%s\".\"%s\" RENAME COLUMN \"%s\" TO \"%s\"",
            escapeIdentifier(schemaName),
            escapeIdentifier(tableName),
            escapeIdentifier(oldName),
            escapeIdentifier(newName));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Column renamed: " + oldName + " to " + newName + " in " + schemaName + "." + tableName);
        }
    }

    /**
     * Truncate a table.
     */
    public void truncateTable(String schemaName, String tableName) throws SQLException {
        String sql = String.format("TRUNCATE TABLE \"%s\".\"%s\"",
            escapeIdentifier(schemaName),
            escapeIdentifier(tableName));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Table truncated: " + schemaName + "." + tableName);
        }
    }

    /**
     * Rename a table.
     */
    public void renameTable(String schemaName, String oldName, String newName) throws SQLException {
        String sql = String.format("ALTER TABLE \"%s\".\"%s\" RENAME TO \"%s\"",
            escapeIdentifier(schemaName),
            escapeIdentifier(oldName),
            escapeIdentifier(newName));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Table renamed: " + oldName + " to " + newName + " in " + schemaName);
        }
    }

    /**
     * Copy a table structure and data.
     */
    public void copyTable(String schemaName, String fromTable, String toTable) throws SQLException {
        String sql = String.format("CREATE TABLE \"%s\".\"%s\" AS SELECT * FROM \"%s\".\"%s\"",
            escapeIdentifier(schemaName),
            escapeIdentifier(toTable),
            escapeIdentifier(schemaName),
            escapeIdentifier(fromTable));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            logMessage(INFO, "Table copied: " + fromTable + " to " + toTable + " in " + schemaName);
        }
    }

    /**
     * Update all rows in a column with a specific value.
     */
    public void updateColumnValue(String schemaName, String tableName, String columnName, Object value) throws SQLException {
        String sql = String.format("UPDATE \"%s\".\"%s\" SET \"%s\" = ?",
            escapeIdentifier(schemaName),
            escapeIdentifier(tableName),
            escapeIdentifier(columnName));

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, value);
            stmt.executeUpdate();
            logMessage(INFO, "Column " + columnName + " updated in " + schemaName + "." + tableName);
        }
    }

    /**
     * Map Fivetran DataType to SQL type.
     *
     * @param datatype The Fivetran DataType enum value
     * @param column Optional Column containing additional type parameters
     * @return SQL type string
     */
    public String mapDatatypeToSql(DataType datatype, Column column) {
        // Handle DECIMAL specially - needs precision/scale from column definition
        if (datatype == DataType.DECIMAL) {
            if (column.hasParams() && column.getParams().hasDecimal()) {
                int precision = column.getParams().getDecimal().getPrecision();
                int scale = column.getParams().getDecimal().getScale();
                return String.format("DECIMAL(%d, %d)", precision, scale);
            } else {
                // Default fallback when precision/scale not specified
                return "DECIMAL(38, 10)";
            }
        }

        // Handle STRING specially - may have string_byte_length for VARCHAR sizing
        if (datatype == DataType.STRING) {
            if (column.hasParams() && column.getParams().getStringByteLength() > 0) {
                return "VARCHAR(" + column.getParams().getStringByteLength() + ")";
            }
            // Default fallback to unlimited VARCHAR
            return "VARCHAR";
        }

        Map<DataType, String> typeMapping = new HashMap<>();
        typeMapping.put(DataType.BOOLEAN, "BOOLEAN");
        typeMapping.put(DataType.SHORT, "SMALLINT");
        typeMapping.put(DataType.INT, "INTEGER");
        typeMapping.put(DataType.LONG, "BIGINT");
        typeMapping.put(DataType.FLOAT, "REAL");
        typeMapping.put(DataType.DOUBLE, "DOUBLE");
        typeMapping.put(DataType.NAIVE_DATE, "DATE");
        typeMapping.put(DataType.NAIVE_DATETIME, "TIMESTAMP");
        typeMapping.put(DataType.UTC_DATETIME, "TIMESTAMPTZ");
        typeMapping.put(DataType.NAIVE_TIME, "TIME");
        typeMapping.put(DataType.BINARY, "BLOB");
        typeMapping.put(DataType.JSON, "JSON");
        typeMapping.put(DataType.XML, "VARCHAR");
        typeMapping.put(DataType.UNSPECIFIED, "VARCHAR");

        return typeMapping.getOrDefault(datatype, "VARCHAR");
    }

    /**
     * Map SQL type to Fivetran DataType.
     */
    private DataType mapSqlTypeToDatatype(String sqlType) {
        sqlType = sqlType.toUpperCase();

        logMessage(INFO, "Mapping SQL type: " + sqlType);

        if (sqlType.contains("BOOL")) return DataType.BOOLEAN;
        if (sqlType.contains("SMALLINT") || sqlType.contains("INT2")) return DataType.SHORT;
        if (sqlType.contains("INTEGER") || sqlType.contains("INT4") || sqlType.contains("INT ")) return DataType.INT;
        if (sqlType.contains("BIGINT") || sqlType.contains("INT8")) return DataType.LONG;
        if (sqlType.contains("DECIMAL") || sqlType.contains("NUMERIC")) return DataType.DECIMAL;
        // Check DOUBLE before FLOAT because "DOUBLE" contains the word but not "FLOAT"
        if (sqlType.contains("DOUBLE") || sqlType.contains("FLOAT8")) return DataType.DOUBLE;
        // Check for FLOAT - covers FLOAT, REAL, FLOAT4
        if (sqlType.contains("FLOAT") || sqlType.contains("REAL") || sqlType.contains("FLOAT4")) return DataType.FLOAT;
        if (sqlType.contains("DATE") && !sqlType.contains("TIME")) return DataType.NAIVE_DATE;
        if (sqlType.contains("TIMESTAMP") && sqlType.contains("TZ")) return DataType.UTC_DATETIME;
        if (sqlType.contains("TIMESTAMP")) return DataType.NAIVE_DATETIME;
        if (sqlType.contains("TIME")) return DataType.NAIVE_TIME;
        if (sqlType.contains("BLOB") || sqlType.contains("BYTEA")) return DataType.BINARY;
        if (sqlType.contains("JSON")) return DataType.JSON;

        // If no match found, log warning and return STRING as fallback
        logMessage(WARNING, "Unknown SQL type '" + sqlType + "', defaulting to STRING");
        return DataType.STRING;
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
