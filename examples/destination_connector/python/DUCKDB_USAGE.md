# DuckDB Usage Guide

The Python destination connector now uses an actual DuckDB instance instead of in-memory `table_map` dictionary.

## What Changed

Previously, the connector used an in-memory Python dictionary (`table_map = {}`) to simulate table storage. Now it uses **DuckDB**, providing:

- **Real database operations**: Actual SQL DDL/DML instead of dictionary manipulation
- **Persistence**: Data survives between connector restarts (when using file-based DB)
- **Realistic example**: Demonstrates how to build a real destination connector
- **Query capability**: Inspect data using standard SQL tools (DuckDB CLI, DBeaver, etc.)

## Connection Management

The connector maintains a **persistent connection** to the DuckDB database:
- Connection opens when server starts
- Stays open during all operations (fast performance)
- Closes when server stops
- Standard application server pattern

## Database Lock Behavior

DuckDB uses exclusive file locking:
- **While server runs**: Database file is locked
- **After server stops**: Lock is released, file is accessible
- This is the standard pattern for database applications

## Starting the Connector

### Default Configuration
By default, the connector uses a file-based DuckDB database named `destination.db`:

```bash
python main.py --port 50052
```

This creates/uses a file called `destination.db` in the current directory.

### Customizing Database Location
To use a different database file or in-memory database, modify the `DuckDBHelper` initialization in `main.py`:

```python
# File-based database (default)
DestinationImpl.db_helper = DuckDBHelper("destination.db")

# In-memory database (no persistence)
DestinationImpl.db_helper = DuckDBHelper(":memory:")

# Custom path
DestinationImpl.db_helper = DuckDBHelper("/path/to/my_database.db")
```

## Accessing the Database

### While Connector is Running

The database is locked while the connector is running. Use one of these approaches:

#### Option 1: Stop and Inspect (Recommended) ✅
```bash
# Stop the connector
Ctrl+C

# Access with DuckDB CLI or DBeaver
duckdb destination.db

# Restart when done
python main.py
```

#### Option 2: Read-Only Access
```bash
# DuckDB CLI
duckdb destination.db -readonly

# DBeaver connection URL
jdbc:duckdb:/path/to/destination.db?access_mode=READ_ONLY
```

#### Option 3: Use In-Memory Database for Testing
```python
# Modify main.py to use in-memory database:
# Change: DestinationImpl.db_helper = DuckDBHelper("destination.db")
# To:     DestinationImpl.db_helper = DuckDBHelper(":memory:")
# Then restart: python main.py
```

### After Stopping the Connector

```bash
# DuckDB CLI
duckdb destination.db

# Query freely
SELECT * FROM main.transaction;
SHOW TABLES;
```

**DBeaver**: Connect normally - full read/write access.

## Stopping the Connector

Press `Ctrl+C` to gracefully shutdown. The connector will:
1. Stop accepting new requests
2. Complete any in-flight operations
3. Shut down the gRPC server

No database cleanup needed!

## Database Schema

Tables are created in schemas as specified by the connector requests. Default schema is typically `main`.

View tables:
```sql
SHOW TABLES;
SELECT * FROM information_schema.tables;
```

## Migration Operations

All schema migrations are performed as actual DDL operations:
- **CreateTable**: `CREATE TABLE`
- **AlterTable**: `ALTER TABLE ADD COLUMN`
- **Truncate**: `TRUNCATE TABLE`
- **DropTable**: `DROP TABLE`
- **RenameTable**: `ALTER TABLE RENAME TO`
- **AddColumn**: `ALTER TABLE ADD COLUMN`
- **DropColumn**: `ALTER TABLE DROP COLUMN`
- **RenameColumn**: `ALTER TABLE RENAME COLUMN`

## Tips

1. **Development**: Use in-memory database (`:memory:`) to avoid lock issues
2. **Testing**: Use persistent database to inspect data between runs
3. **Debugging**: Stop connector → inspect with DuckDB CLI → restart connector
4. **Production**: Ensure proper shutdown handlers are in place

## Implementation Details

### Files Modified

- **`main.py`**: Updated to use `DuckDBHelper` instead of `table_map`
  - Replaced in-memory dictionary with DuckDB connection
  - Updated all RPC methods (CreateTable, AlterTable, DescribeTable, Truncate)
  - Added error handling for database operations

- **`schema_migration_helper.py`**: Refactored to perform actual database operations
  - Changed from `table_map` manipulation to DuckDB DDL operations
  - All migration operations now execute real SQL commands
  - Added new helper methods for database-level column operations

- **`duckdb_helper.py`**: Already existed, provides DuckDB abstraction layer
  - Handles SQL type mapping
  - Provides methods for common DDL operations
  - Manages connection lifecycle
