# Schema Migration Helper Guide

## Why have a Schema Migration Helper?

Schema migrations are a general mechanism that Fivetran uses to manage DDL/DML operations on existing destination tables. These migrations are occasionally needed to manage certain DDL/DML operations in a customer's destination without requiring the customer to do it or fix the schema manually.

Some frequently used schema migrations have been grouped into different schema migration operations defined below. These grouped operations provide standardized abstractions that are advantageous for partners to implement, as they handle common scenarios with built-in best practices for data integrity and history preservation.

> Important: If some migration operations are not implemented, customer may observe unexpected behaviours or failures during schema migration requests.

There can be multiple reasons for these migrations:
- Table/Column level migrations: Sometimes, source connectors introduce schema changes that require data transformation or restructuring and may trigger bulk fixes or handle special cases. It's crucial to apply these schema changes to the destination before processing new data for the affected table. These changes are less common than basic schema updates—such as adding, dropping, or modifying columns—which are handled by `AlterTable` RPC method.
- Table sync mode migrations: Customers can trigger migrations to convert their existing tables from one sync mode to another ([soft-delete mode](https://fivetran.com/docs/core-concepts/sync-modes/soft-delete#softdeletemode) / [history mode](https://fivetran.com/docs/core-concepts/sync-modes/history-mode#switchingmodes) / live mode). These migrations involve a series of data transformations that are easier to execute as a single grouped schema migration helper abstraction to maintain history and deleted row information.

> Note: Basic schema migrations such as adding/dropping columns, changing data types, and modifying primary keys are automatically handled by Fivetran through the `AlterTable` RPC call. The Schema Migration Helper Service described in this document handles more complex migration scenarios that cannot be achieved through standard `AlterTable` operations alone.

This document guides on how to implement the `Migrate` RPC method for different types of schema migrations.

> Note for partners: LIVE mode, a new sync mode, will be rolled out in Dec 2025. Please ensure your implementation is ready to support the new centrally enabled live mode functionality. Further communication from Fivetran regarding the rollout plan and timeline for live mode will be shared soon.
---

### How to implement the migrate method

Your `Migrate` RPC method should handle all defined migrations based on the `MigrationDetails` object passed in the request.

#### MigrationDetails object structure

| Field        | Type      | Description                                         |
|--------------|-----------|-----------------------------------------------------|
| `schema`     | string    | The schema where the table is located               |
| `table`      | string    | The table to be migrated                            |
| `operation`  | Operation | The specific migration operation to be performed    |

#### Supported operation types

| Operation type                    | Purpose                         | Sub-operations                                         |
|-----------------------------------|---------------------------------|--------------------------------------------------------|
| `AddOperation`                    | Add column                      | `ADD_COLUMN_WITH_DEFAULT_VALUE`, `ADD_COLUMN_IN_HISTORY_MODE` |
| `UpdateColumnValueOperation`      | Update data values in column    | N/A                                                    |
| `RenameOperation`                 | Rename table or column          | `RENAME_TABLE`, `RENAME_COLUMN`                        |
| `CopyOperation`                   | Copy table or column            | `COPY_TABLE`, `COPY_TABLE_TO_HISTORY_MODE`, `COPY_COLUMN` |
| `DropOperation`                   | Drop table or column            | `DROP_TABLE`, `DROP_COLUMN_IN_HISTORY_MODE`            |
| `TableSyncModeMigrationOperation` | Migrate table between sync modes | `LIVE_TO_HISTORY`, `SOFT_DELETE_TO_HISTORY`, `HISTORY_TO_LIVE`, `HISTORY_TO_SOFT_DELETE`, `SOFT_DELETE_TO_LIVE`, `LIVE_TO_SOFT_DELETE` |

Each operation type has its own set of fields required to perform the migration. Based on the operation field in the request, your `migrate` method should implement the corresponding SQL queries.

---

## Operation details and example SQL

### Add operation

#### ADD_COLUMN_WITH_DEFAULT_VALUE

This migration should add a new column with the specified column type and default value.

Request details:

| Field                                 | Type     | Description                    |
|----------------------------------------|----------|--------------------------------|
| `AddColumnWithDefaultValue.column`     | string   | The name of the column to add  |
| `AddColumnWithDefaultValue.column_type`| DataType | The data type of the new column|
| `AddColumnWithDefaultValue.default_value`| value  | The default value to set for the new column |

Implementation:
```sql
ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type> DEFAULT <default_value>;
```
If the ALTER TABLE query doesn't support the DEFAULT clause, then:

1. Add the column without a default value:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type>;
    ```
2. Update the column with the default value:
    ```sql
    UPDATE <schema.table> SET <column_name> = <default_value>;
    ```

> Note: If the ADD_COLUMN_WITH_DEFAULT_VALUE migration results in an unsupported error, Fivetran will fall back to using the already implemented `AlterTable` RPC implementation to create a new table. Please note that the new table will not contain any historical (back-dated) data.

---

#### ADD_COLUMN_IN_HISTORY_MODE

This migration should add a column to a table in history mode.  
The idea is to record the history of the DDL operation (add column with default value) by inserting new rows with the default value and updating the existing active records accordingly.

Request details:

| Field                                   | Type      | Description                                   |
|------------------------------------------|-----------|-----------------------------------------------|
| `AddColumnInHistoryMode.column`          | string    | The name of the column to add                 |
| `AddColumnInHistoryMode.column_type`     | DataType  | The data type of the new column               |
| `AddColumnInHistoryMode.default_value`   | value     | The default value for the new column          |
| `AddColumnInHistoryMode.operation_timestamp` | timestamp | The timestamp of the migration operation      |

- `operation_timestamp` is the timestamp of the DDL operation trigger and is used to set the `_fivetran_start`, `_fivetran_end`, and `_fivetran_active` values appropriately to maintain history mode integrity.

Implementation:

- Validation before starting the migration:
   - Ensure that the table is not empty. If it is empty, the migration can be skipped as there are no records to maintain history for.
   - Ensure `max(_fivetran_start)` < `operation_timestamp` for all active records.

1. Add the new column with the specified type:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type>;
    ```
2. Insert new rows to record the history of the DDL operation:
    ```sql
    INSERT INTO {schema.table} (<column_list>)
    (
      SELECT 
        <unchanged_cols>, 
        {default_value}::{column_type} as {column_name}, 
        {operation_timestamp} as _fivetran_start 
      FROM {schema.table} 
      WHERE 
          _fivetran_active = true
          AND _fivetran_start < {operation_timestamp}
    );
    ```
3. Update the newly added rows with the `default_value` and `operation_timestamp`:
    ```sql
    UPDATE <schema.table>
    SET <column> = default_value,
        _fivetran_start = <operation_timestamp>
    WHERE <condition_to_identify_new_row>;
    ```
4. Update the previous active record's `_fivetran_end` to `(operation timestamp) - 1ms` and set `_fivetran_active` to `FALSE`:
    ```sql
    UPDATE <schema.table>
    SET _fivetran_end = <operation_timestamp> - INTERVAL '1 millisecond',
        _fivetran_active = FALSE
    WHERE _fivetran_active = TRUE
      AND _fivetran_start < <operation_timestamp>;
    ```

---

### UPDATE_COLUMN_VALUE_OPERATION

This migration should update the specified column with a new value.

Request details:

| Field                              | Type  | Description                           |
|-------------------------------------|-------|---------------------------------------|
| `UpdateColumnValueOperation.column` | string| The name of the column to update      |
| `UpdateColumnValueOperation.value`  | value | The new value to set (can be `NULL`)  |

Implementation:
```sql
UPDATE <schema.table> SET <column_name> = <new_value>;
```
> Note: `NULL` can also be expected as a valid value to update the column.

---

### Rename operation

#### RENAME_TABLE

This migration should rename the specified table in the schema.

Request details:

| Field                      | Type   | Description                     |
|----------------------------|--------|---------------------------------|
| `RenameTable.from_table`   | string | The current table name (old name)|
| `RenameTable.to_table`     | string | The new table name              |

Implementation:
```sql
ALTER TABLE <schema.from_table> RENAME TO <to_table>;
```

Alternative approach (if RENAME TABLE is not supported):

1. Create a new table with the new name and copy data from the old table:
    ```sql
    CREATE TABLE <schema.to_table> AS SELECT * FROM <schema.from_table>;
    ```
2. Drop the old table:
    ```sql
    DROP TABLE <schema.from_table>;
    ```

> Note: If the RENAME TABLE migration results in an unsupported error, fivetran will fall back to using the already implemented `AlterTable` RPC implementation to create a new table. Please note that the new table will not contain any historical (back-dated) data.

---

#### RENAME_COLUMN

This migration should rename the specified column in the table.

Request details:

| Field                         | Type   | Description                      |
|-------------------------------|--------|----------------------------------|
| `RenameColumn.from_column`     | string | The current column name (old name)|
| `RenameColumn.to_column`       | string | The new column name              |

Implementation:
```sql
ALTER TABLE <schema.table> RENAME COLUMN <from_column> TO <to_column>;
```

Alternative approach (if RENAME COLUMN is not supported):

1. Add the new column with the same type as the old column:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN <to_column> <column_type>;
    ```
2. Update the new column with the values from the old column:
    ```sql
    UPDATE <schema.table> SET <to_column> = <from_column>;
    ```
3. Drop the old column:
    ```sql
    ALTER TABLE <schema.table> DROP COLUMN <from_column>;
    ```

> Note: If the RENAME COLUMN migration returns an unsupported error, Fivetran will fall back to using the already implemented `AlterTable` RPC to add a column with the new name. The new column won't have back-dated data.

---

### Copy operation

#### COPY_TABLE

This migration should create a new table and copy the data from the source table to the destination table.

Request details:

| Field                   | Type   | Description             |
|-------------------------|--------|-------------------------|
| `CopyTable.from_table`  | string | The source table name   |
| `CopyTable.to_table`    | string | The destination table name|

Implementation:

1. Create a new table with the new name:
    ```sql
    CREATE TABLE <schema.to_table> AS SELECT * FROM <schema.from_table>;
    ```

> Note: If the COPY TABLE migration returns an unsupported error, Fivetran will fall back to using the already implemented `CreateTable` RPC to create a new table with the same schema as from_table. The new table won't have data copied from the source table.

---

#### COPY_COLUMN

This migration should add a new column and copy the data from the source column to the destination column.

Request details:

| Field                       | Type   | Description                |
|-----------------------------|--------|----------------------------|
| `CopyColumn.from_column`    | string | The source column name     |
| `CopyColumn.to_column`      | string | The destination column name|

Implementation:

1. Add the new column with the same type as the old column:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN <to_column> <column_type>;
    ```
2. Update the new column with the values from the old column:
    ```sql
    UPDATE <schema.table> SET <to_column> = <from_column>;
    ```

> Note: If the COPY COLUMN migration returns an unsupported error, Fivetran will fall back to using the already implemented `AlterTable` RPC to add a new column with the same type as from_column. The new column won't have data as in the source column.

---

#### COPY_TABLE_TO_HISTORY_MODE

This migration should copy an existing table from a non-history mode to a new table in history mode.

Request details:

| Field                             | Type   | Description                    |
|-----------------------------------|--------|--------------------------------|
| `CopyTableToHistoryMode.from_table`| string | The source table name          |
| `CopyTableToHistoryMode.to_table`  | string | The destination table name     |
| `CopyTableToHistoryMode.soft_deleted_column` | string | The soft delete column name (if applicable) |

Implementation:

1. Create a new table with the new name and add the history mode columns:
    ```sql
    CREATE TABLE <schema.to_table> (
        <columns>
    );
    ```
2. Copy the data from the old table to the new table:
    ```sql
    INSERT INTO <schema.to_table> (<columns>)
    SELECT <columns>
    FROM <schema.from_table>;
    ```
3. Follow steps in the sync mode migration `SOFT_DELETE_TO_HISTORY` if `soft_deleted_column` is not null, OR `LIVE_TO_HISTORY` in order to migrate it to history mode.

---

### Drop operation

#### DROP_TABLE

This migration should drop the specified table.

Request details:

| Field                     | Type    | Description                       |
|---------------------------|---------|-----------------------------------|
| `DropOperation.drop_table`| boolean | Indicates if the table should be dropped |

Implementation:
```sql
DROP TABLE <schema.table>;
```

---

#### DROP_COLUMN_IN_HISTORY_MODE

This migration should drop a column from a table in history mode while maintaining history mode integrity.

Request details:

| Field                                | Type      | Description                                   |
|---------------------------------------|-----------|-----------------------------------------------|
| `DropColumnInHistoryMode.column`      | string    | The name of the column to drop                |
| `DropColumnInHistoryMode.operation_timestamp` | timestamp | The timestamp of the migration operation      |

- `operation_timestamp` is the timestamp of the DDL operation trigger and is used to set the `_fivetran_start`, `_fivetran_end`, and `_fivetran_active` values appropriately to maintain history mode integrity.

Implementation:
- Implementation is similar to the `ADD_COLUMN_IN_HISTORY_MODE` migration.

Validation before starting the migration:
- Ensure that the table is not empty. If it is empty, the migration can be skipped as there are no records to maintain history for.
- Ensure `max(_fivetran_start) < operation_timestamp` for all active records.

1. Insert new rows to record the history of the DDL operation:
    ```sql
    INSERT INTO {schema.table} (<column_list>)
    (
        SELECT 
          <unchanged_cols>, 
          NULL as {column_name}, 
          {operation_timestamp} as _fivetran_start 
        FROM {schema.table} 
        WHERE 
            _fivetran_active
            AND {column_name} IS NOT NULL
            AND _fivetran_start < {operation_timestamp}
    );
    ```
2. Update the newly added row with the `operation_timestamp`:
    ```sql
    UPDATE {schema.table} 
    SET {column_name} = NULL
    WHERE _fivetran_start = {operation_timestamp};
    ```
3. Update the previous record's `_fivetran_end` to `(operation timestamp) - 1ms` and set `_fivetran_active` to `FALSE`:
    ```sql
    UPDATE {schema.table} 
       SET 
         _fivetran_end = {operation_timestamp} - 1, 
         _fivetran_active = FALSE 
       WHERE 
         _fivetran_active = true AND
         {column} IS NOT NULL AND
         _fivetran_start < {operation_timestamp};
    ```

---

### Table sync mode migrations

These migrations convert tables from one sync mode to another. The `MigrationDetails`, along with `schema` and `table`, contains the `TableSyncModeMigrationType` field to determine which migration to perform.

Common request fields:

| Field                                           | Type    | Description                                              |
|-------------------------------------------------|---------|----------------------------------------------------------|
| `TableSyncModeMigrationOperation.soft_deleted_column` | string | The soft delete column name (used in applicable migrations)|
| `TableSyncModeMigrationOperation.keep_deleted_rows`   | boolean| Whether to keep deleted rows (used in applicable migrations)|

`soft_deleted_column`: In most cases, this will be `_fivetran_deleted`, but it may be a different column depending on the specific request.

---

#### LIVE_TO_HISTORY

This migration converts a table from live mode to history mode.

Implementation:

1. Add the history mode columns to the table:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN _fivetran_start TIMESTAMP AS PRIMARY KEY,
                                ADD COLUMN _fivetran_end TIMESTAMP,
                                ADD COLUMN _fivetran_active BOOLEAN DEFAULT TRUE;
    ```
2. Set all the records as active and set the `_fivetran_start`, `_fivetran_end`, and `_fivetran_active` columns appropriately.
    ```sql
    UPDATE <schema.table>
    SET _fivetran_start = NOW(),
        _fivetran_end = '9999-12-31 23:59:59',
        _fivetran_active = TRUE;
    ```

---

#### SOFT_DELETE_TO_HISTORY

This migration converts a table from SOFT DELETE to HISTORY mode.

Implementation:

1. Add the history mode columns to the table:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN _fivetran_start TIMESTAMP AS PRIMARY KEY,
                                ADD COLUMN _fivetran_end TIMESTAMP,
                                ADD COLUMN _fivetran_active BOOLEAN DEFAULT TRUE;
    ```
2. Use `soft_deleted_column` to identify active records and set the values of `_fivetran_start`, `_fivetran_end`, and `_fivetran_active` columns appropriately:
    ```sql
    UPDATE <schema.table>
    SET 
        _fivetran_active = CASE 
                               WHEN <soft_deleted_column> = TRUE THEN FALSE 
                               ELSE TRUE 
                           END,
        _fivetran_start = CASE 
                              WHEN <soft_deleted_column> = TRUE THEN TIMESTAMP '<minTimestamp>' 
                              ELSE (SELECT MAX(_fivetran_synced) FROM <schema.table>) 
                          END,
        _fivetran_end = CASE 
                            WHEN <soft_deleted_column> = TRUE THEN TIMESTAMP '<minTimestamp>' 
                            ELSE TIMESTAMP '<maxTimestamp>' 
                        END
    WHERE <condition>;
    ```

---

#### HISTORY_TO_LIVE

This migration converts a table from HISTORY to LIVE mode.

Implementation:

1. Drop the primary key constraint if it exists:
    ```sql
    ALTER TABLE <schema.table> DROP CONSTRAINT IF EXISTS <primary_key_constraint>;
    ```
2. If `keep_deleted_rows` is `FALSE`, then drop rows which are not active (skip if `keep_deleted_rows` is `TRUE`):
    ```sql
    DELETE FROM <schema.table>
    WHERE _fivetran_active = FALSE;
    ```
3. Drop the history mode columns:
    ```sql
    ALTER TABLE <schema.table> DROP COLUMN _fivetran_start,
                                DROP COLUMN _fivetran_end,
                                DROP COLUMN _fivetran_active;
    ```
4. Recreate the primary key constraint if it was dropped in step 1:
    ```sql
    ALTER TABLE <schema.table> ADD CONSTRAINT <primary_key_constraint> PRIMARY KEY (<columns>);
    ```

---

#### HISTORY_TO_SOFT_DELETE

This migration converts a table from HISTORY mode to SOFT DELETE mode.

Implementation:

1. Drop the primary key constraint if it exists:
    ```sql
    ALTER TABLE <schema.table> DROP CONSTRAINT IF EXISTS <primary_key_constraint>;
    ```
2. If `_fivetran_deleted` doesn't exist, then add it:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN _fivetran_deleted BOOLEAN DEFAULT FALSE;
    ```
3. Delete history records for the whole table.
4. Update the `soft_deleted_column` column based on `_fivetran_active`:
    ```sql
    UPDATE <schema.table>
    SET <soft_deleted_column> = CASE 
                                WHEN _fivetran_active = TRUE THEN FALSE 
                                ELSE TRUE 
                            END;
    ``` 
   > NOTE: `soft_deleted_column` can be `_fivetran_deleted` or any other column based on the request.

5. Drop the history mode columns:
    ```sql
    ALTER TABLE <schema.table> DROP COLUMN _fivetran_start,
                                DROP COLUMN _fivetran_end,
                                DROP COLUMN _fivetran_active;
    ```
6. Recreate the primary key constraint if it was dropped in step 1:
    ```sql
    ALTER TABLE <schema.table> ADD CONSTRAINT <primary_key_constraint> PRIMARY KEY (<columns>);
    ```

---

#### SOFT_DELETE_TO_LIVE

This migration converts a table from soft-delete mode to live mode.

Implementation:

1. Drop records where `<soft_deleted_column>`, from the migration request, is true:
    ```sql
    DELETE FROM <schema.table>
    WHERE <soft_deleted_column> = TRUE;
    ```
2. if `soft_deleted_column = _fivetran_deleted` column, then drop it:
    ```sql
    ALTER TABLE <schema.table> DROP COLUMN _fivetran_deleted;
    ```

---

#### LIVE_TO_SOFT_DELETE

This migration converts a table from live mode to soft-delete mode.

Implementation:

1. Add the `<soft_deleted_column>` column if it does not exist:
    ```sql
    ALTER TABLE <schema.table> ADD COLUMN <soft_deleted_column> BOOLEAN;
    ```
2. Update `<soft_deleted_column>`:
    ```sql
    UPDATE <schema.table>
    SET <soft_deleted_column> = FALSE
      WHERE <soft_deleted_column> IS NULL;
    ```

---