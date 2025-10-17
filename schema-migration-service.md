# What is Schema Migration 

Schema migration is a service which allows developers to update/add/remove the tables and/or columns from the customers warehouse.
Some times the connector modifies the schema and the changes needs to be reflected in the destination before processing new data for that table. This is in additional to the automated schema update process that is part of the sync. 
Apart from that, we also have a history-mode which allows customers to keep track of history of the particular row, now in order to convert some existing table from legacy/live mode to history mode we need to add Fivetran history mode system columns (_fivetran_start, _fivetran_end, _fivetran_active) to the table and initialize those columns with some proper values, this is also being done by schema migration process.

# Migration Types

- **Sync Mode Migrations**: Migrations performed to migrate from live/legacy mode to history mode or vice versa
- **Standard Migration**: Any migration other than sync mode migrations

# Different migrations supported
Below table represents what information is passed in the `MigrationDetails` object for each `MigrationType`:
| MigrationType | MigrationDetails Object |
|---------------|-------------------------|
| `ADD_COLUMN_WITH_DEFAULT_VALUE` | schema, table, column, column_type, value |
| `SET_COLUMN_TO_NULL` | schema, table, column |
| `UPDATE_COLUMN` | schema, table, column, column_type, value |
| `RENAME_COLUMN` | schema, table, from_column, to_column |
| `RENAME_TABLE` | schema, table, from_table, to_table  |
| `COPY_COLUMN` | schema, table, from_column, to_column |
| `COPY_TABLE` | schema, table, from_table, to_table |
| `COPY_TABLE_TO_HISTORY_MODE` | schema, table, from_table, to_table, deleted_column |
| `MIGRATE_TO_HISTORY_MODE` | schema, table, deleted_column |
| `MIGRATE_HISTORY_TO_LIVE_MODE` | schema, table, keep_inactive_rows |
| `MIGRATE_HISTORY_TO_LEGACY_MODE` | schema, table, deleted_column |
| `MIGRATE_LEGACY_TO_LIVE_MODE` | schema, table, deleted_column |
| `ADD_HISTORY_COLUMN` | schema, table, column_type, value, operation_timestamp |
| `DROP_HISTORY_COLUMN` | schema, table, column, operation_timestamp |


## How to Implement the `migrate` method
Your migrate method should contain handling for all above defined migrations based on the info passed in MigrationRequest which are `MigrationDetails` and `MigrationType`. Each migration type will have its own logic to be implemented.
Let's go through each migration type and what is expected to be implemented in the `migrate` method for that migration type.

## Standard Migrations

### ADD_COLUMN_WITH_DEFAULT_VALUE
This migration should add a new column with a default value. The `MigrationDetails` object will contain the schema, table, column name, column type, and the default value to be set for the new column.
```sql
  ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type> DEFAULT <default_value>;
  ```
If ALTER TABLE query doesnt support DEFAULT clause, then:
1. Add the column without default value:
```sql
  ALTER TABLE <schema.table> ADD COLUMN <column_name> <column_type>;
```
1. Update the column with the default value:
```sql
  UPDATE <schema.table> SET <column_name> = <default_value>;
```
### SET_COLUMN_TO_NULL
This migration should set the specified column to NULL for all rows in the table. The `MigrationDetails` object will contain the schema, table, and column name.
```sql
  UPDATE <schema.table> SET <column_name> = NULL;
```

### UPDATE_COLUMN
This migration should update the specified column with a new default value. The `MigrationDetails` object will contain the schema, table, column name, column type, and the new value to be set for the column.
```sql
  UPDATE <schema.table> SET <column_name> = <new_value>;
```

### RENAME_COLUMN
This migration should rename the specified column in the table. The `MigrationDetails` object will contain the schema, table, from_column (old name), and to_column (new name).
```sql
  ALTER TABLE <schema.table> RENAME COLUMN <from_column> TO <to_column>;
```
If RENAME COLUMN query is not supported, then:
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

If RENAME COLUMN migration returns unsupported error, then:
we fallback to use the alterTable for adding a new column and dropping the old column as described above.
NOTE: new column wont have back dated data


### RENAME_TABLE
This migration should rename the specified table in the schema. The `MigrationDetails` object will contain the schema, table, from_table (old name), and to_table (new name).
```sql
  ALTER TABLE <schema.from_table> RENAME TO <to_table>;
```
If RENAME TABLE query is not supported, then:
1. Create a new table with the new name:
```sql
  CREATE TABLE <schema.to_table> AS SELECT * FROM <schema.from_table>;
```
2. Drop the old table:
```sql
  DROP TABLE <schema.from_table>;
```

if RENAME TABLE migration returns unsupported error, then:
we fallback to use the createTable for creating a new table and dropping the old table as described above.
NOTE: new table wont have back dated data

### COPY_COLUMN
This migration should add a new column and copy the data from the `from_column`to `to_column`. The `MigrationDetails` object will contain the schema, table, from_column (old name), and to_column (new name).
Perform the following steps:
1. Add the new column with the same type as the old column:
```sql
  ALTER TABLE <schema.table> ADD COLUMN <to_column> <column_type>;
```
2. Update the new column with the values from the old column:
```sql
  UPDATE <schema.table> SET <to_column> = <from_column>;
```

Fallback to the above steps if COPY COLUMN migration returns unsupported error, then:
we fallback to use the alterTable for adding a new column with same type as from_column.
NOTE: new column won't have back dated data

### COPY_TABLE
This migration should create a new table and copy the data from the `from_table` to `to_table`. The `MigrationDetails` object will contain the schema, table, from_table (old name), and to_table (new name).
Perform the following steps:
1. Create a new table with the new name:
```sql
  CREATE TABLE <schema.to_table> AS SELECT * FROM <schema.from_table>;
```

Fallback to the above steps if COPY TABLE migration returns unsupported error, then:
we fallback to use the createTable for creating a new table with same schema as from_table.
NOTE: new table won't have back dated data

## Sync Mode Migrations

### ADD_HISTORY_COLUMN
This migration should add column to table in history mode. The `MigrationDetails` object will contain the schema, table, column_type, value, and operation_timestamp.
Perform the following steps:
1. Add the new column with the specified type:
```sql
  ALTER TABLE <schema.table> ADD COLUMN <column_name> TIMESTAMP;
```
2. Check if the table is empty
```sql
  IF (SELECT COUNT(*) FROM <schema.table>) = 0 THEN
      RETURN;
  END IF;
```

3. Insert a new row to record the history of the DDL operation
```sql
  INSERT INTO <schema.table> (<columns>)
  VALUES (<values>);
```
4. Copy the existing active record (_fivetran_active and max(_fivetran_start)) from the table
```sql
  INSERT INTO <schema.table> (<columns>)
  SELECT <columns>
  FROM <schema.table>
  WHERE _fivetran_active = TRUE
    AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM <schema.table> WHERE _fivetran_active = TRUE);
```
5. Update the newly added row with the default value and operation timestamp
```sql
  UPDATE <schema.table>
  SET <column_name> = <value>,
      _fivetran_start = <operation_timestamp>
  WHERE <condition_to_identify_new_row>;
```
6. Update the previous record's _fivetran_end to (operation timestamp) - 1ms
```sql
  UPDATE <schema.table>
  SET _fivetran_end = <operation_timestamp> - INTERVAL '1 millisecond'
  WHERE _fivetran_active = TRUE
    AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM <schema.table> WHERE _fivetran_active = TRUE);
```

### DROP_HISTORY_COLUMN
This migration should drop the history column from the table in history mode. The `MigrationDetails` object will contain the schema, table, column, and operation_timestamp.
Perform the following steps:
1. Check if the table is empty
```sql
  IF (SELECT COUNT(*) FROM <schema.table>) = 0 THEN
      RETURN;
  END IF;
```
2. Insert a new row to record the history of the DDL operation
```sql
  INSERT INTO <schema.table> (<columns>)
  VALUES (<values>);
```
3. Copy the existing active record (_fivetran_active and max(_fivetran_start)) from the table
```sql
  INSERT INTO <schema.table> (<columns>)
  SELECT <columns>
  FROM <schema.table>
  WHERE _fivetran_active = TRUE
    AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM <schema.table> WHERE _fivetran_active = TRUE);
```
4. Update the newly added row with the operation timestamp
```sql
  UPDATE <schema.table>
  SET _fivetran_start = <operation_timestamp>
  WHERE <condition_to_identify_new_row>;
```
5. Update the previous record's _fivetran_end to (operation timestamp) - 1ms
```sql
  UPDATE <schema.table>
  SET _fivetran_end = <operation_timestamp> - INTERVAL '1 millisecond'
  WHERE _fivetran_active = TRUE
    AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM <schema.table> WHERE _fivetran_active = TRUE);
```

### MIGRATE_TO_HISTORY_MODE
This migration should convert the table to history mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Add the history mode columns to the table:
```sql
  ALTER TABLE <schema.table> ADD COLUMN _fivetran_start TIMESTAMP,
                              ADD COLUMN _fivetran_end TIMESTAMP,
                              ADD COLUMN _fivetran_active BOOLEAN DEFAULT TRUE;
```
2. Check the currrent sync_mode `SYNC_MODE`
3. If the sync_mode is `LIVE` then:
    - set all the records as active and set  _fivetran_start, fivetran_end, and _fivetran_active columns appropriately.
    - ```sql
     UPDATE <schema.table>
     SET _fivetran_start = NOW(),
         _fivetran_end = `9999-12-31 23:59:59`,
         _fivetran_active = TRUE;
     ```
4. If the sync_mode is `LEGACY` then:
    - use deleted_column to identify active records and set the values of _fivetran_start, fivetran_end, and _fivetran_active columns appropriately
    ```sql
        UPDATE <schema.table>
        SET 
            _fivetran_active = CASE 
                                   WHEN <deleteColumn> = TRUE THEN FALSE 
                                   ELSE TRUE 
                               END,
            _fivetran_start = CASE 
                                  WHEN <deleteColumn> = TRUE THEN TIMESTAMP '<minTimestamp>' 
                                  ELSE (SELECT MAX(_fivetran_synced) FROM <schema.table>) 
                              END,
            _fivetran_end = CASE 
                                WHEN <deleteColumn> = TRUE THEN TIMESTAMP '<minTimestamp>' 
                                ELSE TIMESTAMP '<maxTimestamp>' 
                            END
        WHERE <condition>;
    ```

### COPY_TABLE_TO_HISTORY_MODE
This migration should copy the table to history mode. The `MigrationDetails` object will contain the schema, table, from_table (old name), to_table (new name), and deleted_column.
Perform the following steps:
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
3. Follow steps in `MIGRATE_TO_HISTORY_MODE` and migrate this new table to history mode.


### MIGRATE_HISTORY_TO_LIVE_MODE
This migration should convert the table from history mode to live mode. The `MigrationDetails` object will contain the schema, table, and keep_inactive_rows.
Perform the following steps:
1. Drop primary key constraint if exists:
    ```sql
      ALTER TABLE <schema.table> DROP CONSTRAINT IF EXISTS <primary_key_constraint>;
    ```
2. If `keep_inactive_rows` is false then drop Deprecated rows ( skip if `keep_inactive_rows` is true):
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

### MIGRATE_HISTORY_TO_LEGACY_MODE
This migration should convert the table from history mode to legacy mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Drop primary key constraint if exists:
    ```sql
      ALTER TABLE <schema.table> DROP CONSTRAINT IF EXISTS <primary_key_constraint>;
    ```
2. If `_fivetran_deleted` doesnt exist then add it:
    ```sql
      ALTER TABLE <schema.table> ADD COLUMN _fivetran_deleted BOOLEAN DEFAULT FALSE;
    ```
3. Delete history records:
4. update `_fivetran_deleted` column based on `_fivetran_active`.
    ```sql
      UPDATE <schema.table>
      SET _fivetran_deleted = CASE 
                                  WHEN _fivetran_active = TRUE THEN FALSE 
                                  ELSE TRUE 
                              END;
    ``` 
   NOTE: also update this to `deleted_column` if it exists and different from `_fivetran_deleted`.
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

### MIGRATE_LEGACY_TO_LIVE_MODE
This migration should convert the table from legacy mode to live mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Drop records where `_fivetran_deleted` is false:
    ```sql
      DELETE FROM <schema.table>
      WHERE _fivetran_deleted = FALSE;
    ```
2. Drop _fivetran_deleted column if exists:
    ```sql
      ALTER TABLE <schema.table> DROP COLUMN _fivetran_deleted;
    ```

### MIGRATE_LIVE_TO_LEGACY_MODE
This migration should convert the table from live mode to legacy mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Add `_fivetran_deleted` column if it does not exist:
    ```sql
      ALTER TABLE <schema.table> ADD COLUMN _fivetran_deleted BOOLEAN DEFAULT TRUE;
    ```
2. Update `_fivetran_deleted` column based on the `deleted_column`:
    ```sql
      UPDATE <schema.table>
      SET _fivetran_deleted = CASE 
                                  WHEN <deleted_column> = TRUE THEN TRUE 
                                  ELSE FALSE 
                              END;
    ```
### ADD_HISTORY_COLUMN
This migration should add column to table in history mode. The `MigrationDetails` object will contain the schema, table, column_type, value, and operation_timestamp.
Perform the following steps:
1. Add the new column with the specified type:
   ```sql
     ALTER TABLE <schema.table> ADD COLUMN <column_name> TIMESTAMP;
   ```
2. Check if the table is empty
   ```sql
     IF (SELECT COUNT(*) FROM <schema.table>) = 0 THEN
         RETURN;
     END IF;
   ```

3. Insert a new row to record the history of the DDL operation 
   1. Copy the existing active record (_fivetran_active and max(_fivetran_start)) from the table
   ```sql
   INSERT INTO <schema.table> (<existing_columns>, <new_column>) 
    ( SELECT <existing_columns>, <expectedValue> as <new_column>, <operation_timestamp> as "_FIVETRAN_START"  
   FROM <schema.table>
    WHERE _fivetran_active = TRUE              
    AND _fivetran_start < <operation_timestamp>
   ```
   2. Update the newly added row with the default value and operation timestamp
   
   ```sql
    UPDATE <schema.table> SET <new_column> = <expectedValue> WHERE "_FIVETRAN_START" = <operation_timestamp> )
   ```

   3. Update the previous record's _fivetran_end to (operation timestamp) - 1ms
   
   ```sql
   UPDATE <schema.table> SET   
   "_FIVETRAN_END" = <operation_timestamp> - INTERVAL '1 millisecond',
    "_FIVETRAN_ACTIVE" = FALSE 
   WHERE   
   "_FIVETRAN_ACTIVE" = TRUE AND
   "_FIVETRAN_START" < <operation_timestamp>
   ```
   
   | id | name  | new_column | _fivetran_start      | _fivetran_end          | _fivetran_active |
   |----|-------|------------|---------------------|------------------------|------------------|
   | 1  | Alice | null       | 2025-10-10 12:00:00 | 2025-10-15 12:00:00    | FALSE            |
   | 1  | Alice | <default>  | 2025-10-15 12:00:01 | 9999-12-31 23:59:59    | TRUE             |
   | 2  | Ben   | null       | 2025-10-10 12:00:00 | 2025-10-15 12:00:00    | FALSE            |
   | 2  | Ben   | <default>  | 2025-10-15 12:00:01 | 9999-12-31 23:59:59    | TRUE             |
      

### DROP_HISTORY_COLUMN
This migration should drop the history column from the table in history mode. The `MigrationDetails` object will contain the schema, table, column, and operation_timestamp.
Perform the following steps:
1. Check if the table is empty
   ```sql
     IF (SELECT COUNT(*) FROM <schema.table>) = 0 THEN
         RETURN;
     END IF;
   ```
2. Insert a new row to record the history of the DDL operation
   ```sql
     INSERT INTO <schema.table> (<columns>)
     VALUES (<values>);
   ```
3. Copy the existing active record (_fivetran_active and max(_fivetran_start)) from the table
   ```sql
     INSERT INTO <schema.table> (<columns>)
     SELECT <columns>
     FROM <schema.table>
     WHERE _fivetran_active = TRUE
       AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM <schema.table> WHERE _fivetran_active = TRUE);
   ```
4. Update the newly added row with the operation timestamp
   ```sql
     UPDATE <schema.table>
     SET _fivetran_start = <operation_timestamp>
     WHERE <condition_to_identify_new_row>;
   ```
5. Update the previous record's _fivetran_end to (operation timestamp) - 1ms
   ```sql
     UPDATE <schema.table>
     SET _fivetran_end = <operation_timestamp> - INTERVAL '1 millisecond'
     WHERE _fivetran_active = TRUE
       AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM <schema.table> WHERE _fivetran_active = TRUE);
   ```
      
   | id | name  | deleted_column | _fivetran_start      | _fivetran_end          | _fivetran_active |
   |----|-------|-----------|---------------------|------------------------|------------------|
   | 1  | Alice | abc       | 2025-10-10 12:00:00 | 2025-10-15 12:00:00    | FALSE            |
   | 1  | Alice | <default> | 2025-10-15 12:00:01 | 9999-12-31 23:59:59    | TRUE             |
   

### MIGRATE_TO_HISTORY_MODE
This migration should convert the table to history mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Add the history mode columns to the table:
```sql
  ALTER TABLE <schema.table> ADD COLUMN _fivetran_start TIMESTAMP,
                              ADD COLUMN _fivetran_end TIMESTAMP,
                              ADD COLUMN _fivetran_active BOOLEAN DEFAULT TRUE;
```
2. Check the currrent sync_mode `SYNC_MODE`
3. If the sync_mode is `LIVE` then:
    - set all the records as active and set  _fivetran_start, fivetran_end, and _fivetran_active columns appropriately.
    - ```sql
     UPDATE <schema.table>
     SET _fivetran_start = NOW(),
         _fivetran_end = `9999-12-31 23:59:59`,
         _fivetran_active = TRUE;
     ```
4. If the sync_mode is `LEGACY` then:
    - use deleted_column to identify active records and set the values of _fivetran_start, fivetran_end, and _fivetran_active columns appropriately
    ```sql
        UPDATE <schema.table>
        SET 
            _fivetran_active = CASE 
                                   WHEN <deleteColumn> = TRUE THEN FALSE 
                                   ELSE TRUE 
                               END,
            _fivetran_start = CASE 
                                  WHEN <deleteColumn> = TRUE THEN TIMESTAMP '<minTimestamp>' 
                                  ELSE (SELECT MAX(_fivetran_synced) FROM <schema.table>) 
                              END,
            _fivetran_end = CASE 
                                WHEN <deleteColumn> = TRUE THEN TIMESTAMP '<minTimestamp>' 
                                ELSE TIMESTAMP '<maxTimestamp>' 
                            END
        WHERE <condition>;
    ```

### COPY_TABLE_TO_HISTORY_MODE
This migration should copy the table to history mode. The `MigrationDetails` object will contain the schema, table, from_table (old name), to_table (new name), and deleted_column.
Perform the following steps:
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
3. Follow steps in `MIGRATE_TO_HISTORY_MODE` and migrate this new table to history mode.


### MIGRATE_HISTORY_TO_LIVE_MODE
This migration should convert the table from history mode to live mode. The `MigrationDetails` object will contain the schema, table, and keep_inactive_rows.
Perform the following steps:
1. Drop primary key constraint if exists:
    ```sql
      ALTER TABLE <schema.table> DROP CONSTRAINT IF EXISTS <primary_key_constraint>;
    ```
2. If `keep_inactive_rows` is false then drop Deprecated rows ( skip if `keep_inactive_rows` is true):
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

### MIGRATE_HISTORY_TO_LEGACY_MODE
This migration should convert the table from history mode to legacy mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Drop primary key constraint if exists:
    ```sql
      ALTER TABLE <schema.table> DROP CONSTRAINT IF EXISTS <primary_key_constraint>;
    ```
2. If `_fivetran_deleted` doesnt exist then add it:
    ```sql
      ALTER TABLE <schema.table> ADD COLUMN _fivetran_deleted BOOLEAN DEFAULT FALSE;
    ```
3. Delete history records:
4. update `_fivetran_deleted` column based on `_fivetran_active`.
    ```sql
      UPDATE <schema.table>
      SET _fivetran_deleted = CASE 
                                  WHEN _fivetran_active = TRUE THEN FALSE 
                                  ELSE TRUE 
                              END;
    ``` 
   NOTE: also update this to `deleted_column` if it exists and different from `_fivetran_deleted`.
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

### MIGRATE_LEGACY_TO_LIVE_MODE
This migration should convert the table from legacy mode to live mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Drop records where `_fivetran_deleted` is false:
    ```sql
      DELETE FROM <schema.table>
      WHERE _fivetran_deleted = FALSE;
    ```
2. Drop _fivetran_deleted column if exists:
    ```sql
      ALTER TABLE <schema.table> DROP COLUMN _fivetran_deleted;
    ```

### MIGRATE_LIVE_TO_LEGACY_MODE
This migration should convert the table from live mode to legacy mode. The `MigrationDetails` object will contain the schema, table, and deleted_column.
Perform the following steps:
1. Add `_fivetran_deleted` column if it does not exist:
    ```sql
      ALTER TABLE <schema.table> ADD COLUMN _fivetran_deleted BOOLEAN DEFAULT TRUE;
    ```
2. Update `_fivetran_deleted` column based on the `deleted_column`:
    ```sql
      UPDATE <schema.table>
      SET _fivetran_deleted = CASE 
                                  WHEN <deleted_column> = TRUE THEN TRUE 
                                  ELSE FALSE 
                              END;
    ```
