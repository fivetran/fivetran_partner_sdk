# What is Schema Migration 

Schema migration is a service which allows developers to update/add/remove the tables and/or columns from the customers warehouse.
Some times the connector modifies the schema and the changes needs to be reflected in the destination before processing new data for that table. This is in additional to the automated schema update process that is part of the sync. 
Apart from that, we also have a history-mode which allows customers to keep track of history of the particular row, now in order to convert some existing table from legacy/live mode to history mode we need to add Fivetran history mode system columns (_fivetran_start, _fivetran_end, _fivetran_active) to the table and initialize those columns with some proper values, this is also being done by schema migration process.

# Migration Types

- **Sync Mode Migrations**: Migrations performed to migrate from live/legacy mode to history mode or vice versa
- **Standard Migration**: Any migration other than sync mode migrations

# Different migrations supported
We are currently supporting following migrations,

- ADD COLUMN
- ADD COLUMN WITH DEFAULT VALUE
- ADD HISTORY COLUMN
- COPY COLUMN
- COPY TABLE
- DROP COLUMN
- DROP HISTORY COLUMN
- CHANGE DATA TYPE
- SET COLUMN TO NULL
- UPDATE COLUMN
- MIGRATE LEGACY TO LIVE MODE
- MIGRATE HISTORY TO LIVE MODE
- MIGRATE HISTORY TO LEGACY MODE
- MIGRATE LIVE TO HISTORY MODE
- COPY TABLE TO HISTORY MODE
- DROP TABLE
- RENAME COLUMN
- RENAME TABLE

## Sync Mode Migration Steps

### Migration to History Mode

1. Add History Mode Column

```sql
ALTER TABLE users
  ADD COLUMN _fivetran_start TIMESTAMP,
  ADD COLUMN _fivetran_end TIMESTAMP,
  ADD COLUMN _fivetran_active BOOLEAN;
```

2. Check the existing Mode 
   1. **Live Mode**: set all the records as active and set  _fivetran_start, fivetran_end, and _fivetran_active columns appropriately.
      
```sql
UPDATE users
SET
_fivetran_active = TRUE,
_fivetran_start = _fivetran_synced,
_fivetran_end = '9999-12-31 23:59:59';
```


   2. **Legacy Mode**:  use deleted_column to identify active records and set the values of _fivetran_start, fivetran_end, and _fivetran_active columns appropriately

```sql
UPDATE users
SET
  _fivetran_active = CASE
    WHEN deleted_column = FALSE THEN TRUE
    ELSE FALSE
  END,
  _fivetran_start = _fivetran_synced,
  _fivetran_end = CASE
    WHEN deleted_column = FALSE THEN '9999-12-31 23:59:59'
    ELSE _fivetran_synced
  END;
```

### Migrate to Live Mode
1. Drop primary keys
```sql
ALTER TABLE users DROP CONSTRAINT users_pkey;
```
2. Drop deprecated rows (where _fivetran_active = false)
```sql
DELETE FROM users WHERE _fivetran_active = FALSE;
```
3. Drop History Mode columns (_fivetran_start, fivetran_end, and _fivetran_active)
```sql
ALTER TABLE users
  DROP COLUMN _fivetran_start,
  DROP COLUMN _fivetran_end,
  DROP COLUMN _fivetran_active;
```
4. Add primary keys back
```sql
ALTER TABLE users ADD PRIMARY KEY (id);
```

### Migrate to Legacy mode
1. Drop primary keys
```sql
ALTER TABLE users DROP CONSTRAINT users_pkey;
```
2. If the keepInactiveRows flag is set to true, keep only the last record for each primary key (irrespective of the _fivetran_active value)
Else, drop inactive rows (where _fivetran_active = false)

```sql
#Keep only the last record per primary key (highest `_fivetran_synced`):
DELETE FROM users
WHERE id NOT IN (
  SELECT id FROM (
    SELECT id, MAX(_fivetran_synced) AS max_synced
    FROM users
    GROUP BY id
  ) AS latest
  JOIN users u ON u.id = latest.id AND u._fivetran_synced = latest.max_synced
);
```

```sql
Drop inactive rows:
DELETE FROM users WHERE _fivetran_active = FALSE;
```

3. Drop History Mode columns (_fivetran_start, fivetran_end, and _fivetran_active)
```sql
ALTER TABLE users
  DROP COLUMN _fivetran_start,
  DROP COLUMN _fivetran_end,
  DROP COLUMN _fivetran_active;
```
4. Add primary keys back
```sql
ALTER TABLE users ADD PRIMARY KEY (id);
```


### Drop Column History Mode
1. Validate that the operation timestamp is less than the max( _fivetran_start) in the table
```sql
SELECT COUNT(*) AS validation_count
FROM users
WHERE _fivetran_start > :operation_ts;
```
> Proceed only if `validation_count = 0`.

2. Insert a new row to record the history of the DDL operation
   1. Copy the existing active record (_fivetran_active and max( _fivetran_start)) from the table
```sql
INSERT INTO users (
  id, name, deleted_column, _fivetran_start, _fivetran_end, _fivetran_active
)
SELECT
  id,
  name,
  DEFAULT, -- warehouse default value for `deleted_column`
  :operation_ts,
  '9999-12-31 23:59:59',
  TRUE
FROM users
WHERE _fivetran_active = TRUE
  AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM users);
```
   2. Update the newly added row:
      1. Set the deleted column value to the warehouse default 
      2. Set _fivetran_start to the operation timestamp
3. Update the previous record fivetran_end to (operation timestamp) - 1ms.

```sql
UPDATE users
SET _fivetran_end = (:operation_ts - INTERVAL '0.001 second')
WHERE _fivetran_active = TRUE
  AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM users)
  AND _fivetran_end = '9999-12-31 23:59:59';
```

### Add Column History Mode: 
Before the migration, if the new column does not exist, we execute the existing ADD_COLUMN migration and follow the following steps to update the history
1. Check if the table is empty, return if rowCount = 0
2. Insert a new row to record the history of the DDL operation
```sql
INSERT INTO users (
  id, name, new_column, _fivetran_start, _fivetran_end, _fivetran_active
)
SELECT
  id,
  name,
  DEFAULT, -- warehouse default value for `new_column`
  :operation_ts,
  '9999-12-31 23:59:59',
  TRUE
FROM users
WHERE _fivetran_active = TRUE
  AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM users);
```

3. Copy the existing active record (_fivetran_active and max( _fivetran_start)) from the table

4. Update the newly added row:
   1. Set the deleted column value to the warehouse default 
   2. Set _fivetran_start to the operation timestamp 
   3. Update the previous record fivetran_end to (operation timestamp) - 1ms.

```sql
UPDATE users
SET _fivetran_end = (:operation_ts - INTERVAL '0.001 second')
WHERE _fivetran_active = TRUE
  AND _fivetran_start = (SELECT MAX(_fivetran_start) FROM users)
  AND _fivetran_end = '9999-12-31 23:59:59';
```

### Copy Table To History Mode : 
Before migration , if new_table name does not already exist.

1. Create a copy of the old table name.
2. Now the new cloned table will undergo migration to History mode as described in **Migration to History Mode**. 
