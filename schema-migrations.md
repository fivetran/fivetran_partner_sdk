## What are Schema Migrations
Schema migrations in Fivetran comprise single or multi-step destination queries which allows us to manipulate data in the customer's warehouse in a manner that source connectors cannot do.
Destinations written natively by Fivetran support schema migrations.

## Why are they needed?
These are required for exceptional scenarios such as a bug fix comprising change in the schema.
Sometimes because of some buggy code we might upcast the column data type, or add an extra unnecessary column, etc.  Also in some cases if we have somehow created some column name_s instead of name then we need to copy all the data from one column to another, we can achieve such things using schema migration.
In order to get these issues resolved without having the customer need to take some action (eg - manually running queries) we need to support Schema Migrations in partner destinations.
Apart from that, we also have a history-mode which allows customers to keep track of history of the particular row, Now in order to convert some existing table from legacy/live mode to history mode we need to add Fivetran history mode system columns (_fivetran_start, _fivetran_end, _fivetran_active) to the table and initialize those columns with some proper values, this is also being done by schema migration process.
In addition to bug fixes, schema migrations encompass some native functionalities such as : history mode.

## How do they work?
Fivetran's prod DB contains the information as to whether a connection needs to perform a schema migration. This table can be filled by devs manually, or by logic in connector code written by devs.
At the start of each sync, Fivetran checks if the connection has any schema migrations to perform. If it does, it will call the appropriate methods in the writer code (see migrate method below) to perform the migration.

## Partner's responsibility
As a partner building a destination, you are required to implement the `migrate` method in your writer code. This method will be called by Fivetran when a schema migration is needed. The `migrate` method should contain the logic to perform the necessary schema changes in the customer's warehouse.
The example below shows how to implement the `migrate` method in a destination writer:

```python
```

## Migrate method
The `migrate` method is called the `MigrationDetails` and `MigrationType` objects. The `MigrationDetails` object contains the details of the migration, such as the schema, table to be migrated. The `MigrationType` object contains the type of migration to be performed.
Partner code will need to check what `MigrationType` is being passed and perform the necessary actions. The `MigrationDetails` object will contain the details of the migration, such as the schema, table to be migrated, and the column to be added or renamed.

Below table represents what information is passed in the `MigrationDetails` object for each `MigrationType`:
| MigrationType | MigrationDetails Object |
|---------------|-------------------------|
| `ADD_COLUMN_WITH_DEFAULT_VALUE` | schema, table, column, column_type, value |
| `ADD_HISTORY_COLUMN` | schema, table, column_type, value, operation_timestamp |
| `UPDATE_COLUMN` | schema, table, column, column_type, value |
| `COPY_COLUMN` | schema, table, from_column, to_column |
| `DROP_HISTORY_COLUMN` | schema, table, column, operation_timestamp |
| `SET_COLUMN_TO_NULL` | schema, table, column |
| `MIGRATE_HISTORY_TO_LIVE_MODE` | schema, table, keep_inactive_rows |
| `MIGRATE_HISTORY_TO_LEGACY_MODE` | schema, table, deleted_column |
| `MIGRATE_LEGACY_TO_LIVE_MODE` | schema, table, deleted_column |
| `MIGRATE_LIVE_TO_HISTORY_MODE` | schema, table, deleted_column |
| `COPY_TABLE_TO_HISTORY_MODE` | schema, table, from_table, to_table, deleted_column |
| `RENAME_COLUMN` | schema, table, from_column, to_column |
| `COPY_TABLE` | schema, table, from_table, to_table |
| `RENAME_TABLE` | schema, table, from_table, to_table  |

## Detailed steps for each type of migration along with workarounds (includes sample queries to be added by Bhabha)
