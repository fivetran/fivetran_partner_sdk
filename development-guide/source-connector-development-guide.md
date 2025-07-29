# Source Connector Guidelines

- Don't push anything other than source data to the destination. State data is saved to a production database and returned in `UpdateRequest`.
- Make sure to handle new schemas/tables/columns per the information and user choices in `UpdateRequest#selection`.
- Make sure you checkpoint at least once an hour. In general, the more frequently you do it, the better.

> NOTE: Source connectors do not support the `NAIVE_TIME` data type because many Fivetran-developed destinations do not support it. Only [partner-built destinations](README.md#destinations) support the `NAIVE_TIME` data type.

### Checkpointing
The `checkpoint` operation is a critical concept in Fivetran. It marks a specific point in the sync process up to which the `state` (a collection of `cursors`) can be safely saved. Following good checkpointing practices is _strongly recommended_, especially given the high volume of data and number of tables involved in a typical Fivetran sync.

Below are some best practices:
- Frequent checkpointing is important to avoid data reprocessing in the event of a failure. Without regular checkpoints, the system may need to re-fetch large amounts of already-processed data, resulting in redundant work and increased sync times.
- When possible, consider checkpointing even within a table sync. If that’s not feasible, a checkpoint should be made at minimum after each table is synced — this applies when tables are being synced synchronously.
- When implementing a checkpoint logic, it’s crucial to account for sync failures. Ensure that the cursor is not advanced prematurely, as this could result in data loss.

## RPC calls
### Schema
The `Schema` RPC call retrieves the user's schemas, tables, and columns. It also includes an optional `selection_not_supported` field that indicates whether the user can select or deselect tables and columns within the Fivetran dashboard.

### Update
The `Update` RPC call should retrieve data from the source. We send a request using the `UpdateRequest` message, which includes the user's connection state, credentials, and schema information. The response, streaming through the `UpdateResponse` message, can contain data records and other supported operations.

### Record types

#### Upsert
The `upsert` record type essentially translates to a delete + insert SQL operation, i.e., if a row with that primary key is already present in the destination, it will first be deleted then re-inserted. If the row with that primary key does not exist in the destination, it boils down to a simple insert.
This means that `upsert` always requires all columns to be present in the record even if they are not modified in the source. If a column is absent, the value will be updated to `null` in the destination.
This is the most frequently used record type.

#### Update
The `update` record type should be used when you want to partially update a row in the destination, i.e., only the columns present in the record will be updated. The rest of the columns will remain unchanged. If a row with that primary key is not present in the destination, it is simply ignored.

#### Delete
The `delete` record type is used to soft delete a particular record in the destination. If a record with that primary key is not present in the destination, it is simply ignored.

#### Truncate
The truncate record type is used to soft delete any rows that existed prior to the timestamp when truncate is called. Soft delete means updating the _`fivetran_deleted` column of a row to `true`.

It should be called before upserts only — otherwise, all rows in the table will be incorrectly marked as soft deleted.

`Truncate` is particularly useful during a re-sync (i.e., when an initial sync is triggered again). Because re-syncs do not fetch records that have been deleted in the source, we can't guarantee that all existing rows will be overwritten. To prevent stale data from persisting, `truncate` soft deletes all rows that existed before the re-sync began. It does this by identifying rows where `_fivetran_synced` is earlier than the timestamp when `truncate` was called, and setting `_fivetran_deleted = true for them`.

Unlike other record types mentioned above, `truncate` operates on the entire table rather than on individual rows.

## Testing
The following is a list of test scenarios we recommend you consider:

- Test mapping of all data types between Fivetran types and source/destination types (e.g. [Mysql](https://fivetran.com/docs/databases/mysql#typetransformationsandmapping))
- Big data loads
- Big incremental updates
- Narrow event tables
- Wide fact tables
