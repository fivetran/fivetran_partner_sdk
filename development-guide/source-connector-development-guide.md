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

## Testing
The following is a list of test scenarios we recommend you consider:

- Test mapping of all data types between Fivetran types and source/destination types (e.g. [Mysql](https://fivetran.com/docs/databases/mysql#typetransformationsandmapping))
- Big data loads
- Big incremental updates
- Narrow event tables
- Wide fact tables
