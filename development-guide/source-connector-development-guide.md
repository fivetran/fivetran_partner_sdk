# Source Connector guidelines

- Don't push anything other than source data to the destination. State data is saved to production database and returned in `UpdateRequest`.
- Don't forget to handle new schemas/tables/columns per the information and user choices in `UpdateRequest#selection`.
- Make sure you checkpoint at least once an hour. In general, the more frequently you do it, the better.

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