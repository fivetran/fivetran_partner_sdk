# SDK Development Guide

## Getting Started

Fivetran SDK uses [gRPC](https://grpc.io/) to talk to partner code. The partner side of the interface is always the server side. Fivetran implements the client side and initiates the requests.

Start by using one of the examples as a template for your new SDK connector. If we don't have an example in 
your language, create a new gRPC server project in your language of choice using the protobuf definition files at the root of this repo.

## General guidelines

### Versions
* gRPC: 1.61.1
* protobuf: 4.28.3

### Language

At the moment, partner code should be developed in a language that can generate a statically linked binary executable.

### Command line Arguments
The executable needs to do the following:
* Accept a `--port` argument that takes an integer as a port number to listen to.
* Listen on both IPV4 (i.e. 0.0.0.0) and IPV6 (i.e ::0), but if only one is possible, it should listen on IPV4.

### Proto files

* Partners should not add the proto files to their repos. Proto files should be pulled in from this repo at build time and added to `.gitignore` so they are excluded.
* Always use proto files from latest release and update your code if necessary. Older releases proto files can be considered deprecated and will be expired at later date.

### Logging

- Write logs out to STDOUT in the following JSON format. Accepted levels are INFO, WARNING, and SEVERE. `message-origin` can be `sdk_connector` or `sdk_destination`.

```
{
    "level":"INFO",
    "message": "Your log message goes here"
    "message-origin": "sdk_connector"
}
```

- Try to make log messages as _precise_ as possible, which can make it easier to debug issues. 
- Provide context in log messages. Contextual information can make it much easier to debug issues.
- Write a separate error message for each exception.
- Log _after_ an action. When you log after an action, additional context can be provided.
- Include details about "what went wrong" in your error message
- Manage the volume of the log. Ask yourself if a consumer of the log message will find it useful to solve a problem and whether that need justifies the cost of storing and managing that log. Sources of excessive logging include: 
    - **Tracing entries and exits in methods** - Don't do this unless it is absolutely necessary. 
    - **Logging inside tight loops** - Be careful about what you are logging inside loops, especially if the loop runs for many iterations.
    - **Including benign errors** - When a successful execution flow includes handling errors.
    - **Repeating errors** - For instance, if you log an exception trace before each retry, you might end up logging the exception trace unnecessarily or too many times.
- Consider logging of timing data - Logging the time taken for time-sensitive operations like network calls can make it easier to debug performance issues in production. Consider if logging of timing data can be useful in your connector.

### Error handling
- Partner code should handle any source and destination-related errors.
- Partner code should retry any transient errors internally without deferring them to Fivetran.
- Partner code should use [gRPC built-in error mechanism](https://grpc.io/docs/guides/error/#language-support) to relay errors instead of throwing exceptions and abruptly closing the connection.
- Partner code should capture and relay a clear message when the account permissions are not sufficient.

### User alerts
> NOTE: Available in V2 only.
- Partners can throw alerts on the Fivetran dashboard to notify customers about potential issues with their connector.
- These issues may include bad source data or connection problems with the source itself. Where applicable, the alerts should also provide guidance to customers on how to resolve the problem.
- We allow throwing [errors](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/alerts#errors) and [warnings](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/alerts#warnings).
- Partner code should use [Warning](https://github.com/fivetran/fivetran_sdk/blob/main/common.proto#L160) and [Task](https://github.com/fivetran/fivetran_sdk/blob/main/common.proto#L164) messages defined in the proto files to relay information or errors to Fivetran.
- Usage example:
```
responseObserver.onNext(
                UpdateResponse.newBuilder()
                        .setTask(
                                Task.newBuilder()
                                        .setMessage("Unable to connect to the database. Please provide the correct credentials.")
                                        .build()
                        )
                        .build());
```
> NOTE: We continue with the sync in case of Warnings, and break execution when Tasks are thrown. 

### Retries
- Partner code should retry transient problems internally
- Fivetran will not be able to handle any problems that the partner code runs into
- If an error is raised to Fivetran's side, the sync will be terminated and retried from the last good known spot according to saved [cursors](https://fivetran.com/docs/getting-started/glossary#cursor) from the last successful batch.

### Security
The following are hard requirements to be able to deploy partner code to Fivetran production:
- Do not decrypt batch files to disk. Fivetran does not allow unencrypted files at rest. If you need to upload batch files in plaintext, do the decryption in "streaming" mode. 
- Do not log sensitive data. Ensure only necessary information is kept in logs, and never log any sensitive data. Such data may include credentials (passwords, tokens, keys, etc.), customer data, payment information, or PII.
- Encrypt HTTP requests. Entities like URLs, URL parameters, and query parameters are always encrypted for logging, and customer approval is needed to decrypt and examine them.

## Setup Form Guidelines
- Keep the form clear and concise, only requesting essential information for successful connector setup.
- Use clear and descriptive labels for each form field. Avoid technical jargon if possible.
- Organize the fields in a logical order that reflects the setup process.

### RPC Calls
#### ConfigurationForm
The `ConfigurationForm` RPC call retrieves all the setup form fields and tests information. You can provide various parameters for the fields to enhance the user experience, such as descriptions, optional fields, and more.

#### Test
The [`ConfigurationForm` RPC call](#configurationform) retrieves the tests that need to be executed during connection setup. The `Test` call then invokes the test with the customer's credentials as parameters. As a result, it should return a success or failure indication for the test execution.

### Supported setup form fields 
- Text field: A standard text input field for user text entry. You can provide a `title` displayed above the field. You can indicate whether the field is `required`, and you may also include an optional `description` displayed below the field to help explain what the user should complete.
- Dropdown: A drop-down menu that allows users to choose one option from the list you provided.
- Toggle field: A toggle switch for binary options (e.g., on/off or yes/no).
- Conditional fields (Available in V2): This feature allows you to define fields that are dependent on the value of a specific parent field. The message consists of two nested-messages: `VisibilityCondition` and a list of dependent form fields. The `VisibilityCondition` message specifies the parent field and its condition value. The list of dependent fields defines the fields that are shown when the value of the parent field provided in the setup form matches the specified condition field.

## Source Connector guidelines

- Don't push anything other than source data to the destination. State data is saved to production database and returned in `UpdateRequest`.
- Don't forget to handle new schemas/tables/columns per the information and user choices in `UpdateRequest#selection`.
- Make sure you checkpoint at least once an hour. In general, the more frequently you do it, the better.

### RPC Calls
#### Schema
The `Schema` RPC call retrieves the user's schemas, tables, and columns. It also includes an optional `selection_not_supported` field that indicates whether the user can select or deselect tables and columns within the Fivetran dashboard.

#### Update
The `Update` RPC call should retrieve data from the source. We send a request using the `UpdateRequest` message, which includes the user's connection state, credentials, and schema information. The response, streaming through the `UpdateResponse` message, can contain data records and other supported operations.

## Destination Connector guidelines

- The destination connector should implement the listed rpc calls to load the data sent by Fivetran.

### System Columns
- In addition to source columns, Fivetran sends the following additional system columns if and when required:
    - `_fivetran_synced`: This is a `UTC_DATETIME` column that represents the start of sync. Every table has this system column.
    - `_fivetran_deleted`: This column is used to indicate whether a given row is deleted at the source or not. If the source soft-deletes a row or a table, this system column is added to the table.
    - `_fivetran_id`: Fivetran supports primary-keyless source tables by adding this column as a stand-in pseudo primary key column so that all destination tables have a primary key.
    - `_fivetran_active`, `_fivetran_start`, `_fivetran_end`: These columns are used in history mode. For more information, refer [here](how-to-handle-history-mode-batch-files.md).

### Compression
Batch files are compressed using [ZSTD](https://en.wikipedia.org/wiki/Zstd).  

### Encryption
- Each batch file is encrypted separately using [AES-256](https://en.wikipedia.org/wiki/Advanced_Encryption_Standard) in [CBC](https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation) mode and with `PKCS5Padding`.
- You can find the encryption key for each batch file in the `WriteBatchRequest#keys` field.
- First 16 bytes of each batch file hold the IV vector.

### Batch Files
- Each batch file is limited in size to 100MB.
- Number of records in each batch file can vary depending on row size.
- We support CSV and PARQUET file format.

#### CSV
- Fivetran creates batch files using `com.fasterxml.jackson.dataformat.csv.CsvSchema`, which by default doesn't consider backslash '\\' an escape character. If you are reading the batch file then make sure that you do not consider backslash '\\' an escape character.
- BINARY data is written to batch files using base64 encoding. You need to decode it to get back the original byte array.

#### PARQUET (Available in V2)
- Apache Avro schema is used to define the structure of the data in the batch files. When writing data, ensure that the schema is correctly defined and matches the data format to prevent issues during deserialization.
- Parquet files are written using `AvroParquetWriter`.
- BINARY data written is converted to byte array. Ensure that when reading from Parquet files, this byte array is properly handled and reconstructed as needed.

### RPC Calls
#### CreateTable
The `CreateTable` RPC call should create the table. If you attempt to create a table that already exists, the call should fail.
If the target schema is missing, the `CreateTable` RPC call should not fail. The destination should create the missing schema.

#### Capabilities
The `Capabilities` RPC call should return the destination's capabilities, such as reading batch files in CSV or PARQUET.

#### DescribeTable
The `DescribeTable` RPC call should report all columns in the destination table, including Fivetran system columns such as `_fivetran_synced` and `_fivetran_deleted`. It should also provide other additional information as applicable such as data type, `primary_key`, and `DecimalParams`.

#### Truncate
- The `Truncate` RPC call might be requested for a table that does not exist in the destination. In that case, it should NOT fail, simply ignore the request and return `success = true`.
- `utc_delete_before` has millisecond precision.
- `soft` indicates that the `truncate` operation is a soft truncate operation and instead of removing the rows, the specified `delete_column` needs to be marked as `true`. 

#### AlterTable
- The `AlterTable` RPC call should be used for changing primary key columns, adding columns, and changing data types. 
- However, this operation should not drop any columns even if the `AlterTable` request has a table with a different set of columns. Dropping columns could lead to unexpected customer data loss and is against Fivetran's general approach to data movement.

#### WriteBatchRequest
The `WriteBatchRequest` RPC call provides details about the batch files containing the records to be pushed to the destination. We provide the `WriteBatchRequest` parameter that contains all the information required for you to read the batch files. Here are some of the fields included in the request message:

- `replace_files` is for the `upsert` operation where the rows should be inserted if they don't exist or updated if they do. Each row will always provide values for all columns. Set the `_fivetran_synced` column in the destination with the values coming in from the batch files.

- `update_files` is for the `update` operation where modified columns have actual values whereas unmodified columns have the special value `unmodified_string` in `FileParams`. Soft-deleted rows will arrive in here as well. Update the `_fivetran_synced` column in the destination with the values coming in from the batch files.

- `delete_files` is for the `hard delete` operation. Use primary key columns (or `_fivetran_id` system column for primary-keyless tables) to perform `DELETE FROM`.

- `keys` is a map that provides a list of secret keys, one for each batch file, that can be used to decrypt them.

- `file_params` provides information about the file type and any configurations applied to it, such as encryption or compression.

Also, Fivetran deduplicates operations such that each primary key shows up only once in any of the operations.

> NOTE: For CSV batch files, do not assume the order of columns. Always read the CSV file header to determine the column order.

- `FileParams`:
    - `null_string` value is used to represent `NULL` value in all batch files.
    - `unmodified_string` value is used to indicate columns in `update_files` where the values did not change.

#### WriteHistoryBatchRequest
> NOTE: This RPC call is available in V2 only.

The `WriteHistoryBatchRequest` RPC call provides details about the batch files containing the records to be written to the destination for [**History Mode**](https://fivetran.com/docs/using-fivetran/features#historymode). In addition to the parameters of the [`WriteBatchRequest`](#writebatchrequest), this request also contains the `earliest_start_files` parameter used for updating history mode-specific columns for the existing rows in the destination.

> NOTE: To learn how to handle `earliest_start_files`, `replace_files`, `update_files` and `delete_files` in history mode, follow the [How to Handle History Mode Batch Files](how-to-handle-history-mode-batch-files.md) guide.

### Examples of Data Types
Examples of each [DataType](https://github.com/fivetran/fivetran_sdk/blob/main/common.proto#L73C6-L73C14) as they would appear in CSV batch files are as follows:
- UNSPECIFIED: This data type never appears in batch files
- BOOLEAN: "true", "false"
- SHORT: -32768 .. 32767
- INT: -2147483648 .. 2147483647
- LONG: -9223372036854776000 .. 9223372036854775999
- DECIMAL: Floating point values with max precision of 38 and max scale of 37
- FLOAT: Single-precision 32-bit IEEE 754 values, e.g. 3.4028237E+38
- DOUBLE: Double-precision 64-bit IEEE 754 values, e.g. -2.2250738585072014E-308
- NAIVE_TIME: Time without a timezone in the ISO-8601 calendar system, e.g. 10:15:30 
- NAIVE_DATE: Date without a timezone in the ISO-8601 calendar system, e.g. 2007-12-03
- NAIVE_DATETIME: A date-time without timezone in the ISO-8601 calendar system, e.g. 2007-12-03T10:15:30
- UTC_DATETIME: An instantaneous point on the timeline, always in UTC timezone, e.g. 2007-12-03T10:15:30.123Z
- BINARY: Binary data is represented as protobuf `bytes` (such as [ByteString](https://github.com/protocolbuffers/protobuf/blob/main/java/core/src/main/java/com/google/protobuf/ByteString.java) in Java), e.g. `[B@7d4991ad` (showing ByteString as bytes)
- XML: "`<tag>`This is xml`</tag>`"
- STRING: "This is text"
- JSON: "{\"a\": 123}"

## Testing
The following is a list of test scenarios we recommend you consider:
### Source
- Test mapping of all data types between Fivetran types and source/destination types (e.g. [Mysql](https://fivetran.com/docs/databases/mysql#typetransformationsandmapping))
- Big data loads
- Big incremental updates
- Narrow event tables
- Wide fact tables
### Destination
In addition to the suggestions above, consider the following as well:
- Make sure to test with at least one of each of the following source connector types:
    - [Database](https://fivetran.com/docs/databases) (Postgres, MongoDB, etc.)
    - [Application](https://fivetran.com/docs/applications) (Github, Hubspot, Google Sheets, etc.)
    - [File](https://fivetran.com/docs/files) (S3, Google Drive, etc.)
    - [Fivetran Platform connector](https://fivetran.com/docs/logs/fivetran-platform)
- Exercise `AlterTable` in various ways:
    - Adding one or more columns
    - Change of primary key columns (adding and removing columns from primary key constraint)
    - Changing data type of non-primary key columns 
    - Changing data type of primary key columns
- Test tables with and without primary-key
 
## FAQ

### Is it possible for me to see the connector log output?
Sort of. We will email you the logs for a failed sync through support but the number of log messages is limited and this is a slow process for debugging in general. What you need to do is add your own logging for your own platform of choice so you are not reliant on us for logs. Plus, that way, you can implement alerts, monitoring, etc.

### Is it normal that, for a sync, there is an upsert event followed by a truncate event?
Yes, definitely. This is most likely an initial sync where there is nothing but upsert operations, all followed by a truncate, which is meant to (soft) delete any rows that may have existed prior to the initial sync starting. This is done to make sure all rows that may have existed prior to the initial sync are marked as deleted (since we cannot be sure the initial sync will necessarily overwrite them all). The "before timestamp" is key to the truncate operation so you don't just mark the entire table deleted. It should pick out the rows that existed prior to the sync starting, in other words, where `_fivetran_synced` < "truncate before timestamp".
