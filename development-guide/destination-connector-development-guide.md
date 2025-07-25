# Destination Connector Guidelines

The destination connector should implement the listed rpc calls to load the data Fivetran sends.

## System columns
In addition to source columns, Fivetran sends the following additional system columns if and when required:
- `_fivetran_synced`: This is a `UTC_DATETIME` column that represents the start of sync. Every table has this system column.
- `_fivetran_deleted`: This column is used to indicate whether a given row is deleted at the source or not. If the source soft-deletes a row or a table, this system column is added to the table.
- `_fivetran_id`: Fivetran supports primary-keyless source tables by adding this column as a stand-in pseudo primary key column so that all destination tables have a primary key.
- `_fivetran_active`, `_fivetran_start`, `_fivetran_end`: These columns are used in history mode. For more information, see our [How to Handle History Mode Batch Files documentation](../how-to-handle-history-mode-batch-files.md).

## Compression
Batch files are compressed using [ZSTD](https://en.wikipedia.org/wiki/Zstd).

## Encryption
- Each batch file is encrypted separately using [AES-256](https://en.wikipedia.org/wiki/Advanced_Encryption_Standard) in [CBC](https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation) mode and with `PKCS5Padding`. - You can find the encryption key for each batch file in the `WriteBatchRequest#keys` field.
- First 16 bytes of each batch file hold the IV vector.

## Batch files
- Each batch file is limited in size to 100MB.
- Number of records in each batch file can vary depending on row size.
- We support CSV and PARQUET file format.

### CSV
Fivetran creates batch files using `com.fasterxml.jackson.dataformat.csv.CsvSchema`, which by default doesn't consider backslash '\\' an escape character. If you are reading the batch file then make sure that you do not consider backslash '\\' an escape character.

BINARY data is written to batch files using base64 encoding. You need to decode it to get back the original byte array.

### PARQUET (available in V2)
Fivetran Partner SDK v2 writes batch files in Apache Parquet, a columnar file format that delivers higher compression and better scan performance than row‑oriented formats such as CSV.

#### Implementation details

- **Schema definition** – The Partner SDK uses the *Java* implementation of the [Apache Avro](https://avro.apache.org/) schema to describe each record. Make sure the schema matches your data exactly; any mismatch will cause deserialization failures.
- **Writer** – Parquet files are generated with [`AvroParquetWriter`](https://github.com/apache/parquet-java/blob/master/parquet-avro/src/main/java/org/apache/parquet/avro/AvroParquetWriter.java).
- **Binary columns** – Binary values are serialized as byte arrays. When you read a Parquet file, convert these byte arrays back to their original types.

#### Why Parquet?

- **Performance** – Columnar storage reduces I/O, so syncs finish faster and consume fewer resources.
- **Compression** – Parquet’s built-in compression typically shrinks files by *up to* 75 percent compared with CSV.
- **Type fidelity** – Numeric, temporal, and nested types (including decimals, timestamps, and JSON) are preserved without type loss.

#### Data types mapping

Data mappings from Fivetran to Parquet batch files are as below:

| Fivetran Type | Parquet Type    |
|---------------|-----------------|
| Boolean       | `boolean`       |
| Short, Int    | `INT32`         |
| Long          | `INT64`         |
| Float         | `Float`         |
| Double        | `Double`        |
| String        | `String`        |
| BigDecimal    | `Decimal`       |
| Instant       | `String`        |
| LocalDate     | `String`        |
| Binary        | `Binary`        |
| LocalDateTime | UNSUPPORTED     |


## RPC Calls
### CreateTable
The `CreateTable` RPC call should create the table. If you attempt to create a table that already exists, the call should fail.
If the target schema is missing, the `CreateTable` RPC call should not fail. The destination should create the missing schema.

### Capabilities
The `Capabilities` RPC call should return the destination's capabilities, such as reading batch files in CSV or PARQUET.

### DescribeTable
The `DescribeTable` RPC call should report all columns in the destination table, including Fivetran system columns such as `_fivetran_synced` and `_fivetran_deleted`. It should also provide other additional information as applicable such as data type, `primary_key`, and `DecimalParams`.

### Truncate
- The `Truncate` RPC call might be requested for a table that does not exist in the destination. In that case, it should NOT fail, simply ignore the request and return `success = true`.
- `utc_delete_before` has millisecond precision.
- `soft` indicates that the `truncate` operation is a soft truncate operation and instead of removing the rows, the specified `delete_column` needs to be marked as `true`.

### AlterTable
- The `AlterTable` RPC call should be used for changing primary key columns, adding columns, and changing data types.
- However, this operation should not drop any columns even if the `AlterTable` request has a table with a different set of columns. Dropping columns could lead to unexpected customer data loss and is against Fivetran's general approach to data movement.

### WriteBatchRequest
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

### WriteHistoryBatchRequest
> NOTE: This RPC call is available in V2 only.

The `WriteHistoryBatchRequest` RPC call provides details about the batch files containing the records to be written to the destination for [**History Mode**](https://fivetran.com/docs/using-fivetran/features#historymode). In addition to the parameters of the [`WriteBatchRequest`](#writebatchrequest), this request also contains the `earliest_start_files` parameter used for updating history mode-specific columns for the existing rows in the destination.

> NOTE: To learn how to handle `earliest_start_files`, `replace_files`, `update_files` and `delete_files` in history mode, follow the [How to Handle History Mode Batch Files](../how-to-handle-history-mode-batch-files.md) guide.

## Examples of data types
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

- Test mapping of all data types between Fivetran types and source/destination types (e.g. [Mysql](https://fivetran.com/docs/databases/mysql#typetransformationsandmapping))
- Big data loads
- Big incremental updates
- Narrow event tables
- Wide fact tables
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

### Is it normal that, for a sync, there is an upsert event followed by a truncate event?
Yes, definitely. This is most likely an initial sync where there is nothing but upsert operations, all followed by a truncate, which is meant to (soft) delete any rows that may have existed prior to the initial sync starting. This is done to make sure all rows that may have existed prior to the initial sync are marked as deleted (since we cannot be sure the initial sync will necessarily overwrite them all). The "before timestamp" is key to the truncate operation so you don't just mark the entire table deleted. It should pick out the rows that existed prior to the sync starting, in other words, where `_fivetran_synced` < "truncate before timestamp".
