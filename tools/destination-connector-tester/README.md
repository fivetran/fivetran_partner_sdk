# Destination Tester

## Pre-requisites
- Docker Desktop >= 4.23.0 or [Rancher Desktop](https://rancherdesktop.io/) >= 1.12.1
- gRPC server is running for the particular example (see [example readme's](/examples/destination_connector/))

## How To Run

1. Pull the latest docker image from [public-docker-us/sdktesters-v2/sdk-tester](https://console.cloud.google.com/artifacts/docker/build-286712/us/public-docker-us/sdktesters-v2%2Fsdk-tester?invt=Abm4dQ&inv=1) Google Artifact Registry, use the following commands:
   
    - Authenticate Docker to Google Artifact Registry: Run the following command to allow Docker to use your Google credentials
    ```
   gcloud auth configure-docker us-docker.pkg.dev
    ```
    - Pull the Image: 
    ```
   docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version> 
    ```

2. Run a container using the image with the following command. Make sure to map a local directory for the tool by replacing `<local-data-folder>` placeholders in the command, and replace `<version>` with the version of the image you pulled. Use the port number your gRPC server is running on. It should be `50052` if you followed the Fivetran documentation correctly.

```
docker run --mount type=bind,source=<local-data-folder>,target=/data -a STDIN -a STDOUT -a STDERR -it -e WORKING_DIR=<local-data-folder> -e GRPC_HOSTNAME=host.docker.internal --network=host us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version> --tester-type destination --port <port-number>
```

3. To rerun the container from step #2, use the following command:

```
docker start -i <container-id>
```

## Input Files

Destination tester simulates operations from a source by reading input files from the local data folder. Each input file represents a batch of operations, encoded in JSON format. Data types in [common.proto](https://github.com/fivetran/fivetran_sdk/blob/main/common.proto#L73) file can be used as column data types.

### List of Operations

#### Table Operations
* describe_table
* create_table
* alter_table

#### Single Record Operations
* upsert
* update
* delete
* soft_delete

#### Bulk Record Operations
* truncate_before
* soft_truncate_before

### Example Input Files

#### Basic Operations
For basic table and record operations, refer to [input.json](input-files/input.json). This file demonstrates:
- Table operations (create_table, alter_table, describe_table)
- Single record operations (upsert, update, delete, soft_delete)
- Bulk record operations (truncate_before, soft_truncate_before)

#### Schema Migration Operations
For testing schema migration operations, the following input files are available:

1. **[schema_migrations_input_ddl.json](input-files/schema_migrations_input_ddl.json)** - DDL schema migration operations including:
   - add_column
   - change_column_data_type
   - drop_column

2. **[schema_migrations_input_dml.json](input-files/schema_migrations_input_dml.json)** - DML schema migration operations including:
   - copy_column
   - update_column_value
   - add_column_with_default_value
   - set_column_to_null
   - copy_table
   - rename_column
   - rename_table
   - drop_table

3. **[schema_migrations_input_sync_modes.json](input-files/schema_migrations_input_sync_modes.json)** - Sync mode schema migration operations including:
   - add_column_in_history_mode
   - drop_column_in_history_mode
   - copy_table_to_history_mode
   - migrate_history_to_live
   - migrate_soft_delete_to_live
   - migrate_live_to_soft_delete
   - migrate_soft_delete_to_history
   - migrate_live_to_history
   - migrate_history_to_soft_delete

#### Operations on Non-Existent Records
For operations that reference a record or table that does not exist in the destination, refer to the examples in
[operations_on_nonexistent_records](input-files/operations_on_nonexistent_records).These files demonstrate:

- update on a non-existent record
- delete on a non-existent record
- soft_delete on a non-existent record
- truncate_before on a table that does not exist
- Record operations issued after a truncate_before has removed all rows

These operations must be safely ignored by the destination.

## CLI Arguments

The tester supports the following optional CLI arguments to alter its default behavior. You can append these options to the end of the `docker run` command provided in step 2 of [How To Run](https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-connector-tester#how-to-run) section above.

#### --port
This option defines the port the tester should run on. It should be the same as the port number your gRPC server is running on.

#### --plain-text
This option disables encryption and compression of batch files for debugging purposes.

#### --input-file
The tester by default reads all input files from local data folder and executes them in the alphabetical order they appear. You can specify a single input file to be read and executed using this option. Providing just the filename is sufficient.

#### --schema-name
The tester by default creates a schema named `tester`. This option allows the tester to run with a custom schema name by specifying `--schema-name <custom_schema_name>` where `<custom_schema_name>` is your custom schema name.

#### --disable-operation-delay
The tester by default adds a delay to operation for real-time simulation. Specifying this argument disables the delay.

#### --batch-file-type
We now support both CSV and PARQUET batch files. If this argument is not provided, testers will generate CSV batch files.
