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

2. Run a container using the image with the following command. Make sure to map a local directory for the tool by replacing `<local-data-folder>` placeholders in the command, and replace `<version>` with the version of the image you pulled.

```
docker run --mount type=bind,source=<local-data-folder>,target=/data -a STDIN -a STDOUT -a STDERR -it -e WORKING_DIR=<local-data-folder> -e GRPC_HOSTNAME=host.docker.internal --network=host us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version> --tester-type destination --port 50052
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

### Example input file
Here is an example input file named `input_1.json`:

```json
{
   "create_table" : {
      "transaction": {
         "columns": {
            "id": "INT",
            "amount" : "DOUBLE",
            "desc": "STRING"
         },
         "primary_key": ["id"],
         "history_mode": true
      },
      "campaign": {
         "columns": {
            "name": "STRING",
            "num": {"type": "DECIMAL", "precision": 6, "scale": 3}
         },
         "primary_key": []
      }
   },
   "alter_table" : {
      "transaction": {
         "columns": {
            "id": "INT",
            "amount" : "FLOAT",
            "desc": "STRING"
         },
         "primary_key": ["id"],
         "history_mode": true
      }
   },
   "describe_table" : [
      "transaction"
   ],
   "ops" : [
      {
         "upsert": {
            "transaction": [
               {"id":1, "amount": 100.45, "desc":null, "op_time":"2005-05-23T20:57:00Z"},
               {"id":2, "amount": 150.33, "desc": "two", "op_time":"2005-05-23T20:57:00Z"},
               {"id":3, "amount": 150.33, "desc": "two", "op_time":"2005-05-23T20:57:00Z"},
               {"id":4, "amount": 150.33, "desc": "two", "op_time":"2005-05-23T20:57:00Z"}
            ],
            "campaign": [
               {"_fivetran_id": "abc-123-xyz", "name": "Christmas", "num": 100.23},
               {"_fivetran_id": "vbn-543-hjk", "name": "New Year", "num": 200.56}
            ]
         }
      },
      {
         "truncate_before": [
            "campaign"
         ]
      },
      {
         "update": {
            "transaction": [
               {"id":1, "amount": 200 ,"op_time":"2005-05-24T20:58:00Z"},
               {"id":5, "amount": 200 ,"op_time":"2005-05-24T20:57:00Z"}
            ]
         }
      },
      {
         "soft_truncate_before": [
            "transaction"
         ]
      },
      {
         "upsert": {
            "transaction": [
               {"id":10, "amount": 200, "desc": "three", "op_time":"2005-05-26T20:57:00Z"},
               {"id":10, "amount": 100, "desc": "thee", "op_time":"2005-05-26T20:58:00Z"},
               {"id":20, "amount": 50, "desc": "mone", "op_time":"2005-05-26T21:57:00Z"}
            ],
            "campaign": [
               {"_fivetran_id": "dfg-890-lkj", "name": "Christmas 2", "num": 400.32}
            ]
         }
      },
      {
         "delete": {
            "transaction": [
               {"id":3, "op_time":"2005-05-27T20:57:00Z"},
               {"id":6, "op_time":"2005-05-27T20:57:00Z"}
            ],
            "campaign": [
               {"_fivetran_id": "abc-123-xyz"}
            ]
         }
      },
      {
         "soft_delete": {
            "transaction": [
               {"id":4, "op_time":"2005-05-27T20:57:00Z"},
               {"id":5, "op_time":"2005-05-27T20:57:00Z"}
            ],
            "campaign": [
               {"_fivetran_id": "dfg-890-lkj"}
            ]
         }
      }
   ]
}

```

## CLI Arguments

The tester supports the following optional CLI arguments to alter its default behavior. You can append these options to the end of the `docker run` command provided in step 2 of [How To Run](https://github.com/fivetran/fivetran_sdk/tree/main/tools/destination-connector-tester#how-to-run) section above.

#### --port
This option defines the port the tester should run on.

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
