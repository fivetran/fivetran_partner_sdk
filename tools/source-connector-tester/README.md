# Connector Tester

## Pre-requisites
- Docker Desktop >= 4.23.0 or [Rancher Desktop](https://rancherdesktop.io/) >= 1.12.1
- gRPC server is running for the particular example (see [example readme's](/examples/source_connector/))

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
   
2. Run a container using the image with the following command. Make sure to map a local directory for storing files that the tool generates by replacing `<local-data-folder>` in the command, and replace <version> with the version of the image you pulled. Use the port number your gRPC server is running on. It should be `50051` if you followed the Fivetran documentation correctly.

```
docker run --mount type=bind,source=<local-data-folder>,target=/data -a STDIN -a STDOUT -a STDERR -it -e GRPC_HOSTNAME=host.docker.internal --network=host us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<version> --tester-type source --port <port-number>
```

3. Once the sync is done running, it will persist the records in a `warehouse.db` database file. This is an instance of [DuckDB](https://duckdb.org/) database. You can connect to it to validate the results of your sync using [DuckDB CLI](https://duckdb.org/docs/api/cli) or [DBeaver](https://duckdb.org/docs/guides/sql_editors/dbeaver)

4. To rerun the container from step #2, use the following command:

```
docker start -i <container-id>
```

## CLI Arguments

The tester supports the following optional CLI arguments to alter its default behavior. You can append these options to the end of the docker run command provided in step 2 of [How To Run](https://github.com/fivetran/fivetran_sdk/tree/main/tools/source-connector-tester#how-to-run) section above.

#### --port
This option defines the port the tester should run on. It should be the same as the port number your gRPC server is running on.

#### --destination-schema
With this option, you can alter the schema name used in the destination from the default `default_schema`.
