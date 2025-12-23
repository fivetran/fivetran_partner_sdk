# SDK Development Guide

Fivetran SDK uses [gRPC](https://grpc.io/) to talk to partner code. The partner side of the interface is always the server side. Fivetran implements the client side and initiates the requests.

## General guidelines

### Versions
- gRPC: 1.61.1
- protobuf: 4.29.3

### Language

Partner code should be developed in a language that can generate a statically linked binary executable.

#### Supported languages
Fivetran currently supports connectors built in the following languages:

- Python (recommended for new partners)
- Go
- Rust
- Java
- C++

We encourage new source partners to build their source connectors in Python for quicker onboarding experience and to start by submitting a community connector to our [Connector SDK repository](https://github.com/fivetran/fivetran_connector_sdk).

For detailed language-specific requirements, see the [source connector development guide](source-connector-development-guide.md) and [destination connector development guide](destination-connector-development-guide.md).

### Command line arguments
The executable needs to do the following:
- Accept a `--port` argument that takes an integer as a port number to listen to.
- Listen on both IPV4 (i.e. 0.0.0.0) and IPV6 (i.e ::0), but if only one is possible, it should listen on IPV4.

### Proto files

- Partners should not add the proto files to their repos. Proto files should be pulled in from this repo at build time and added to `.gitignore` so they are excluded.
- Always use proto files from latest release and update your code if necessary. Older releases proto files can be considered deprecated and will be expired at later date.

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

- Partners can throw alerts on the Fivetran dashboard to notify customers about potential issues with their connector.
- These issues may include bad source data or connection problems with the source itself. Where applicable, the alerts should also provide guidance to customers on how to resolve the problem.
- We allow throwing [errors](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/alerts#errors) and [warnings](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/alerts#warnings).
- Partner code should use [Warning](https://github.com/fivetran/fivetran_sdk/blob/main/common.proto#L160) and [Task](https://github.com/fivetran/fivetran_sdk/blob/main/common.proto#L164) messages defined in the proto files to relay information or errors to Fivetran.

#### Guidelines for warnings and tasks

The ability to send multiple responses depends on whether the RPC returns a streaming response:

##### RPCs that support multiple responses (streaming):
- RPCs defined with the `stream` keyword allow you to call `responseObserver.onNext()` multiple times. Example: `rpc Update (UpdateRequest) returns (stream UpdateResponse) {}` (source connector)
- You can send multiple warnings, records, checkpoints, etc. in separate response messages.
- You can send only one task message - once a task is issued, the sync stops immediately.

##### RPCs that support only a single response (non-streaming):
- Most destination connector RPCs return single (non-streaming) responses: `AlterTable`, `CreateTable`, `WriteBatch`, `Truncate`, `Migrate`, etc. Example: `rpc AlterTable(AlterTableRequest) returns (AlterTableResponse) {}`
- You can call `responseObserver.onNext()` only once, followed by `responseObserver.onCompleted()`.
- Each response uses a `oneof` field, meaning you can return only one of: success, warning, or task (not multiple).

#### Usage examples

##### Multiple responses (streaming):
```java
// Update supports streaming - you can send multiple responses
responseObserver.onNext(
    UpdateResponse.newBuilder()
        .setWarning(Warning.newBuilder()
            .setMessage("Table 'users' has 5 rows with invalid email format.")
            .build())
        .build()
);

responseObserver.onNext(
    UpdateResponse.newBuilder()
        .setWarning(Warning.newBuilder()
            .setMessage("Table 'orders' has 3 rows with missing timestamps.")
            .build())
        .build()
);

// Continue sending data
responseObserver.onNext(
    UpdateResponse.newBuilder()
        .setRecord(record)
        .build()
);

// Send task to stop sync
responseObserver.onNext(
    UpdateResponse.newBuilder()
        .setTask(Task.newBuilder()
            .setMessage("Unable to connect to the database. Please verify credentials.")
            .build())
        .build()
);
```

##### Single response:
```java
// AlterTable does NOT support streaming - only ONE response allowed
// You can return EITHER success, warning, OR task (not multiple)

// Option 1: Return success
responseObserver.onNext(
    AlterTableResponse.newBuilder()
        .setSuccess(true)
        .build()
);
responseObserver.onCompleted();

// Option 2: Return warning (in a different call)
responseObserver.onNext(
    AlterTableResponse.newBuilder()
        .setWarning(Warning.newBuilder()
            .setMessage("Column type change may result in data loss.")
            .build())
        .build()
);
responseObserver.onCompleted();

// Option 3: Return task to stop sync (in a different call)
responseObserver.onNext(
    AlterTableResponse.newBuilder()
        .setTask(Task.newBuilder()
            .setMessage("Insufficient permissions to alter table.")
            .build())
        .build()
);
responseObserver.onCompleted();

// INCORRECT - Cannot send multiple responses for AlterTable:
// responseObserver.onNext(AlterTableResponse.newBuilder().setWarning(...).build());
// responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build()); // ERROR!
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

## Setup form guidelines
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
- Descriptive dropdown: A dropdown field with contextual descriptions for each option, helping users choose the right value. Use a label–description pair for each option in `DescriptiveDropDownFields`.
- Toggle field: A toggle switch for binary options (e.g., on/off or yes/no).
- Upload field: Lets users upload files, such as certificates and keys, through the setup form. Use `allowed_file_type` to specify permitted file types; allowed file types must be defined starting with a dot '.' (e.g., [".pem", ".json"]). Use `max_file_size_bytes` to set a file size limit. Follow this [example](https://github.com/fivetran/fivetran_partner_sdk/blob/2f4f5d401d621db5ea66b70590c8914b2c88b65a/examples/destination_connector/java/src/main/java/destination/DestinationServiceImpl.java#L196) for guidance on how to configure the `UploadField`. We automatically convert your uploaded files to base64-encoded strings in the SDK configuration object. You must implement decoding of the base64 string to reconstruct the uploaded file locally.
- Conditional fields: This feature allows you to define fields that are dependent on the value of a specific parent field. The message consists of two nested-messages: `VisibilityCondition` and a list of dependent form fields. The `VisibilityCondition` message specifies the parent field and its condition value. The list of dependent fields defines the fields that are shown when the value of the parent field provided in the setup form matches the specified condition field.

## Source connector guidelines
Refer to our [source connector development guide](source-connector-development-guide.md).

## Destination connector guidelines
Refer to our [destination connector development guide](destination-connector-development-guide.md).

## How we use your service
This section outlines how we integrate partner services into our infrastructure. We build and run your service as a `standalone binary` that implements a gRPC server.
To ensure a smooth and repeatable integration, we require your service code to follow a defined structure and include clear instructions for how to build the binary.

### What we do

- We build a standalone binary from the code you provide.
- We run this binary in a Linux/amd64 environment, inside a Docker container.
- Your binary must start and run a gRPC server that implements _all_ required gRPC calls as defined in our proto files.

---

### Code requirements

To be accepted, your codebase must:

- Contain a clear and runnable `main` entry point (e.g., `main.go`, `main.py`, `Main.java`, etc.) that starts a gRPC server.
- Conform to the agreed-upon gRPC interface. _All_ required gRPC service methods must be implemented.
- Be structured to support a clean, repeatable build process.
- Avoid dependencies that require manual input or undocumented setup steps.

> NOTE: If your code does not conform to these requirements, we will request changes and pause the process until resolved.

---

### Build instructions

> **For detailed language-specific build requirements and automated build pipeline specifications, see our [Binary Build Requirements Guide](binary-build-requirements.md).**

To help us build the binary consistently, you must provide either of the following in your repository:
- A build script (e.g., `build.sh` or `Makefile`) that automates the binary creation.
- A clearly documented, step-by-step guide in a `README.md` or `BUILD.md` file with commands we can run to build the binary.

This should result in:

- Producing a self-contained executable targeting `linux/amd64`
- Include all necessary steps such as dependency installation, compilation flags, or environment setup

### Testing the binary
Before submitting the binary or code for service, you should test that the binary runs correctly on the target platform `linux/amd64` using Docker. This helps ensure that it behaves as expected in our environment.

See the following example Docker test command:

```bash
docker run --rm \
  --platform linux/amd64 \
  -v <local_path_to_binary>:/usr/local/myapp:ro \
  -v /tmp:/tmp \
  -p <port>:<port> \
  us-docker.pkg.dev/build-286712/public-docker-us/azul/zulu-openjdk-debian:17-jre-headless-latest-2024-08-05 \
  /usr/local/myapp/<binary_name> --port <port>
```
Make sure the binary executes without errors and the gRPC server starts as expected.
Then, run the tester to verify that all gRPC endpoints work as expected.  
For details on running the tester, see the [Source Tester](https://github.com/fivetran/fivetran_partner_sdk/blob/main/tools/source-connector-tester/README.md) and [Destination Tester](https://github.com/fivetran/fivetran_partner_sdk/blob/main/tools/destination-connector-tester/README.md) documentation.

---

## FAQ

### Is it possible for me to see the connector log output?
Sort of. We will email you the logs for a failed sync through support but the number of log messages is limited and this is a slow process for debugging in general. What you need to do is add your own logging for your own platform of choice so you are not reliant on us for logs. Plus, that way, you can implement alerts, monitoring, etc.

### What is the maximum supported gRPC message size for Partner SDK services?
The maximum supported gRPC message size is 128 MB (134,217,728 bytes). This limit applies to both inbound and outbound messages between the Fivetran platform and
Partner SDK services.