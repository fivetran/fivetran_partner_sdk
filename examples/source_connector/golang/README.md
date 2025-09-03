# Fivetran Go Source Connector Example

This example demonstrates how to build a Fivetran source connector using Go and gRPC. The connector implements the Fivetran Partner SDK to sync data from a custom source to Fivetran's data pipeline.

## Overview

This Go connector example shows:
- How to implement the gRPC-based Fivetran Source Connector interface
- Configuration form creation with conditional fields and various input types
- Schema discovery and table/column definitions
- Data synchronization with different record types (UPSERT, UPDATE, DELETE)
- State management for incremental syncs
- Logging and error handling

## What This Connector Does

The example connector simulates a data source with:
- **Schema**: `schema1` containing two tables
  - `table1`: Contains columns `a1` (primary key, unspecified type) and `a2` (double type)
  - `table2`: Contains column `b1` (primary key, string type)
- **Sample Data**: Generates 3 sample records with UPSERT operations, followed by UPDATE and DELETE operations
- **State Management**: Tracks sync progress using a cursor that increments with each record
- **Configuration Options**: Supports various authentication methods (OAuth2.0, API Key, Basic Auth, None)

## Prerequisites

- Go 1.21 or later
- Protocol Buffers compiler (`protoc`)
- Go protobuf plugins

### Installing Prerequisites

1. Install Go from https://golang.org/doc/install
2. Install Protocol Buffers compiler:
   ```bash
   # macOS
   brew install protobuf
   
   # Ubuntu/Debian
   sudo apt install protobuf-compiler
   ```
3. Install Go protobuf plugins:
   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   ```

## Quick Start

Run all commands from the golang folder root:

```bash
cd examples/source_connector/golang

# Build the connector
scripts/build.sh

# Run the connector server
./main
```

Alternatively, you can use the run script:
```bash
scripts/run.sh
```

## Build Process Explained

### 1. Copy Protocol Buffers (`copy_protos.sh`)
```bash
cp ../../../*.proto proto/
```
Copies the Fivetran SDK protocol buffer definitions from the repository root to the local `proto/` directory.

### 2. Compile Protocol Buffers (`compile_protos.sh`)
```bash
protoc \
    --proto_path=proto \
    --go_out=proto \
    --go_opt=paths=source_relative \
    --go-grpc_out=proto \
    --go-grpc_opt=paths=source_relative \
    common.proto \
    connector_sdk.proto
```
Generates Go code from the protocol buffer definitions, creating both message types and gRPC service stubs.

### 3. Build Application (`build.sh`)
```bash
go build golang_connector/main.go
```
Compiles the Go connector into an executable binary.

## Implementation Details

### Core Methods

The connector implements four main gRPC methods:

#### 1. `ConfigurationForm`
- Defines the configuration UI for the connector
- Supports various field types: text fields, dropdowns, toggles, conditional fields
- Includes connection and selection tests

#### 2. `Schema`
- Returns the available schemas, tables, and columns
- Defines data types and primary keys
- Enables Fivetran to understand the source data structure

#### 3. `Update`
- Performs the actual data synchronization
- Handles state management for incremental syncs
- Sends records with different operation types (UPSERT, UPDATE, DELETE)
- Sends checkpoints to save sync state

#### 4. `Test`
- Validates connector configuration
- Tests connectivity and data access

### State Management

The connector uses a simple JSON state structure:
```go
type MyState struct {
    Cursor int32 `json:"cursor"`
}
```
The cursor tracks the sync progress and is persisted between sync runs.

### Logging

The connector uses structured JSON logging compatible with Fivetran's logging requirements:
```go
func LogMessage(level string, message string) {
    log := map[string]interface{}{
        "level":          level,
        "message":        message,
        "message-origin": "sdk_connector",
    }
    logJSON, _ := json.Marshal(log)
    fmt.Println(string(logJSON))
}
```

## Docker Support

Build and run using Docker:

```bash
# Build the Docker image
docker build -t golang-connector .

# Run the container
docker run -p 50051:50051 golang-connector
```

The connector runs on port 50051 by default.

## Testing the Connector

Once running, the connector exposes a gRPC server on `localhost:50051`. You can test it using:

1. **Fivetran Connector Tester**: Use the provided testing tools in the repository
2. **gRPC clients**: Connect directly using gRPC client tools like grpcurl
3. **Custom test scripts**: Write Go test code using the generated gRPC client stubs

## Customization

To adapt this example for your own data source:

1. **Modify the Schema method**: Define your actual tables and columns
2. **Update the ConfigurationForm**: Add fields specific to your data source
3. **Implement the Update method**: Replace the sample data generation with actual data fetching
4. **Enhance state management**: Track appropriate sync state for your use case
5. **Add error handling**: Implement proper error handling for your data source

## Configuration Options

The example connector supports these configuration fields:
- **API Base URL**: The endpoint for your data source API
- **Authentication Method**: OAuth2.0, API Key, Basic Auth, or None
- **Client ID/Secret**: For OAuth2.0 authentication
- **API Key**: For API key authentication
- **Username/Password**: For basic authentication
- **API Version**: v1, v2, or v3
- **Enable Metrics**: Boolean toggle for metrics collection

## Troubleshooting

### Common Issues

1. **Protocol buffer compilation fails**: Ensure `protoc` and Go plugins are installed and in PATH
2. **Import errors**: Verify that Go modules are properly initialized and dependencies are downloaded
3. **Port already in use**: Change the port using the `-port` flag: `./main -port 50052`
4. **gRPC connection issues**: Ensure the server is running and accessible on the specified port

### Debug Mode

Run with additional logging:
```bash
go run golang_connector/main.go -port 50051
```

The connector will output structured JSON logs showing configuration, state, and operation details.