# Fivetran Java Source Connector Example

This example demonstrates how to build a Fivetran source connector using Java and gRPC. The connector implements the Fivetran Partner SDK to sync data from a custom source to Fivetran's data pipeline.

## Overview

This Java connector example showcases:
- Implementation of the gRPC-based Fivetran Source Connector interface
- Configuration form creation with conditional fields and various input types
- Schema discovery and table/column definitions
- Data synchronization with different record types (UPSERT, UPDATE, DELETE)
- State management for incremental syncs
- Structured JSON logging compatible with Fivetran requirements
- Gradle build configuration with Protocol Buffers

## What This Connector Does

The example connector simulates a data source with:
- **Schema**: Two tables without schema name
  - `table1`: Contains columns `a1` (primary key, unspecified type) and `a2` (double type)
  - `table2`: Contains columns `b1` (primary key, string type) and `b2` (unspecified type)
- **Sample Data**: Generates 3 sample records with UPSERT operations, followed by UPDATE and DELETE operations
- **State Management**: Tracks sync progress using a cursor that increments with each record
- **Configuration Options**: Supports various authentication methods (OAuth2.0, API Key, Basic Auth, None)

## Prerequisites

- **JDK v17** or later  
  > **Note:** JDK 17+ is required for compatibility with Gradle 8 and the build tooling, even though the connector targets Java 8 bytecode for maximum compatibility.
- **Gradle 8** or later
- Protocol Buffers compiler (automatically handled by Gradle plugin)

### Installing Prerequisites

1. **Install JDK 17+**:
   ```bash
   # macOS (using Homebrew)
   brew install openjdk@17
   
   # Ubuntu/Debian
   sudo apt update
   sudo apt install openjdk-17-jdk
   ```

2. **Install Gradle 8+**:
   ```bash
   # macOS (using Homebrew)
   brew install gradle
   
   # Ubuntu/Debian
   sudo apt install gradle
   
   # Or use the included Gradle wrapper (gradlew)
   ```

## Quick Start

### Using Gradle Commands

1. **Copy Protocol Buffer definitions**:
   ```bash
   gradle copyProtos
   ```

2. **Build the JAR file**:
   ```bash
   gradle jar
   ```

3. **Run the connector**:
   ```bash
   java -jar build/libs/JavaConnector.jar
   ```

### Using Gradle Wrapper (Recommended)

If you don't have Gradle installed locally, use the included wrapper:

```bash
# Copy proto files
./gradlew copyProtos

# Build the JAR
./gradlew jar

# Run the connector
java -jar build/libs/JavaConnector.jar
```

### Alternative: Direct Gradle Run

```bash
./gradlew run
```

## Build Process Explained

### 1. Copy Protocol Buffers (`copyProtos` task)
```groovy
tasks.register('copyProtos', Copy) {
    from file("$rootDir/../../..")
    into file("src/main/proto/")
    include "*.proto"
}
```
Copies the Fivetran SDK protocol buffer definitions from the repository root to `src/main/proto/`.

### 2. Protocol Buffer Compilation (Automatic)
The `com.google.protobuf` Gradle plugin automatically:
- Compiles `.proto` files to Java classes
- Generates gRPC service stubs
- Places generated code in `build/generated/source/proto/main/`

### 3. Java Compilation and JAR Creation
```groovy
jar {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes('Main-Class' : 'connector.JavaConnector')
    }
    from {
        configurations.runtimeClasspath.collect { it.isDirectory() ? it : zipTree(it) }
    }
}
```
Creates a fat JAR with all dependencies included.

## Implementation Details

### Core Classes

#### 1. `JavaConnector.java`
- Main entry point that starts the gRPC server
- Configurable port (default: 50051)
- Usage: `java -jar JavaConnector.jar --port 50052`

#### 2. `ConnectorServiceImpl.java`
Implements the four main gRPC service methods:

**`configurationForm()`**
- Defines the configuration UI for the connector
- Supports text fields, dropdowns, toggles, and conditional fields
- Includes connection and selection tests

**`schema()`**
- Returns available tables and columns
- Defines data types and primary keys
- Uses `WithoutSchema` response (no schema grouping)

**`update()`**
- Performs data synchronization
- Handles state management for incremental syncs
- Sends records with different operation types
- Sends checkpoints to save sync state

**`test()`**
- Validates connector configuration
- Tests connectivity and data access

#### 3. `State.java`
Simple state management class:
```java
public class State {
    public Integer cursor = 0;
}
```

### Logging Configuration

The connector implements structured JSON logging:
```java
String.format("{\"level\":\"%s\", \"message\": %s, \"message-origin\": \"sdk_connector\"}%n",
        level, jsonMessage);
```

Features:
- Filters to only log INFO, WARNING, and SEVERE levels
- JSON format compatible with Fivetran requirements
- Custom formatter for structured output

### Dependencies

Key dependencies defined in `build.gradle`:
- **gRPC**: `io.grpc:grpc-protobuf:1.59.1` and related packages
- **Protocol Buffers**: `com.google.protobuf:protobuf-java-util:3.25.1`
- **Jackson**: `com.fasterxml.jackson.core:jackson-databind:2.14.1` for JSON processing
- **Netty**: `io.grpc:grpc-netty-shaded:1.59.1` for networking

## Docker Support

### Build Docker Image
```bash
# First build the JAR
gradle jar

# Build Docker image
docker build -t java-connector .
```

### Run with Docker
```bash
docker run -p 50051:50051 java-connector
```

The Docker container:
- Uses OpenJDK 11 runtime
- Exposes port 50051
- Includes JVM flags for module access compatibility

## Configuration Options

The example connector supports these configuration fields:

### Required Fields
- **API Base URL**: The endpoint for your data source API

### Authentication Method (Dropdown)
- **OAuth2.0**: Shows Client ID and Client Secret fields
- **API Key**: Shows API Key field  
- **Basic Auth**: Shows Username and Password fields
- **None**: No additional authentication fields

### Optional Fields
- **API Version**: v1, v2, or v3 (default: v2)
- **Enable Metrics**: Boolean toggle for metrics collection

### Configuration Tests
- **connect**: Tests connection to the data source
- **select**: Tests data selection capabilities

## Testing the Connector

Once running, the connector exposes a gRPC server on `localhost:50051`. Test it using:

1. **Fivetran Connector Tester**: Use the testing tools in the repository
2. **gRPC clients**: Connect using grpcurl or similar tools:
   ```bash
   grpcurl -plaintext localhost:50051 list
   ```
3. **Custom test clients**: Write Java test code using the generated gRPC client stubs

## Customization Guide

To adapt this example for your own data source:

### 1. Modify Schema Definition
In `ConnectorServiceImpl.schema()`:
```java
Table.newBuilder().setName("your_table").addAllColumns(
    Arrays.asList(
        Column.newBuilder().setName("your_column").setType(DataType.STRING).setPrimaryKey(true).build()
    )
).build()
```

### 2. Update Configuration Form
In `ConnectorServiceImpl.getConfigurationForm()`:
- Add fields specific to your data source
- Modify authentication options
- Update validation tests

### 3. Implement Data Fetching
In `ConnectorServiceImpl.update()`:
- Replace sample data generation with actual API calls
- Implement proper error handling
- Add retry logic for failed requests

### 4. Enhance State Management
- Extend the `State` class with relevant tracking information
- Implement incremental sync logic
- Handle pagination cursors or timestamps

### 5. Add Data Validation
- Validate configuration parameters
- Implement proper error responses
- Add input sanitization

## Troubleshooting

### Common Issues

1. **Gradle build fails**:
   ```bash
   # Clean and rebuild
   ./gradlew clean build
   ```

2. **Protocol buffer compilation errors**:
   ```bash
   # Ensure proto files are copied first
   ./gradlew copyProtos generateProto
   ```

3. **Port already in use**:
   ```bash
   # Use a different port
   java -jar build/libs/JavaConnector.jar --port 50052
   ```

4. **Missing dependencies**:
   ```bash
   # Refresh dependencies
   ./gradlew --refresh-dependencies build
   ```

### Debug Mode

Enable verbose logging by modifying the logger configuration in `ConnectorServiceImpl`:
```java
rootLogger.setLevel(Level.FINE);
```

### Gradle Tasks

Useful Gradle tasks:
```bash
./gradlew tasks              # List all available tasks
./gradlew dependencies       # Show dependency tree  
./gradlew clean             # Clean build artifacts
./gradlew build             # Full build including tests
./gradlew run               # Run without building JAR
```

## Performance Considerations

- **Memory**: The fat JAR includes all dependencies (~50MB)
- **Startup**: gRPC server starts quickly (< 5 seconds)
- **Concurrency**: Single-threaded example, consider thread pools for production
- **Connection Pooling**: Add HTTP connection pooling for external APIs
- **Batch Processing**: Implement batch record sending for large datasets
