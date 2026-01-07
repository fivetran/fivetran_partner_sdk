# Fivetran Java Destination Connector Example

This example demonstrates how to build a Fivetran destination connector using Java and gRPC. The connector implements the Fivetran Partner SDK to receive data from Fivetran's data pipeline and write it to a custom destination.

## Overview

This Java destination connector example showcases:
- Implementation of the gRPC-based Fivetran Destination Connector interface
- Configuration form creation with conditional fields for different destination types
- Schema and table management (describe, create, alter)
- Batch data writing with file-based operations
- **History Mode support** with specialized batch handling
- Table truncation operations
- Structured JSON logging compatible with Fivetran requirements
- Gradle build configuration with Protocol Buffers

## What This Connector Does

The example destination connector simulates writing data to multiple destination types:

### Supported Destination Types
- **Database**: Traditional relational database (host, port, user, password, database, table)
- **File**: File-based storage (host, port, user, password, table, file path)
- **Cloud**: Cloud storage services (host, port, user, password, region)

### Data Operations
- **Table Management**: Create, alter, describe, and truncate tables using DuckDB
- **Batch Writing**: Process data files in batches for optimal performance
- **History Mode**: Advanced historical data tracking with specialized batch processing
- **Configuration Testing**: Validate connectivity and configuration settings

### Key Features
- **Conditional Configuration**: Dynamic form fields based on destination type selection
- **DuckDB Storage**: Real database operations with data persistence (file-based or in-memory)
- **File-Based Batch Processing**: Handles replace, update, and delete operations via files
- **History Mode Batching**: Specialized handling for time-based data versioning

### Data Storage
The connector uses **DuckDB** for data persistence:
- **Database File**: `destination.db`
- **Persistence**: Data survives connector restarts
- **Multi-Schema Support**: Tables organized by schema (default: `fivetran_destination`)
- **In-Memory Option**: Can be configured to use in-memory database for testing

## Prerequisites

- **JDK v17** or later
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
   
   # Or download from https://adoptium.net/
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

3. **Run the destination connector**:
   ```bash
   java -jar build/libs/JavaDestination.jar
   ```

### Using Gradle Wrapper (Recommended)

If you don't have Gradle installed locally:

```bash
# Copy proto files
./gradlew copyProtos

# Build the JAR
./gradlew jar

# Run the connector (default port 50052)
java -jar build/libs/JavaDestination.jar
```

### Alternative: Direct Gradle Run

```bash
./gradlew run
```

### Custom Port

```bash
java -jar build/libs/JavaDestination.jar --port 50053
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
Copies the Fivetran SDK protocol buffer definitions from the repository root.

### 2. Protocol Buffer Compilation (Automatic)
The `com.google.protobuf` Gradle plugin automatically:
- Compiles `.proto` files to Java classes
- Generates gRPC service stubs for `DestinationConnector`
- Places generated code in `build/generated/source/proto/main/`

### 3. Java Compilation and JAR Creation
Creates a fat JAR with all dependencies, including:
- gRPC runtime libraries
- Protocol buffer utilities
- Jackson for JSON processing
- ZStandard compression (zstd-jni)
- CSV processing capabilities

## Implementation Details

### Core Classes

#### 1. `JavaDestination.java`
- Main entry point that starts the gRPC server
- **Default port**: 50052 (different from source connectors)
- Configurable port via `--port` argument

#### 2. `DestinationServiceImpl.java`
Implements the main gRPC destination service methods:
- **DuckDB integration**: Uses `DuckDBHelper` for data storage and persistence
- **Table operations**: Delegates operations to `TableOperationsHelper`
- **Schema migrations**: Uses `SchemaMigrationHelper` for table modifications

#### 3. `DuckDBHelper.java`
Database operations helper:
- **Connection management**: Handles the DuckDB JDBC connection lifecycle
- **Transaction support**: Provides BEGIN/COMMIT/ROLLBACK for multi-step operations
- **SQL operations**: Create, alter, drop tables and columns
- **Type mapping**: Converts between Fivetran DataType and SQL types
- **Persistence**: Stores data in `destination.db` file (or in-memory)

#### 4. `TableOperationsHelper.java`
Table operations handler:
- **CreateTable**: Creates new tables with specified schema
- **DescribeTable**: Queries table metadata from DuckDB
- **AlterTable**: Handles column additions, type changes, and primary key changes
- **Truncate**: Implements both hard and soft truncate operations

#### 5. `TableMetadataHelper.java`
Table metadata utilities:
- **History mode columns**: Manages `_fivetran_start`, `_fivetran_end`, `_fivetran_active`
- **Soft delete columns**: Handles soft delete column operations
- **Table copying**: Utilities for table structure manipulation

#### 6. `SchemaMigrationHelper.java`
Schema migration operations:
- **Table migrations**: Drop, copy, rename tables
- **Column operations**: Add, copy, remove columns
- **Sync mode changes**: Handle transitions between update and history modes

### Destination Connector Methods

#### 1. `configurationForm()`
Creates a sophisticated configuration UI with conditional fields:

**Main Configuration Field:**
- **Writer Type**: Dropdown (Database, File, Cloud)

**Conditional Fields** (shown based on Writer Type selection):
- **Database**: host, port, user, password, database, table
- **File**: host, port, user, password, table, filePath
- **Cloud**: host, port, user, password, region

**Additional Options:**
- **Enable Encryption**: Toggle for data transfer encryption

#### 2. `test()`
- Validates destination configuration
- Tests connectivity to the destination system
- Supports multiple test types (connect, select)

#### 3. `describeTable()`
- Queries table schema information from DuckDB
- Returns column names, data types, and type parameters (precision/scale for DECIMAL)
- Returns `not_found=true` if table doesn't exist

#### 4. `createTable()`
- Creates new tables in DuckDB with the specified schema
- Executes `CREATE TABLE` SQL statement with column definitions
- Logs table creation details with schema information

#### 5. `alterTable()`
- Modifies existing table schemas in DuckDB
- Adds new columns to existing tables (incremental updates) and can drop columns when the `drop_columns` flag is set
- Handles column type changes using `ALTER COLUMN SET DATA TYPE`
- Manages primary key constraint changes
- All operations wrapped in transactions for atomicity

#### 6. `truncate()`
- Removes all data from specified tables using DuckDB `TRUNCATE TABLE`
- Hard truncate: Deletes all rows from the table
- Soft truncate: Updates deleted column flag, supports time-based truncation
- Logs truncation operations with table and schema details

#### 7. `writeBatch()`
- **Main data writing method** for standard operations
- **Note**: This example implementation only prints batch files for demonstration purposes
- **Does NOT write data to DuckDB** - production implementations should implement actual data loading logic
- Processes three types of files:
  - **Replace files**: Complete record replacements
  - **Update files**: Partial record updates
  - **Delete files**: Record deletions
- See: [WriteBatch documentation](https://github.com/fivetran/fivetran_partner_sdk/blob/main/development-guide/destination-connector-development-guide.md#writebatchrequest)

#### 8. `writeHistoryBatch()` **Advanced Feature**
- **Specialized method** for history mode operations
- **Note**: This example implementation only prints batch files for demonstration purposes
- **Does NOT write data to DuckDB** - production implementations should implement actual data loading logic with history tracking
- Processes files in specific order for data consistency:
  1. **`earliest_start_files`**: Records with earliest timestamps
  2. **`replace_files`**: Complete record replacements
  3. **`update_files`**: Partial updates with history tracking
  4. **`delete_files`**: Record deactivation (sets `_fivetran_active` to FALSE)
- See: [How to handle history mode batch files](https://github.com/fivetran/fivetran_partner_sdk/blob/main/how-to-handle-history-mode-batch-files.md)

### History Mode Explained

History Mode is an advanced feature for tracking data changes over time:

```java
/*
 * The `WriteHistoryBatch` method processes batch files in exact order:
 * 1. earliest_start_files - Records with earliest _fivetran_start
 * 2. replace_files - Complete record replacements
 * 3. update_files - Modified column values with history tracking
 * 4. delete_files - Record deactivation
 */
```

**Key Concepts:**
- **`_fivetran_start`**: Timestamp when record became active
- **`_fivetran_end`**: Timestamp when record became inactive
- **`_fivetran_active`**: Boolean indicating if record is currently active
- **Historical Tracking**: Maintains complete change history

### Logging Configuration

Advanced structured JSON logging:
```java
String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}%n",
        level, jsonMessage);
```

**Features:**
- Filters to INFO, WARNING, and SEVERE levels
- JSON format compatible with Fivetran requirements
- Custom formatter for structured output
- Separate message origin identifier for destinations

### Dependencies

Key dependencies in `build.gradle`:
- **gRPC**: `io.grpc:grpc-protobuf:1.61.1` and related packages
- **Protocol Buffers**: `com.google.protobuf:protobuf-java-util:3.25.2`
- **Jackson**: `com.fasterxml.jackson.core:jackson-databind:2.15.2` for JSON/CSV processing
- **ZStandard**: `com.github.luben:zstd-jni:1.5.5-11` for compression
- **CSV Processing**: `jackson-dataformat-csv:2.2.3` for file handling
- **Database**: `org.duckdb:duckdb_jdbc:1.4.3.0` for data storage and persistence
- **JSON Logging**: `com.google.code.gson:gson:2.10.1` for structured logging

## Docker Support

### Build Docker Image
```bash
# First build the JAR
gradle jar

# Build Docker image
docker build -t java-destination .
```

### Run with Docker
```bash
docker run -p 50052:50052 java-destination
```

The Docker container:
- Uses OpenJDK 11 runtime
- **Exposes port 50052** (destination default)
- Includes JVM flags for module access compatibility

## Configuration Options

### Writer Type Selection
The main configuration dropdown that determines which fields are shown:

#### Database Writer
- **Host**: Database server hostname
- **Port**: Database server port
- **User**: Database username
- **Password**: Database password (masked)
- **Database**: Target database name
- **Table**: Target table name

#### File Writer
- **Host**: File server hostname
- **Port**: File server port
- **User**: File server username
- **Password**: File server password (masked)
- **Table**: Target table name
- **File Path**: Destination file path

#### Cloud Writer
- **Host**: Cloud service endpoint
- **Port**: Cloud service port
- **User**: Cloud service username
- **Password**: Cloud service password (masked)
- **Region**: Cloud region (Azure, AWS, Google Cloud)

### Global Options
- **Enable Encryption**: Toggle for secure data transfer

### Configuration Tests
- **connect**: Tests connection to the destination
- **select**: Tests data writing capabilities

## Testing the Connector

Once running, the connector exposes a gRPC server on `localhost:50052`. Test it using:

1. **Fivetran Destination Tester**: Use the testing tools in the repository
2. **gRPC clients**: Connect using grpcurl:
   ```bash
   grpcurl -plaintext localhost:50052 list
   grpcurl -plaintext localhost:50052 fivetran_sdk.v2.DestinationConnector/ConfigurationForm
   ```
3. **Custom test clients**: Write Java test code using the generated gRPC client stubs

## Troubleshooting

### Common Issues

1. **Port conflict with source connector**:
   ```bash
   # Destination uses port 50052 by default (source uses 50051)
   java -jar build/libs/JavaDestination.jar --port 50053
   ```

2. **Gradle build fails**:
   ```bash
   # Clean and rebuild
   ./gradlew clean build
   ```

3. **Protocol buffer compilation errors**:
   ```bash
   # Ensure proto files are copied first
   ./gradlew copyProtos generateProto
   ```

4. **Missing dependencies**:
   ```bash
   # Refresh dependencies
   ./gradlew --refresh-dependencies build
   ```

5. **File processing errors**:
   - Ensure file paths are accessible
   - Check file permissions
   - Verify file format compatibility

### Debug Mode

Enable verbose logging:
```java
// In DestinationServiceImpl constructor
rootLogger.setLevel(Level.FINE);
```

## Performance Considerations

### Batch Processing
- **File-based operations**: Optimized for large data volumes
- **Memory management**: Processes files in streams to avoid memory issues
- **Compression support**: Built-in ZStandard compression for large files
- **Connection pooling**: Implement database connection pooling for production

### History Mode Optimization
- **Sequential processing**: Ensures data consistency
- **Efficient timestamp handling**: Optimized for time-based queries
- **Indexing strategy**: Design appropriate indexes for `_fivetran_start` and `_fivetran_end`

### Scaling Recommendations
- **Parallel file processing**: Process multiple files concurrently where possible
- **Batch size tuning**: Adjust batch sizes based on destination capabilities
- **Memory allocation**: Increase JVM heap size for large datasets
- **Connection optimization**: Use appropriate connection pool sizes

## Security Considerations

- **Password masking**: Configuration passwords are properly masked in forms
- **Connection encryption**: Support for encrypted data transfer
- **Input validation**: Validate all configuration parameters
- **File access control**: Implement proper file system permissions
- **Database security**: Use parameterized queries to prevent SQL injection

This comprehensive destination connector example provides a solid foundation for building production-ready destination connectors that can handle various destination types, complex data operations, and advanced features like history mode tracking.
