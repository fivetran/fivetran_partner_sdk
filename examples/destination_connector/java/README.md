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
- **Table Management**: Create, alter, describe, and truncate tables
- **Batch Writing**: Process data files in batches for optimal performance
- **History Mode**: Advanced historical data tracking with specialized batch processing
- **Configuration Testing**: Validate connectivity and configuration settings

### Key Features
- **Conditional Configuration**: Dynamic form fields based on destination type selection
- **In-Memory Table Registry**: Tracks table schemas and structure
- **File-Based Batch Processing**: Handles replace, update, and delete operations via files
- **History Mode Batching**: Specialized handling for time-based data versioning

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
- Returns table schema information
- Uses in-memory table registry (`tableMap`)
- Returns `not_found` if table doesn't exist

#### 4. `createTable()`
- Creates new tables in the destination
- Stores table schema in memory for future reference
- Logs table creation details

#### 5. `alterTable()`
- Modifies existing table schemas
- Updates the in-memory table registry
- Supports adding/modifying columns

#### 6. `truncate()`
- Removes all data from specified tables
- Supports both hard and soft truncation
- Logs truncation operations

#### 7. `writeBatch()`
- **Main data writing method** for standard operations
- Processes three types of files:
  - **Replace files**: Complete record replacements
  - **Update files**: Partial record updates
  - **Delete files**: Record deletions

#### 8. `writeHistoryBatch()` ⭐ **Advanced Feature**
- **Specialized method** for history mode operations
- Processes files in specific order for data consistency:
  1. **`earliest_start_files`**: Records with earliest timestamps
  2. **`replace_files`**: Complete record replacements
  3. **`update_files`**: Partial updates with history tracking
  4. **`delete_files`**: Record deactivation (sets `_fivetran_active` to FALSE)

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

## Customization Guide

To adapt this example for your actual destination:

### 1. Implement Real Data Writing
In `writeBatch()` and `writeHistoryBatch()`:
```java
@Override
public void writeBatch(WriteBatchRequest request, StreamObserver<WriteBatchResponse> responseObserver) {
    try {
        // Parse configuration
        Map<String, String> config = request.getConfigurationMap();
        String writerType = config.get("writerType");
        
        // Process each file type
        for (String file : request.getReplaceFilesList()) {
            processReplaceFile(file, config);
        }
        for (String file : request.getUpdateFilesList()) {
            processUpdateFile(file, config);
        }
        for (String file : request.getDeleteFilesList()) {
            processDeleteFile(file, config);
        }
        
        responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
    } catch (Exception e) {
        logger.severe("Batch write failed: " + e.getMessage());
        responseObserver.onNext(WriteBatchResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(e.getMessage())
                .build());
    }
    responseObserver.onCompleted();
}
```

### 2. Add Real Database Connectivity
```java
import java.sql.*;

private Connection getConnection(Map<String, String> config) throws SQLException {
    String url = String.format("jdbc:postgresql://%s:%s/%s", 
                              config.get("host"), 
                              config.get("port"), 
                              config.get("database"));
    return DriverManager.getConnection(url, 
                                     config.get("user"), 
                                     config.get("password"));
}
```

### 3. Implement Real Table Operations
```java
@Override
public void createTable(CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {
    try (Connection conn = getConnection(request.getConfigurationMap())) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(request.getTable().getName()).append(" (");
        
        for (Column col : request.getTable().getColumnsList()) {
            sql.append(col.getName()).append(" ").append(mapDataType(col.getType())).append(",");
        }
        sql.deleteCharAt(sql.length() - 1); // Remove last comma
        sql.append(")");
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
        
        responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
    } catch (SQLException e) {
        logger.severe("Create table failed: " + e.getMessage());
        responseObserver.onNext(CreateTableResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(e.getMessage())
                .build());
    }
    responseObserver.onCompleted();
}
```

### 4. Add File Processing Logic
```java
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

private void processReplaceFile(String filePath, Map<String, String> config) {
    try {
        CsvMapper csvMapper = new CsvMapper();
        // Read and process CSV file
        List<Map<String, Object>> records = csvMapper
                .readerFor(Map.class)
                .readValues(new File(filePath))
                .readAll();
        
        // Write to destination
        writeRecordsToDestination(records, config);
    } catch (Exception e) {
        logger.severe("Failed to process replace file: " + e.getMessage());
    }
}
```

### 5. Implement History Mode Processing
```java
@Override
public void writeHistoryBatch(WriteHistoryBatchRequest request, StreamObserver<WriteBatchResponse> responseObserver) {
    try {
        // Process in exact order for data consistency
        
        // 1. Process earliest start files first
        for (String file : request.getEarliestStartFilesList()) {
            processEarliestStartFile(file, request.getConfigurationMap());
        }
        
        // 2. Process replace files
        for (String file : request.getReplaceFilesList()) {
            processHistoryReplaceFile(file, request.getConfigurationMap());
        }
        
        // 3. Process update files with history tracking
        for (String file : request.getUpdateFilesList()) {
            processHistoryUpdateFile(file, request.getConfigurationMap());
        }
        
        // 4. Process delete files (deactivate records)
        for (String file : request.getDeleteFilesList()) {
            processHistoryDeleteFile(file, request.getConfigurationMap());
        }
        
        responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
    } catch (Exception e) {
        logger.severe("History batch write failed: " + e.getMessage());
        responseObserver.onNext(WriteBatchResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage(e.getMessage())
                .build());
    }
    responseObserver.onCompleted();
}
```

### 6. Add Cloud Storage Integration
```java
// For AWS S3
import software.amazon.awssdk.services.s3.S3Client;

// For Azure Blob Storage
import com.azure.storage.blob.BlobServiceClient;

// For Google Cloud Storage
import com.google.cloud.storage.Storage;
```

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
