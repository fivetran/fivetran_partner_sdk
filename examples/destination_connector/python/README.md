# Fivetran Python Destination Connector Example

This example demonstrates how to build a Fivetran destination connector using Python and gRPC. The connector implements the Fivetran Partner SDK to receive data from Fivetran's data pipeline and write it to a custom destination system.

## Overview

This Python destination connector example showcases:
- Implementation of the gRPC-based Fivetran Destination Connector interface
- **Advanced conditional configuration forms** with dynamic field visibility
- Schema and table management (describe, create, alter, truncate)
- Batch data writing with encrypted and compressed file processing
- **History Mode support** with specialized batch handling for time-based data tracking
- **File decryption and decompression** using AES encryption and Zstandard compression
- Structured JSON logging compatible with Fivetran requirements
- Virtual environment isolation for dependency management

## What This Connector Does

The example destination connector simulates writing data to multiple destination types with sophisticated configuration management:

### Supported Destination Types
- **Database**: Traditional relational database (host, port, user, password, database, table)
- **File**: File-based storage (host, port, user, password, table, file path)
- **Cloud**: Cloud storage services (host, port, user, password, region)

### Data Operations
- **Table Management**: Create, alter, describe, and truncate tables with in-memory registry
- **Batch Writing**: Process encrypted and compressed data files for optimal security and performance
- **History Mode**: Advanced historical data tracking with specialized batch processing order
- **File Processing**: Decrypt AES-encrypted files and decompress Zstandard-compressed data
- **Configuration Testing**: Validate connectivity and configuration settings

### Key Features
- **Dynamic Configuration Forms**: Conditional field visibility based on destination type selection
- **In-Memory Table Registry**: Tracks table schemas and metadata across operations
- **Advanced File Processing**: Handles AES decryption and Zstandard decompression
- **History Mode Batch Processing**: Implements precise ordering for historical data consistency
- **Structured Logging**: JSON-formatted logs with severity levels and message origins

## Prerequisites

- **Python 3.9** or later
- **pip** for package management
- **Virtual environment** support (built-in with Python 3.3+)

### Installing Prerequisites

1. **Install Python 3.9+**:
   ```bash
   # macOS (using Homebrew)
   brew install python@3.9
   
   # Ubuntu/Debian
   sudo apt update
   sudo apt install python3.9 python3.9-venv python3.9-pip
   
   # Or download from https://www.python.org/downloads/
   ```

2. **Verify installation**:
   ```bash
   python3 --version  # Should be 3.9 or later
   pip3 --version
   ```

## Quick Start

### Using Build Scripts (Recommended)

1. **Build and setup the environment**:
   ```bash
   sh build.sh
   ```
   
   This script will:
   - Create a virtual environment named `destination_run`
   - Copy protocol buffer definitions from the repository root
   - Install all required Python dependencies
   - Generate gRPC Python stubs from protocol buffers

2. **Run the destination connector**:
   ```bash
   sh run.sh
   ```
   
   This will:
   - Activate the virtual environment
   - Start the connector on the default port (50052)
   - Deactivate the environment when stopped

### Manual Setup (Alternative)

If you prefer manual control:

```bash
# Create virtual environment
python3 -m venv destination_run

# Activate virtual environment
source destination_run/bin/activate

# Copy protocol files
mkdir -p protos
cp ../../../*.proto protos/

# Install dependencies
pip install -r requirements.txt

# Create directory for generated Python stubs
mkdir -p sdk_pb2

# Generate Python gRPC stubs
python -m grpc_tools.protoc --proto_path=protos --python_out=sdk_pb2 --grpc_python_out=sdk_pb2 protos/*.proto

# Run the connector
python main.py

# Deactivate when done
deactivate
```

### Custom Port

```bash
# Run on a custom port
source destination_run/bin/activate
python main.py --port 50053
deactivate
```

## Build Process Explained

### 1. Virtual Environment Setup
```bash
python3 -m venv destination_run
source destination_run/bin/activate
```
Creates an isolated Python environment to avoid dependency conflicts.

### 2. Protocol Buffer Processing
```bash
mkdir -p protos
cp ../../../*.proto protos/
```
Copies the Fivetran SDK protocol buffer definitions from the repository root.

### 3. Dependency Installation
```bash
pip install -r requirements.txt
```
Installs all required packages including:
- **gRPC libraries**: `grpcio==1.65.5` and `grpcio-tools==1.65.5` (Python 3.12 compatible)
- **Protocol Buffers**: `protobuf==5.27.2`
- **Encryption**: `pycryptodome==3.20.0` for AES decryption
- **Compression**: `zstandard~=0.23.0` for Zstandard decompression

### 4. gRPC Code Generation
```bash
mkdir -p sdk_pb2
python -m grpc_tools.protoc --proto_path=protos --python_out=sdk_pb2 --grpc_python_out=sdk_pb2 protos/*.proto
```
Generates Python classes and gRPC service stubs from protocol buffer definitions.

## Implementation Details

### Core Files

#### 1. `main.py`
The main connector implementation containing:
- **DestinationImpl class**: Implements the gRPC destination service interface
- **Server setup**: Configures and starts the gRPC server on port 50052 (default)
- **JSON logging**: Structured logging with severity levels

#### 2. `read_csv.py`
Advanced file processing utilities:
- **AES decryption**: `aes_decrypt()` function for encrypted file processing
- **Zstandard decompression**: `zstd_decompress()` for compressed data
- **CSV parsing and display**: `decrypt_file()` for complete file processing pipeline

### Destination Connector Methods

#### 1. `ConfigurationForm()`
Creates a sophisticated dynamic configuration UI:

**Main Configuration Field:**
- **Writer Type**: Dropdown selector (Database, File, Cloud)

**Conditional Fields** (dynamically shown/hidden):
- **Database Mode**: host, port, user, password, database, table
- **File Mode**: host, port, user, password, table, filePath
- **Cloud Mode**: host, port, user, password, region (Azure/AWS/Google Cloud)

**Global Options:**
- **Enable Encryption**: Toggle for data transfer encryption

**Configuration Tests:**
- **connect**: Tests connection to the destination
- **select**: Tests data selection capabilities

**Implementation Features:**
- **VisibilityCondition**: Controls when fields appear based on Writer Type selection
- **ConditionalFields**: Groups related fields for each destination type
- **Field validation**: Built-in validation for required fields

#### 2. `Test()`
- Validates destination configuration based on test type
- Supports multiple test scenarios (connect, select)
- Returns success/failure status for configuration validation

#### 3. `DescribeTable()`
- Returns table schema information from in-memory registry
- Uses `table_map` dictionary to track table metadata
- Returns `not_found=True` if table doesn't exist

#### 4. `CreateTable()`
- Creates new tables in the destination system
- Stores table schema in `table_map` for future reference
- Logs table creation details with schema information

#### 5. `AlterTable()`
- Modifies existing table schemas
- Updates the in-memory table registry with new schema
- Supports adding/modifying columns and constraints

#### 6. `Truncate()`
- Removes data from specified tables
- Supports both hard and soft truncation modes
- Logs truncation operations with table and schema details

#### 7. `WriteBatch()`
- **Main data writing method** for standard batch operations
- Processes three types of encrypted and compressed files:
  - **Replace files**: Complete record replacements
  - **Update files**: Partial record updates
  - **Delete files**: Record deletions
- **File processing pipeline**: Decryption → Decompression → CSV parsing → Display

#### 8. `WriteHistoryBatch()` ⭐ **Advanced Feature**
- **Specialized method** for history mode operations
- Processes files in **exact order** for data consistency:
  1. **`earliest_start_files`**: Records with earliest `_fivetran_start` timestamps
  2. **`replace_files`**: Complete record replacements
  3. **`update_files`**: Partial updates with history tracking
  4. **`delete_files`**: Record deactivation (sets `_fivetran_active` to FALSE)

### History Mode Deep Dive

History Mode is an advanced Fivetran feature for tracking data changes over time:

```python
'''
The `WriteHistoryBatch` method processes batch files in the exact following order:
1. `earliest_start_files`
2. `replace_files` 
3. `update_files`
4. `delete_files`
'''
```

**Key History Mode Concepts:**
- **`_fivetran_start`**: Timestamp when record became active
- **`_fivetran_end`**: Timestamp when record became inactive
- **`_fivetran_active`**: Boolean indicating if record is currently active

**Processing Order Details:**

1. **`earliest_start_files`**:
   - Contains single record per primary key with earliest `_fivetran_start`
   - Deletes overlapping records with later start times
   - Updates history-specific system columns

2. **`update_files`**:
   - Contains records with modified column values
   - Unmodified columns populated from last active record
   - Maintains complete change history

3. **`replace_files`** (called `upsert_files` in documentation):
   - Contains records where all columns are modified
   - Direct insertion with full record replacement

4. **`delete_files`**:
   - Deactivates records by setting `_fivetran_active` to FALSE
   - Updates `_fivetran_end` timestamp appropriately

### File Processing Pipeline

The connector handles sophisticated file processing:

#### 1. AES Decryption (`aes_decrypt`)
```python
def aes_decrypt(key, ciphertext):
    cipher = AES.new(key, AES.MODE_CBC, iv=ciphertext[:AES.block_size])
    plaintext = cipher.decrypt(ciphertext[AES.block_size:])
    return plaintext.rstrip(b'\0')
```
- Uses AES CBC mode encryption
- Extracts initialization vector from ciphertext
- Removes padding after decryption

#### 2. Zstandard Decompression (`zstd_decompress`)
```python
def zstd_decompress(compressed_data):
    decompressor = ZstdDecompressor()
    decompressed_data = decompressor.decompressobj().decompress(compressed_data)
    return decompressed_data
```
- High-performance compression algorithm
- Better compression ratios than gzip
- Optimized for speed and efficiency

#### 3. CSV Processing and Display (`decrypt_file`)
```python
def decrypt_file(input_file_path, value):
    # Read encrypted file → Decrypt → Decompress → Parse CSV → Display
```
- Complete processing pipeline from encrypted file to readable data
- Formatted output with headers and data alignment
- Error handling for corrupted or invalid files

### Logging Configuration

Advanced structured JSON logging for Fivetran compatibility:

```python
def log_message(level, message):
    print(f'{{"level":"{level}", "message": "{message}", "message-origin": "sdk_destination"}}')
```

**Features:**
- **Severity levels**: INFO, WARNING, SEVERE
- **JSON format**: Compatible with Fivetran log processing
- **Message origin**: Identifies logs as coming from destination connector
- **Structured data**: Easy parsing and filtering

**Usage examples:**
```python
log_message(INFO, "Data loading started for table " + table_name)
log_message(WARNING, "Configuration validation warning")
log_message(SEVERE, "Critical error in batch processing")
```

## Dependencies Explained

### Core Dependencies in `requirements.txt`

#### gRPC and Protocol Buffers
- **`grpcio==1.65.5`**: Core gRPC library for service communication (Python 3.12 compatible)
- **`grpcio-tools==1.65.5`**: Tools for generating Python code from proto files (Python 3.12 compatible)
- **`protobuf==5.27.2`**: Protocol buffer runtime for message serialization

#### Encryption and Compression
- **`pycryptodome==3.20.0`**: AES encryption for secure file processing
- **`zstandard~=0.23.0`**: High-performance compression/decompression

#### Development Tools
- **`google~=3.0.0`**: Google API utilities
- **`pip~=24.0`**: Package manager (specified for consistency)
- **`setuptools~=70.0.0`**: Build tools and utilities

### Virtual Environment Benefits

Using a virtual environment provides:
- **Dependency isolation**: Prevents conflicts with system Python packages
- **Reproducible builds**: Exact dependency versions for consistent behavior
- **Easy cleanup**: Simple environment deletion without affecting system
- **Development safety**: Safe experimentation without breaking other projects

## Testing the Connector

Once running, the connector exposes a gRPC server on `localhost:50052`. Test it using:

### 1. Fivetran Destination Tester
Use the testing tools provided in the repository:
```bash
# Navigate to tester directory
cd ../../tools/destination-connector-tester/
# Follow tester-specific instructions
```

### 2. gRPC Command Line Tools
```bash
# List available services
grpcurl -plaintext localhost:50052 list

# Test configuration form
grpcurl -plaintext localhost:50052 fivetran_sdk.v2.DestinationConnector/ConfigurationForm

# Test connectivity
grpcurl -plaintext -d '{"name": "connect", "configuration": {}}' localhost:50052 fivetran_sdk.v2.DestinationConnector/Test
```

### 3. Custom Python Test Client
```python
import grpc
import sys
sys.path.append('sdk_pb2')

from sdk_pb2 import destination_sdk_pb2
from sdk_pb2 import destination_sdk_pb2_grpc

# Create gRPC channel
channel = grpc.insecure_channel('localhost:50052')
stub = destination_sdk_pb2_grpc.DestinationConnectorStub(channel)

# Test configuration form
response = stub.ConfigurationForm(destination_sdk_pb2.ConfigurationFormRequest())
print(f"Configuration form: {response}")
```

## Customization Guide

To adapt this example for your actual destination system:

### 1. Implement Real Data Writing

Replace the print statements in `WriteBatch()` with actual data writing logic:

```python
def WriteBatch(self, request, context):
    try:
        # Extract configuration
        config = dict(request.configuration)
        writer_type = config.get('writerType', 'Database')
        
        # Process files based on destination type
        if writer_type == 'Database':
            self.write_to_database(request, config)
        elif writer_type == 'File':
            self.write_to_file_system(request, config)
        elif writer_type == 'Cloud':
            self.write_to_cloud_storage(request, config)
        
        return destination_sdk_pb2.WriteBatchResponse(success=True)
    except Exception as e:
        log_message(SEVERE, f"Batch write failed: {e}")
        return destination_sdk_pb2.WriteBatchResponse(
            success=False, 
            error_message=str(e)
        )
```

### 2. Add Database Connectivity

```python
import psycopg2  # For PostgreSQL
import pymongo   # For MongoDB
import sqlite3   # For SQLite

def write_to_database(self, request, config):
    # PostgreSQL example
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )
    
    cursor = conn.cursor()
    
    # Process each file type
    for file_path in request.replace_files:
        self.process_replace_file(file_path, cursor, request.keys)
    
    conn.commit()
    conn.close()
```

### 3. Implement Cloud Storage Integration

```python
import boto3           # For AWS S3
import azure.storage   # For Azure Blob Storage
from google.cloud import storage  # For Google Cloud Storage

def write_to_cloud_storage(self, request, config):
    if config['region'] == 'AWS':
        s3_client = boto3.client('s3',
            aws_access_key_id=config['user'],
            aws_secret_access_key=config['password']
        )
        # Process files to S3
        
    elif config['region'] == 'Azure':
        # Azure Blob Storage implementation
        pass
        
    elif config['region'] == 'Google Cloud':
        # Google Cloud Storage implementation
        pass
```

### 4. Enhanced File Processing

```python
import pandas as pd
import json

def process_replace_file(self, file_path, cursor, encryption_keys):
    try:
        # Decrypt and decompress file
        encryption_key = encryption_keys.get(file_path)
        decrypted_data = self.decrypt_and_decompress_file(file_path, encryption_key)
        
        # Parse CSV data
        df = pd.read_csv(io.StringIO(decrypted_data))
        
        # Process each record
        for _, row in df.iterrows():
            # Insert into database
            cursor.execute(
                "INSERT INTO {} ({}) VALUES ({})".format(
                    self.get_table_name(),
                    ', '.join(df.columns),
                    ', '.join(['%s'] * len(df.columns))
                ),
                tuple(row)
            )
            
    except Exception as e:
        log_message(SEVERE, f"Failed to process replace file {file_path}: {e}")
        raise
```

### 5. Advanced History Mode Implementation

```python
def WriteHistoryBatch(self, request, context):
    try:
        config = dict(request.configuration)
        
        # Process in exact order for data consistency
        
        # 1. Process earliest start files
        for file_path in request.earliest_start_files:
            self.process_earliest_start_file(file_path, config, request.keys)
        
        # 2. Process replace files
        for file_path in request.replace_files:
            self.process_history_replace_file(file_path, config, request.keys)
        
        # 3. Process update files with history tracking
        for file_path in request.update_files:
            self.process_history_update_file(file_path, config, request.keys)
        
        # 4. Process delete files (deactivate records)
        for file_path in request.delete_files:
            self.process_history_delete_file(file_path, config, request.keys)
        
        return destination_sdk_pb2.WriteBatchResponse(success=True)
        
    except Exception as e:
        log_message(SEVERE, f"History batch write failed: {e}")
        return destination_sdk_pb2.WriteBatchResponse(
            success=False,
            error_message=str(e)
        )

def process_history_delete_file(self, file_path, config, keys):
    # Deactivate records instead of deleting them
    # UPDATE table SET _fivetran_active = FALSE, _fivetran_end = NOW() WHERE primary_key IN (...)
    pass
```

### 6. Configuration Form Enhancements

```python
def ConfigurationForm(self, request, context):
    form_fields = common_pb2.ConfigurationFormResponse(
        schema_selection_supported=True,
        table_selection_supported=True
    )
    
    # Add custom validation rules
    validation_rule = common_pb2.ValidationRule(
        field_name="port",
        rule_type=common_pb2.ValidationRule.RANGE,
        min_value=1,
        max_value=65535
    )
    
    # Add custom field types
    batch_size_field = common_pb2.FormField(
        name="batchSize",
        label="Batch Size",
        description="Number of records per batch",
        text_field=common_pb2.TextField.PlainText,
        default_value="1000"
    )
    
    form_fields.fields.append(batch_size_field)
    return form_fields
```

## Troubleshooting

### Common Issues

1. **Virtual environment activation fails**:
   ```bash
   # Ensure virtual environment was created successfully
   ls -la destination_run/
   
   # Recreate if necessary
   rm -rf destination_run
   python3 -m venv destination_run
   ```

2. **Python 3.12 compatibility error** (`AttributeError: module 'pkgutil' has no attribute 'ImpImporter'`):
   ```bash
   # This error occurs with older grpcio-tools versions on Python 3.12
   # The build.sh script now uses updated compatible versions
   
   # If you see this error, clean and rebuild:
   rm -rf destination_run
   sh build.sh
   
   # Or manually update to compatible versions:
   pip install grpcio==1.65.5 grpcio-tools==1.65.5 protobuf==5.27.2
   ```

3. **Protocol buffer compilation errors**:
   ```bash
   # Ensure proto files are copied
   ls -la protos/
   
   # Manual protocol buffer compilation
   python -m grpc_tools.protoc --proto_path=protos --python_out=sdk_pb2 --grpc_python_out=sdk_pb2 protos/*.proto
   ```

4. **Dependency installation failures**:
   ```bash
   # Update pip first
   pip install --upgrade pip
   
   # Install with verbose output
   pip install -r requirements.txt -v
   ```

5. **Port conflicts**:
   ```bash
   # Check if port is already in use
   lsof -i :50052
   
   # Use a different port
   python main.py --port 50053
   ```

6. **File decryption errors**:
   - Ensure encryption keys are provided correctly
   - Verify file is properly encrypted with AES CBC mode
   - Check file permissions and accessibility

7. **gRPC connection issues**:
   ```bash
   # Test basic connectivity
   telnet localhost 50052
   
   # Verify gRPC server is running
   grpcurl -plaintext localhost:50052 list
   ```

### Debug Mode

Enable detailed logging for troubleshooting:

```python
import logging

# Add to main.py
logging.basicConfig(level=logging.DEBUG)

# Enhanced error logging in methods
try:
    # Your code here
    pass
except Exception as e:
    log_message(SEVERE, f"Detailed error: {e}")
    import traceback
    traceback.print_exc()
    raise
```

### Performance Monitoring

```python
import time

def WriteBatch(self, request, context):
    start_time = time.time()
    try:
        # Your processing logic
        pass
    finally:
        processing_time = time.time() - start_time
        log_message(INFO, f"Batch processing completed in {processing_time:.2f} seconds")
```

## Performance Considerations

### File Processing Optimization
- **Streaming processing**: Process large files in chunks to manage memory usage
- **Parallel processing**: Use threading for multiple file processing when safe
- **Caching**: Cache frequently accessed table schemas and configurations

### Memory Management
- **Generator functions**: Use generators for large dataset processing
- **File cleanup**: Properly close file handles and clean temporary files
- **Connection pooling**: Reuse database connections for better performance

### Security Best Practices
- **Password handling**: Never log passwords or sensitive configuration data
- **File permissions**: Ensure proper file system permissions for temporary files
- **Encryption validation**: Verify encryption key validity before processing
- **Input sanitization**: Validate all configuration inputs to prevent injection attacks

This comprehensive Python destination connector example provides a robust foundation for building production-ready destination connectors with advanced features like conditional configuration forms, encrypted file processing, and history mode support.