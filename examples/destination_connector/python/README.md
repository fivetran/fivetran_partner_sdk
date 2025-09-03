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

#### 8. `WriteHistoryBatch()`  **Advanced Feature**
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
Follow this guide: https://github.com/fivetran/fivetran_partner_sdk/blob/main/tools/destination-connector-tester/README.md
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

2. **Protocol buffer compilation errors**:
   ```bash
   # Ensure proto files are copied
   ls -la protos/
   
   # Manual protocol buffer compilation
   python -m grpc_tools.protoc --proto_path=protos --python_out=sdk_pb2 --grpc_python_out=sdk_pb2 protos/*.proto
   ```

3. **Dependency installation failures**:
   ```bash
   # Update pip first
   pip install --upgrade pip
   
   # Install with verbose output
   pip install -r requirements.txt -v
   ```

4. **Port conflicts**:
   ```bash
   # Check if port is already in use
   lsof -i :50052
   
   # Use a different port
   python main.py --port 50053
   ```

5. **File decryption errors**:
   - Ensure encryption keys are provided correctly
   - Verify file is properly encrypted with AES CBC mode
   - Check file permissions and accessibility

6. **gRPC connection issues**:
   ```bash
   # Test basic connectivity
   telnet localhost 50052
   
   # Verify gRPC server is running
   grpcurl -plaintext localhost:50052 list
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