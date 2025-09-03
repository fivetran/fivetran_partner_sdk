# Fivetran Node.js Source Connector Example

This example demonstrates how to build a Fivetran source connector using Node.js and gRPC. The connector implements the Fivetran Partner SDK to sync data from a custom source to Fivetran's data pipeline.

## Overview

This Node.js connector example showcases:
- Implementation of the gRPC-based Fivetran Source Connector interface using `@grpc/grpc-js`
- Configuration form creation with conditional fields and various input types
- Schema discovery and table/column definitions
- Data synchronization with different record types (UPSERT, UPDATE, DELETE)
- State management for incremental syncs
- Structured JSON logging compatible with Fivetran requirements
- Single Executable Application (SEA) creation using esbuild and Node.js SEA

## What This Connector Does

The example connector simulates a data source with:
- **Schema**: Two tables without schema name
  - `table1`: Contains columns `a1` (primary key, unspecified type) and `a2` (double type)
  - `table2`: Contains columns `b1` (primary key, string type) and `b2` (unspecified type)
- **Sample Data**: Generates 3 sample records with UPSERT operations, followed by UPDATE and DELETE operations
- **State Management**: Tracks sync progress using a cursor that increments with each record
- **Configuration Options**: Supports various authentication methods (OAuth2.0, API Key, Basic Auth, None)

## Prerequisites

- **Node.js v18.0+** (required for Single Executable Application support)
- **npm** (comes with Node.js)
- **macOS** (the build script is macOS-specific; see [Node.js SEA documentation](https://nodejs.org/api/single-executable-applications.html) for other platforms)

### Installing Prerequisites

1. **Install Node.js 18+**:
   ```bash
   # macOS (using Homebrew)
   brew install node@18
   
   # Ubuntu/Debian (using NodeSource)
   curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
   sudo apt-get install -y nodejs
   
   # Or download from https://nodejs.org/
   ```

2. **Verify installation**:
   ```bash
   node --version  # Should show v18.0.0 or higher
   npm --version
   ```

## Quick Start

### Option 1: Using the Build Script (Recommended)

The build script automates the entire process, creating a single executable binary:

```bash
sh build.sh
```

Then run the created binary:
```bash
./binary
```

**Note**: You may see some warnings when running the binary:
- `ExperimentalWarning: Single executable application is an experimental feature` - This is expected as SEA is still experimental in Node.js
- `Warning: Currently the require() provided to the main script...` - This is also expected due to SEA limitations with dynamic module loading

These warnings are harmless and can be suppressed if needed (see Troubleshooting section).

### Option 2: Manual Development Setup

For development and debugging:

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Copy protocol buffer files**:
   ```bash
   mkdir -p src/protos
   cp ../../../*.proto src/protos/
   ```

3. **Run directly with Node.js**:
   ```bash
   node src/index.js
   ```

4. **Or run with custom port**:
   ```bash
   node src/index.js --port 50052
   ```

## Build Process Explained

The `build.sh` script performs the following steps:

### 1. Dependency Installation
```bash
npm install
```
Installs required packages:
- `@grpc/grpc-js`: gRPC implementation for Node.js
- `esbuild`: Fast bundler for JavaScript
- Additional utilities for stream handling

### 2. Protocol Buffer Setup
```bash
mkdir -p src/protos
cp ../../../../*.proto src/protos/
```
Copies Fivetran SDK protocol buffer definitions to the local project.

### 3. Code Bundling
```bash
npm run build  # Runs: esbuild src/index.js --bundle --platform=node --outfile=bundle.js
```
Creates a single bundled JavaScript file containing all dependencies.

### 4. Single Executable Application (SEA) Creation
```bash
# Create SEA configuration
echo '{ "main": "bundle.js", "output": "sea-prep.blob" }' > sea-config.json

# Generate the blob
node --experimental-sea-config sea-config.json

# Create binary copy
cp $(command -v node) binary
```

### 5. Binary Injection and Signing (macOS)
```bash
# Remove existing signature
codesign --remove-signature binary

# Inject the application blob
npx postject binary NODE_SEA_BLOB sea-prep.blob \
    --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2 \
    --macho-segment-name NODE_SEA

# Re-sign the binary
codesign --sign - binary
```

### 6. Cleanup
Removes temporary files (sea-config.json, sea-prep.blob, bundle.js).

## Implementation Details

### Core Implementation (`src/index.js`)

The connector implements four main gRPC service methods:

#### 1. `configurationForm()`
- Defines the configuration UI for the connector
- Supports text fields, dropdowns, toggles, and conditional fields
- Includes connection and selection tests

#### 2. `schema()`
- Returns available tables and columns
- Defines data types and primary keys
- Uses `without_schema` response format

#### 3. `update()`
- Performs data synchronization using streaming responses
- Handles state management for incremental syncs
- Sends records with different operation types (UPSERT, UPDATE, DELETE)
- Sends checkpoints to save sync state

#### 4. `test()`
- Validates connector configuration
- Tests connectivity and data access

### State Management

The connector uses a simple JSON state structure:
```javascript
const state = JSON.parse(state_json);
state.cursor = (state.cursor || 0) + 1;
```

The cursor tracks sync progress and is persisted between sync runs.

### Logging

Structured JSON logging compatible with Fivetran requirements:
```javascript
function logMessage(level, message) {
    console.log(`{"level":"${level}", "message": "${message}", "message-origin": "sdk_connector"}`);
}
```

Supports INFO, WARNING, and SEVERE log levels.

### Dependencies

Key dependencies in `package.json`:
- **@grpc/grpc-js**: Pure JavaScript gRPC implementation
- **esbuild**: Fast JavaScript bundler for creating single file
- **path & stream**: Node.js utilities for file and stream operations

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

## Platform Support

### macOS (Full Support)
The included build script works out-of-the-box on macOS with code signing.

### Other Platforms
For Linux, Windows, or other platforms, modify the build script according to the [Node.js SEA documentation](https://nodejs.org/api/single-executable-applications.html):

#### Linux Example
```bash
# Replace codesign commands with:
# (No code signing required on Linux)

# Use appropriate binary injection for Linux
npx postject binary NODE_SEA_BLOB sea-prep.blob \
    --sentinel-fuse NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2
```

#### Windows Example
```bash
# Use .exe extension and Windows-specific postject options
cp $(command -v node) binary.exe
# ... modify postject command for Windows
```

## Testing the Connector

Once running, the connector exposes a gRPC server on `localhost:50051`. Test it using:

1. **Fivetran Connector Tester**: Use the testing tools in the repository
2. **gRPC clients**: Connect using grpcurl:
   ```bash
   grpcurl -plaintext localhost:50051 list
   grpcurl -plaintext localhost:50051 fivetran_sdk.v2.SourceConnector/ConfigurationForm
   ```
3. **Custom test clients**: Write Node.js test code using `@grpc/grpc-js`

## Customization Guide

To adapt this example for your own data source:

### 1. Modify Schema Definition
In the `schema()` function:
```javascript
const tableList = {
  tables: [
    {
      name: "your_table",
      columns: [
        { name: "your_column", type: "STRING", primary_key: true }
      ]
    }
  ]
};
```

### 2. Update Configuration Form
In the `configurationForm()` function:
- Add fields specific to your data source
- Modify authentication options
- Update validation tests

### 3. Implement Data Fetching
In the `update()` function:
- Replace sample data generation with actual API calls
- Add HTTP client libraries (axios, fetch, etc.)
- Implement proper error handling and retry logic

### 4. Enhance State Management
- Extend the state object with relevant tracking information
- Implement incremental sync logic
- Handle pagination cursors or timestamps

### 5. Add Data Validation
- Validate configuration parameters
- Implement proper error responses
- Add input sanitization and type checking

## Development Tips

### Hot Reloading
For development, use nodemon:
```bash
npm install -g nodemon
nodemon src/index.js --port 50051
```

### Debugging
Add debug logging:
```javascript
const DEBUG = process.env.DEBUG === 'true';
if (DEBUG) {
    console.log('Debug info:', data);
}
```

Run with debug mode:
```bash
DEBUG=true node src/index.js
```

### Adding HTTP Clients
For real data sources, add HTTP client dependencies:
```bash
npm install axios
# or
npm install node-fetch
```

## Troubleshooting

### Common Issues

1. **Node.js version too old**:
   ```bash
   node --version  # Must be 18.0.0 or higher for SEA support
   ```

2. **Build script fails on non-macOS**:
   - Remove or modify codesign commands
   - Adjust postject parameters for your platform
   - See Node.js SEA documentation for platform-specific instructions

3. **gRPC connection issues**:
   ```bash
   # Check if port is available
   lsof -i :50051
   
   # Use different port
   ./binary --port 50052
   ```

4. **Protocol buffer errors**:
   ```bash
   # Ensure proto files are copied
   ls src/protos/
   
   # Should contain: connector_sdk.proto, common.proto, destination_sdk.proto
   ```

5. **Permission denied on binary**:
   ```bash
   chmod +x binary
   ```

6. **SEA Warnings (Expected and Harmless)**:
   
   You may see these warnings when running the binary - they are expected:
   
   ```
   (node:xxxxx) ExperimentalWarning: Single executable application is an experimental feature and might change at any time
   (node:xxxxx) Warning: Currently the require() provided to the main script embedded into single-executable applications only supports loading built-in modules.
   ```
   
   **Why these occur**:
   - SEA is still experimental in Node.js 18+
   - Dynamic module loading has limitations in SEA
   - Our example works fine because all dependencies are bundled
   
   **To suppress warnings** (optional):
   ```bash
   # Suppress experimental warnings
   ./binary --no-warnings
   
   # Or suppress specific warning types
   NODE_NO_WARNINGS=1 ./binary
   ```

### Debug Mode

For detailed gRPC debugging:
```bash
GRPC_VERBOSITY=DEBUG GRPC_TRACE=all node src/index.js
```

## Performance Considerations

- **Bundle Size**: The bundled application is ~50MB including Node.js runtime
- **Startup Time**: Fast startup (~2-3 seconds) due to bundled dependencies
- **Memory Usage**: Typical Node.js memory footprint (~50-100MB)
- **Concurrency**: Single-threaded event loop handles multiple concurrent connections
- **Streaming**: Efficient streaming for large datasets using gRPC streaming responses

## Security Notes

- **Code Signing**: The macOS build includes code signing for security
- **Binary Distribution**: SEA creates a tamper-evident executable
- **Dependency Bundling**: All dependencies are bundled, reducing external attack surface
- **Configuration Validation**: Always validate and sanitize configuration inputs