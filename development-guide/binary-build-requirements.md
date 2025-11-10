# Partner SDK Binary Build Requirements

This guide outlines the code structure and build requirements for creating binaries using Fivetran's automated build pipeline. Please ensure your repository follows these standards for successful binary generation.

## General Requirements (All Languages)

- Your repository must be accessible (public or grant access to `dev-menon@fivetran.com`)
- Follow the language-specific requirements below based on your implementation

---

## Go

**Requirements:**

1. **Makefile Structure:**
   - Include a `build` target that creates the binary
   - **DO NOT set `GOBIN`** in your Makefile (Setting GOBIN can interfere with cross-compilation from Mac to Linux in the build pipeline)
   - The `build` target should handle:
     - Installation of local dependencies (e.g., `protoc`)
     - Fivetran proto file download
     - Proto file generation

2. **Binary Output:**
   - Binary must be created in the `bin/` directory
   - Binary must be named `server`
   - Final path: `./bin/server`

3. **Go Module:**
   - Include `go.mod` file specifying Go version

**Example Makefile Snippet:**
```makefile
build:
    # Download and generate protos
    # Install dependencies
    # Build binary
    go build -o bin/server
```

---

## Java - Gradle

**Requirements:**

1. **Gradle Wrapper:**
   - Include `gradlew` script with **LF line endings** (not CRLF)
   - Before committing to git, run: `git config --global core.autocrlf input`
   - Make `gradlew` executable: `chmod +x gradlew`

2. **Build Configuration:**
   - Include a `jar` task in `build.gradle`
   - The pipeline will run: `./gradlew jar`

3. **JAR Output:**
   - JAR file must be in `build/libs/` directory
   - **Only ONE JAR file** should be present in `build/libs/` after build
   - Do not generate multiple JARs (avoid creating separate `-javadoc.jar`, `-sources.jar`, etc.)

4. **Proto Files:**
   - Ensure `src/main/proto/` directory exists or can be created
   - Proto generation should be configured in `build.gradle`

---

## Java - Maven

**Requirements:**

1. **POM Configuration:**
   - Include `pom.xml` with proper packaging configuration
   - The pipeline will run: `mvn clean package`

2. **JAR Output:**
   - JAR file must be in `target/` directory
   - Exclude `original-*.jar` files (shaded/fat JAR should be the main output)
   - **Only ONE non-original JAR file** should be present after build

3. **Proto Files:**
   - Proto files will be automatically downloaded to `src/main/proto/`
   - Configure proto generation in `pom.xml` (e.g., using `protobuf-maven-plugin`)

**Example plugin configuration:**
```xml
<plugin>
    <groupId>org.xolstice.maven.plugins</groupId>
    <artifactId>protobuf-maven-plugin</artifactId>
    <configuration>
        <protoSourceRoot>src/main/proto</protoSourceRoot>
    </configuration>
</plugin>
```

---

## Python

**Requirements:**

1. **Required Files in Root Directory:**
   - `main.py` - Entry point of your application
   - `requirements.txt` - All Python dependencies
   - `build.sh` - Script for downloading and generating proto files

2. **build.sh Requirements:**
   - Must be **executable**: `chmod +x build.sh`
   - Must **NOT be world-writable** (security requirement)
   - Should handle:
     - Downloading Fivetran proto files to `./protos/` directory
     - Generating Python proto files using `protoc`
     - Installing `grpcio-tools` if needed

3. **Binary Output:**
   - Binary will be automatically generated using PyInstaller
   - Output location: `dist/main`
   - Do not create your own PyInstaller configuration (pipeline handles this)

**Example build.sh:**
```bash
#!/bin/bash
set -e

# Download proto files to ./protos/
mkdir -p protos
curl -o protos/common.proto https://raw.githubusercontent.com/fivetran/fivetran_partner_sdk/main/common.proto
curl -o protos/connector_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_partner_sdk/main/connector_sdk.proto
curl -o protos/destination_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_partner_sdk/main/destination_sdk.proto

# Generate Python proto files
python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/*.proto
```

---

## Rust

**Requirements:**

1. **Package Naming:**
   - For **source connectors**: Package name must be `fivetran_source`
   - For **destination connectors**: Package name must be `fivetran_destination`
   - Specified in `Cargo.toml`:
     ```toml
     [package]
     name = "fivetran_source"  # or "fivetran_destination"
     ```

2. **Workspace Support:**
   - If using a Cargo workspace, ensure the appropriate package exists
   - The pipeline will build with: `cargo build --release -p <package_name>`

3. **Git Submodules:**
   - If your project uses git submodules (e.g., for Fivetran SDK protos), include `.gitmodules`
   - All submodules will be automatically initialized: `git submodule update --init --recursive`

4. **Binary Output:**
   - Binary will be created in `target/release/` directory
   - Should produce **exactly one executable** (excluding `.so` and `.d` files)

5. **Dependencies:**
   - If using `zstd-sys`, `bindgen`, or other native dependencies, they will be automatically installed
   - No additional configuration needed

---

## C++

**Requirements:**

1. **Makefile Targets:**
   - Include `build_dependencies` target (optional) - installs all required dependencies
   - Include `build_connector` target (required) - builds the binary
   - If `build_connector` doesn't exist, pipeline will try `build` or `make` default target

2. **Build System:**
   - Use CMake with output directory: `build/Release/`
   - The pipeline will run:
     ```bash
     make build_dependencies  # if target exists
     make build_connector     # or `make build`
     ```

3. **Binary Output:**
   - Binary must be in `build/Release/` directory
   - Binary name **must NOT contain "test"** (e.g., no `integration_tests`, `unit_test`, etc.)
   - Binary must **NOT be a `.so` file**
   - Should produce **exactly one executable** matching above criteria

4. **Compiler Requirements:**
   - Code will be built with **Clang 16**
   - CMake **3.28.2** will be used
   - Ensure code is compatible with these versions

**Example Makefile:**
```makefile
build_dependencies:
    # Install protobuf, dependencies, etc.

build_connector:
    mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$(nproc)
```

---

## Testing Your Setup Locally

Before requesting a binary build, test your setup locally:

### Go:
```bash
GOOS=linux GOARCH=amd64 make build
# Verify: ./bin/server exists
```

### Java (Gradle):
```bash
./gradlew jar
# Verify: Single JAR in build/libs/
```

### Java (Maven):
```bash
mvn clean package
# Verify: Single non-original JAR in target/
```

### Python:
```bash
chmod +x build.sh
./build.sh
pip install pyinstaller
pyinstaller --onefile main.py
# Verify: dist/main exists
```

### Rust:
```bash
cargo build --release -p fivetran_source  # or fivetran_destination
# Verify: Binary in target/release/
```

### C++:
```bash
make build_connector
# Verify: Binary in build/Release/ (not a test binary)
```

---

## Common Issues

| Issue | Solution |
|-------|----------|
| **Windows line endings (CRLF)** | Convert to LF: `dos2unix gradlew` or set git config |
| **Multiple JARs generated** | Configure build to produce only shaded/fat JAR |
| **Missing protos** | For Python, implement `build.sh`; for others, ensure proper directory structure |
| **Test binaries included** | Ensure C++ test binaries are named with "test" in the name |
| **build.sh not executable** | Run `chmod +x build.sh` before committing |
| **Wrong package name (Rust)** | Must be exactly `fivetran_source` or `fivetran_destination` |

---

## Need Help?

If you encounter issues meeting these requirements, please reach out to your Fivetran contact in your private Slack channel with:
- Language you're using
- Build error output
- Link to your repository (if public) or relevant code snippets