#!/bin/bash

# Exit on any error
set -e

echo "Setting up Python destination connector..."

# Remove existing virtual environment if it exists
if [ -d "destination_run" ]; then
    echo "Removing existing virtual environment..."
    rm -rf destination_run
fi

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv destination_run

# Activate virtual environment
echo "Activating virtual environment..."
source destination_run/bin/activate

# Upgrade pip to latest version
echo "Upgrading pip..."
pip install --upgrade pip

# Make a directory protos
mkdir -p protos

# Copy proto files to protos directory
echo "Copying protocol buffer files..."
cp ../../../*.proto protos/

# Install the required packages
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Make a directory sdk_pb2
mkdir -p sdk_pb2

# Generate grpc python code and store it in sdk_pb2
echo "Generating gRPC Python stubs..."
python -m grpc_tools.protoc \
       --proto_path=./protos/ \
       --python_out=sdk_pb2 \
       --pyi_out=sdk_pb2 \
       --grpc_python_out=sdk_pb2 protos/*.proto

echo "Build completed successfully!"

# Deactivate virtual environment
deactivate