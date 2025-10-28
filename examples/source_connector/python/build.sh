#!/bin/bash
set -e  # Exit immediately on error

# Use Python 3.11 for venv
python3.11 -m venv connector_run
source connector_run/bin/activate

# Upgrade base tools
python -m pip install --upgrade pip setuptools wheel

# Install dependencies
python -m pip install -r requirements.txt

# Copy protos
mkdir -p protos
cp ../../../*.proto protos/

# Generate gRPC stubs
mkdir -p sdk_pb2

# Use the exact venv Python binary to run grpc_tools
./connector_run/bin/python -m grpc_tools.protoc \
    --proto_path=./protos/ \
    --python_out=sdk_pb2 \
    --pyi_out=sdk_pb2 \
    --grpc_python_out=sdk_pb2 protos/*.proto

#deactivate
