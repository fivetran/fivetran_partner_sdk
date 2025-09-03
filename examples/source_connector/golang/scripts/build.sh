#!/bin/bash
mkdir -p proto
./scripts/copy_protos.sh
./scripts/compile_protos.sh 
go mod tidy
go mod download
go build golang_connector/main.go