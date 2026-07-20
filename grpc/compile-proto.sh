#!/usr/bin/env bash

set -euo pipefail

export PATH="${PATH}:$(go env GOPATH)/bin"

protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false,paths=source_relative ./quanta.proto
