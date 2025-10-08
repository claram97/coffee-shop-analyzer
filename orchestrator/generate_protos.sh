#!/bin/bash
# Script to generate Python protobuf code from .proto files

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTOS_DIR="$SCRIPT_DIR/protos"

echo "Installing protobuf dependencies..."
pip install protobuf grpcio-tools

echo "Generating Python code from .proto files..."
python -m grpc_tools.protoc \
    -I"$PROTOS_DIR" \
    --python_out="$PROTOS_DIR" \
    --pyi_out="$PROTOS_DIR" \
    "$PROTOS_DIR"/eof_message.proto \
    "$PROTOS_DIR"/table_data.proto \
    "$PROTOS_DIR"/query_request.proto \
    "$PROTOS_DIR"/envelope.proto

echo "Creating __init__.py for protos package..."
touch "$PROTOS_DIR/__init__.py"

echo "✅ Protobuf code generation complete!"
echo "Generated files:"
ls -la "$PROTOS_DIR"/*.py
