#!/bin/bash
# Generate architecture diagram from drawio file
# Requires: drawio CLI (brew install --cask drawio)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

DRAWIO_FILE="$BASE_DIR/docs/architecture.drawio"
OUTPUT_FILE="$BASE_DIR/docs/images/architecture.png"

# Check if drawio is installed
if ! command -v drawio &> /dev/null; then
    echo "Error: drawio CLI not found"
    echo "Install with: brew install --cask drawio"
    exit 1
fi

# Check if source file exists
if [[ ! -f "$DRAWIO_FILE" ]]; then
    echo "Error: Source file not found: $DRAWIO_FILE"
    exit 1
fi

# Create output directory if needed
mkdir -p "$(dirname "$OUTPUT_FILE")"

# Export to PNG (2x scale for high resolution)
echo "Exporting $DRAWIO_FILE to $OUTPUT_FILE..."
drawio --export --format png --scale 2 --output "$OUTPUT_FILE" "$DRAWIO_FILE"

if [[ $? -eq 0 ]]; then
    echo "Done! Generated: $OUTPUT_FILE"
    ls -lh "$OUTPUT_FILE"
else
    echo "Error: Export failed"
    exit 1
fi
