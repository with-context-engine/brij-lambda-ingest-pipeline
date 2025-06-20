#!/bin/bash

# Script to build PyMuPDF Lambda Layer
# This creates a zip file containing PyMuPDF that can be used as a Lambda layer

set -e

echo "Building PyMuPDF Lambda Layer..."

# Create temporary directory
TEMP_DIR=$(mktemp -d)
LAYER_DIR="$TEMP_DIR/python"

# Create python directory for Lambda layer structure
mkdir -p "$LAYER_DIR"

# Install PyMuPDF into the layer directory
echo "Installing PyMuPDF..."
pip3 install PyMuPDF -t "$LAYER_DIR"

# Create the layer zip file
echo "Creating layer zip file..."
cd "$TEMP_DIR"
zip -r pymupdf_layer.zip python/

# Move the zip file to project root
mv pymupdf_layer.zip "$OLDPWD/"

# Cleanup
cd "$OLDPWD"
rm -rf "$TEMP_DIR"

echo "PyMuPDF layer built successfully: pymupdf_layer.zip"
echo "File size: $(ls -lh pymupdf_layer.zip | awk '{print $5}')"
echo ""
echo "You can now run 'terraform apply' to deploy the infrastructure."