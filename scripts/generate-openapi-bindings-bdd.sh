#!/usr/bin/env bash

set -e

ROOT_DIR="$(dirname "$0")/.."
TARGET="$ROOT_DIR/tests/bdd/openapi"
SPEC="$ROOT_DIR/control-plane/rest/openapi-specs/v0_api_spec.yaml"

# Cleanup the existing autogenerated code
if [ -d "$TARGET" ]; then
  rm -rf "$TARGET"
fi
mkdir -p "$TARGET"

tmpd=$(mktemp -d /tmp/openapi-gen-bdd-XXXXXXX)

# Generate a new openapi python client for use by the BDD tests
openapi-generator-cli generate -i "$SPEC" -g python -o "$tmpd" --additional-properties packageName="openapi"

mv "$tmpd"/openapi/* "$TARGET"
rm -rf "$tmpd"
