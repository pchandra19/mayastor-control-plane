#!/usr/bin/env bash

set -e

ROOT_DIR="$(dirname "$0")/../.."
TARGET="$ROOT_DIR/tests/bdd/openapi"
SPEC="$ROOT_DIR/control-plane/rest/openapi-specs/v0_api_spec.yaml"

# Cleanup the existing autogenerated code
if [ -d "$TARGET" ]; then
  rm -rf "$TARGET"
fi
mkdir -p "$TARGET"

tmpd=$(mktemp -d /tmp/openapi-gen-bdd-XXXXXXX)

# Work around bug: https://github.com/OpenAPITools/openapi-generator/issues/11763
# export _JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
# But only required for the new python generator, not for python-prior
# Generate a new openapi python client for use by the BDD tests
openapi-generator-cli generate -i "$SPEC" -g python-prior -o "$tmpd" --additional-properties packageName="openapi"

# Path AllOf bug on openapi-generator
cat <<EOF | patch "$tmpd/openapi/model_utils.py"
651c651
<                     if v not in values:
---
>                     if v not in values and str(v) not in list(map(lambda e: str(e), values)):
EOF

mv "$tmpd"/openapi/* "$TARGET"
rm -rf "$tmpd"
