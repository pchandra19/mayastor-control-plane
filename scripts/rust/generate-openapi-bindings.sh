#!/usr/bin/env bash

die()
{
  local _return="${2:-1}"
  echo "$1" >&2
  exit "${_return}"
}

write_version()
{
  write_version="yes"
  if [ -f "$VERSION_FILE" ] && [ "$(which paperclip-ng)" = "$(cat "$VERSION_FILE")" ]; then
    write_version=
  fi

  if [ -n "$write_version" ]; then
    which paperclip-ng > "$VERSION_FILE"
  fi
}

set -e

SCRIPTDIR=$(dirname "$0")
ROOTDIR="$SCRIPTDIR/../.."
TARGET="$ROOTDIR/openapi"
REAL_TARGET="$(realpath "$TARGET")"
VERSION_FILE="$TARGET/version.txt"
RUST_FMT="$ROOTDIR/.rustfmt.toml"
CARGO_TOML="$TARGET/Cargo.toml"
CARGO_LOCK="$TARGET/../Cargo.lock"
SPEC="$ROOTDIR/control-plane/rest/openapi-specs/v0_api_spec.yaml"
REAL_SPEC="$(realpath "$SPEC")"
GIT_FAILED="false"

# Regenerate the bindings only if the rest src changed
check_spec="no"
# Use the Cargo.toml from the openapi-generator
default_toml="no"
# skip git diff at the end
skip_git_diff="no"
# overwrite files only if the md5 changes
skip_if_md5_same="no"

while [ "$#" -gt 0 ]; do
  _arg="$1"
  case "$_arg" in
    --spec-changes)
        check_spec="yes"
        ;;
    --skip-git-diff)
        skip_git_diff="yes"
        ;;
    --default-toml)
        default_toml="yes"
        ;;
    --skip-md5-same)
        skip_if_md5_same="yes"
        ;;
    --if-rev-changed)
        if [[ -f "$VERSION_FILE" ]]; then
          version=$(cat "$VERSION_FILE")
          bin_version=$(which paperclip-ng)
          [[ "$version" = "$bin_version" ]] && exit 0
        fi
        skip_git_diff="yes"
        ;;
    --root-dir)
        test $# -lt 2 && die "Missing value for the optional argument '$_arg'."
        ROOTDIR="$2"
        shift
        ;;
    --root-dir=*)
        ROOTDIR="${_arg#*=}"
        ;;
    --target-dir)
        test $# -lt 2 && die "Missing value for the optional argument '$_arg'."
        TARGET="$2"
        shift
        ;;
    --target-dir=*)
        TARGET="${_arg#*=}"
        ;;
    --spec-file)
        test $# -lt 2 && die "Missing value for the optional argument '$_arg'."
        SPEC="$2"
        shift
        ;;
    --spec-file=*)
        SPEC="${_arg#*=}"
        ;;
  esac
  shift
done

if [[ $check_spec = "yes" ]]; then
  set +e;
  ( cd "$ROOTDIR"; git diff --exit-code "$REAL_SPEC" 1>/dev/null )
  if [ ${PIPESTATUS} == "0" ]; then
    exit 0
  fi
  set -e;
fi

tmpd=$(mktemp -d /tmp/openapi-gen-XXXXXXX)

# Generate a new openapi crate
RUST_LOG=debug paperclip-ng --spec "$SPEC" -o "$tmpd" --templates "$TARGET"/templates
( cd "$tmpd"; rm -rf api; rm -rf examples; rm -rf .* 2>/dev/null || true )

if [[ $default_toml = "no" ]]; then
  cp "$CARGO_TOML" "$tmpd"
fi

# Format the files
# Note, must be formatted on the tmp directory as we've ignored the autogenerated code within the workspace
if [ -f "$RUST_FMT" ]; then
  cp "$RUST_FMT" "$tmpd"
  cp "$CARGO_LOCK" "$tmpd"
  ( cd "$tmpd" && cargo fmt --all || true )
  # Cargo.lock is no longer generated when running cargo fmt
  ( cd "$tmpd"; rm Cargo.lock || true; rm "$(basename "$RUST_FMT")" )
fi

write_version

# lib_.rs include is not present on the generated code yet, so we need to massage it
( cd "$tmpd"; mv src/lib.rs src/lib_.rs; )

if [[ "$skip_if_md5_same" = "yes" ]]; then
  source_md5sum=$(cd "$tmpd"; find . -type f \( ! -name "build.rs" \) -exec md5sum {} \; | md5sum)
  target_md5sum=$(cd "$TARGET"; find . -type f \( ! -name "build.rs" ! -name "lib.rs" ! -name "version.txt" \) -exec md5sum {} \; | md5sum)

  [[ "$target_md5sum" = "$source_md5sum" ]] && exit 0
fi

# Cleanup the existing autogenerated code
cd "$ROOTDIR";
git clean -f -e "!version.txt" -X "$REAL_TARGET" || GIT_FAILED="true"
cd - >/dev/null

if [ ! -f "$TARGET/src/lib.rs" ]; then
  git restore "$TARGET/src/lib.rs" || die "Missing openapi/src/lib.rs"
fi

mv "$TARGET/src/lib.rs" "$tmpd/src/lib.rs"
if [ "$GIT_FAILED" = "true" ]; then
  for dir in $(find "$TARGET" -maxdepth 1 -mindepth 1 -type d); do
    rm -r $dir
  done
fi
mv "$tmpd"/* "$TARGET"/
rm -rf "$tmpd"

# If the openapi bindings were modified then fail the check
if [[ "$skip_git_diff" = "no" ]]; then
  ( cd "$ROOTDIR"; git diff --exit-code "$REAL_TARGET" )
fi
