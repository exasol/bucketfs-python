#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
pushd  "$SCRIPT_DIR" &> /dev/null
if [ -d "dist"]; then
  trap 'rm "dist"/*' EXIT
  rm "dist"/* || true
fi
poetry build


RELEASE_BUILD_STEP_DIST_DIRECTORY="./../language_container/exasol-bucketfs-utils-python-container/flavor_base/release/dist"
echo "Copy" ./../dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"
mkdir "$RELEASE_BUILD_STEP_DIST_DIRECTORY" || true
cp ./../dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"
trap 'rm -rf "$RELEASE_BUILD_STEP_DIST_DIRECTORY" "../dist"' EXIT

echo "Build container"
./../language_container/exaslct export --flavor-path ./../language_container/exasol-bucketfs-utils-python-container/  --output-directory ../.build_output

echo "Generate language activation"
./../language_container/exaslct generate-language-activation --flavor-path ./../language_container/exasol-bucketfs-utils-python-container/ --bucketfs-name bfsdefault --bucket-name default --path-in-bucket container --container-name ml
