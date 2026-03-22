#!/usr/bin/env bash
# Build the custom ignite-rs test Docker image.
# Run from any directory; the script resolves paths relative to itself.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESOURCES_DIR="$(dirname "$SCRIPT_DIR")"

IMAGE_NAME="${IGNITE_TEST_IMAGE:-ignite-rs-test}"
IMAGE_TAG="${IGNITE_TEST_TAG:-latest}"

echo "Building ${IMAGE_NAME}:${IMAGE_TAG} ..."
docker build \
    -t "${IMAGE_NAME}:${IMAGE_TAG}" \
    -f "${SCRIPT_DIR}/Dockerfile" \
    "${RESOURCES_DIR}"

echo "Done. Use IGNITE_TEST_IMAGE=${IMAGE_NAME} IGNITE_TEST_TAG=${IMAGE_TAG} to run tests with this image."
