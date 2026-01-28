#!/usr/bin/env bash
set -euo pipefail

# Prerequisite checks
if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required but not installed or not in PATH." >&2
    exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker is required but not installed or not in PATH." >&2
    exit 2
fi
if ! docker buildx version >/dev/null 2>&1; then
    echo "Error: docker buildx is required but not available. Ensure Docker Buildx is installed and working." >&2
    exit 2
fi

IMAGE_NAME="${1:?Error: IMAGE_NAME required}"
IMAGE_TAG="${2:?Error: IMAGE_TAG required}"
ARCHES="${3:-amd64,arm64}"
TIMEOUT="${4:-300}"
DELAY="${5:-15}"

# If IMAGE_NAME contains a slash, treat as fully-qualified; else, prefix with default registry/namespace
REGISTRY="ghcr.io"
NAMESPACE="feldera"
if [[ "$IMAGE_NAME" == */* ]]; then
    FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
else
    FULL_IMAGE="${REGISTRY}/${NAMESPACE}/${IMAGE_NAME}:${IMAGE_TAG}"
fi

IFS=',' read -ra ARCH_ARRAY <<< "$ARCHES"
echo "Waiting for arches ${ARCH_ARRAY[*]} in ${FULL_IMAGE}..."

start_time=$(date +%s)

while [ $(( $(date +%s) - start_time )) -lt "$TIMEOUT" ]; do
    all_ready=true

    if RESULT=$(docker buildx imagetools inspect "$FULL_IMAGE" --format '{{json .}}'); then
        if [ -z "$RESULT" ]; then
                echo "❌ Image not found: $FULL_IMAGE (empty)"
            sleep $DELAY
            continue
        fi
    else
            echo "❌ Image not found: $FULL_IMAGE (inspect failed)"
        sleep $DELAY
        continue
    fi

    ARCHES_READY=true
    for ARCH in "${ARCH_ARRAY[@]}"; do
        if echo "$RESULT" | jq -e --arg arch "$ARCH" \
            '.manifest.manifests[]? | select(.platform.architecture == $arch and .platform.os == "linux")'; then
            echo "  ✅ ${IMAGE_NAME} $ARCH"
        else
            echo "  ❌ Missing ${IMAGE_NAME} $ARCH"
            ARCHES_READY=false
        fi
    done

    [ "$ARCHES_READY" = true ] && echo "✅ ${IMAGE_NAME}" || { echo "⏳ ${IMAGE_NAME}"; all_ready=false; }
    echo ""

    [ "$all_ready" = true ] && echo "🎉 All ready!" && exit 0
    sleep $DELAY
done

echo "💥 Timeout after ${TIMEOUT}s" && exit 1
