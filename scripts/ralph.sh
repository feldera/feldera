#!/usr/bin/env bash
#
# Ralph Wiggum — long-running Copilot agent loop (bash port)
#
# Usage: scripts/ralph.sh <prompt.md> [-n 10] [--skip-to "Step 4"]
#
set -euo pipefail

usage() {
    echo "Usage: $0 <prompt.md> [-n iterations] [--skip-to instruction]"
    echo
    echo "Ralph Wiggum — re-invokes Copilot in a loop until the task completes."
    echo
    echo "Options:"
    echo "  -n, --iterations N     Max iterations (default: 30)"
    echo "  --skip-to TEXT         Prepend skip instruction to the prompt"
    echo "  -h, --help             Show this help"
    exit 1
}

PROMPT_FILE=""
MAX_ITER=30
SKIP_TO=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--iterations) MAX_ITER="$2"; shift 2 ;;
        --skip-to)       SKIP_TO="$2"; shift 2 ;;
        -h|--help)       usage ;;
        -*)              echo "Unknown flag: $1"; usage ;;
        *)               PROMPT_FILE="$1"; shift ;;
    esac
done

[[ -z "$PROMPT_FILE" ]] && { echo "Error: No prompt file specified."; usage; }
[[ -f "$PROMPT_FILE" ]] || { echo "Error: Cannot read '$PROMPT_FILE'"; exit 1; }

PROMPT="$(cat "$PROMPT_FILE")"

if [[ -n "$SKIP_TO" ]]; then
    PROMPT="**INSTRUCTION: ${SKIP_TO}** — Skip earlier steps and begin from this point.

${PROMPT}"
fi

parse_signal() {
    # Search last 20 lines for { "status": "Succeeded"|"Failed" }
    local output="$1"
    echo "$output" | tail -20 | grep -oP '\{\s*"?status"?\s*:\s*"(Succeeded|Failed)"\s*\}' | tail -1 | grep -oP '(Succeeded|Failed)' || true
}

echo "Starting Ralph — Prompt: ${PROMPT_FILE} — Max iterations: ${MAX_ITER}"
[[ -n "$SKIP_TO" ]] && echo "Skip-to: ${SKIP_TO}"

for (( i=1; i<=MAX_ITER; i++ )); do
    echo
    echo "==============================================================="
    echo "  Ralph Iteration ${i} of ${MAX_ITER}"
    echo "==============================================================="

    OUTPUT_FILE="$(mktemp /tmp/ralph-output-XXXXXX.log)"

    # Run copilot with the prompt, capture output while streaming
    set +e
    copilot -p "$PROMPT" --yolo 2>&1 | tee "$OUTPUT_FILE"
    EXIT_CODE=$?
    set -e

    OUTPUT="$(cat "$OUTPUT_FILE")"
    rm -f "$OUTPUT_FILE"

    SIGNAL="$(parse_signal "$OUTPUT")"

    if [[ "$SIGNAL" == "Succeeded" ]]; then
        echo
        echo "==============================================================="
        echo "  Ralph completed successfully!"
        echo "  Completed at iteration ${i} of ${MAX_ITER}"
        echo "==============================================================="
        exit 0
    fi

    if [[ "$SIGNAL" == "Failed" ]]; then
        echo
        echo "==============================================================="
        echo "  Ralph reported failure."
        echo "  Failed at iteration ${i} of ${MAX_ITER}"
        echo "==============================================================="
        exit 1
    fi

    echo "Iteration ${i} complete — no completion signal found. Continuing..."
    sleep 2
done

echo
echo "Ralph reached max iterations (${MAX_ITER}) without completing."
exit 1
