#!/bin/bash

# Create a new connector.

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage '$0 <connector_name> < <connector_config.yaml>'"
    exit 1
fi

CONFIG=$(</dev/stdin)

json_escape () {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

ESCAPED_CONFIG="$(json_escape "$CONFIG")"

# echo $ESCAPED_CONFIG

curl -s -X POST http://"${DBSP_MANAGER:-localhost:8080}"/configs  -H 'Content-Type: application/json' -d '{"name":"'$1'","config":'"${ESCAPED_CONFIG}"'}'
