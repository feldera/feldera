#!/bin/bash

# Update project config.

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage '$0 <config_id> <config_name> < <config.yaml>'"
    exit 1
fi

CONFIG=$(</dev/stdin)

json_escape () {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

ESCAPED_CONFIG="$(json_escape "$CONFIG")"

# echo $ESCAPED_CONFIG

curl -X PATCH http://localhost:8080/configs -H 'Content-Type: application/json' -d '{"config_id":'$1',"name":"'$2'","config":'"${ESCAPED_CONFIG}"'}'
