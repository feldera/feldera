#!/bin/bash

# Create a new project.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <project_name> < <script.sql>'"
    exit 1
fi

CODE=$(</dev/stdin)

json_escape () {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

ESCAPED_CODE="$(json_escape "$CODE")"

# echo $ESCAPED_CODE

curl -s -X POST http://localhost:8080/new_project  -H 'Content-Type: application/json' -d '{"name":"'$1'","code":'"${ESCAPED_CODE}"'}'
