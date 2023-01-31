#!/bin/bash

# Update project.

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage '$0 <project_id> <project_name> < <script.sql>'"
    exit 1
fi

CODE=$(</dev/stdin)

json_escape () {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

ESCAPED_CODE="$(json_escape "$CODE")"

# echo $ESCAPED_CODE

curl -X PATCH http://"${DBSP_MANAGER:-localhost:8080}"/projects -H 'Content-Type: application/json' -d '{"project_id":'$1',"name":"'$2'","code":'"${ESCAPED_CODE}"'}'
