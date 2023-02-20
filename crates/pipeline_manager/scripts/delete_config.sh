#!/bin/bash

# Delete an existing project config.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <config_id>'"
    exit 1
fi

curl -X DELETE http://localhost:8080/configs/$1  -H 'Content-Type: application/json' -d '{}'
