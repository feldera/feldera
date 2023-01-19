#!/bin/bash

# Delete an existing project config.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <config_id>'"
    exit 1
fi

curl -X POST http://localhost:8080/delete_config  -H 'Content-Type: application/json' -d '{"config_id":'$1'}'
