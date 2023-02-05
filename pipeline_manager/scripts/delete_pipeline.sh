#!/bin/bash

# Stop and delete an existing pipeline.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <pipeline_id>'"
    exit 1
fi

curl -X DELETE http://localhost:8080/pipelines/$1 -H 'Content-Type: application/json' -d '{}'
