#!/bin/bash

# Stop an existing pipeline.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <pipeline_id>'"
    exit 1
fi

curl -X POST http://localhost:8080/pipelines/shutdown  -H 'Content-Type: application/json' -d '{"pipeline_id":'$1'}'
