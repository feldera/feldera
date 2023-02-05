#!/bin/bash

# Create a new pipeline.

set -ex

if [ "$#" -ne 4 ]; then
    echo "Usage '$0 <project_id> <project_version> <config_id> <config_version>'"
    exit 1
fi

response=$(curl -s -X POST http://localhost:8080/pipelines -H 'Content-Type: application/json' -d '{"project_id":'$1',"project_version":'$2',"config_id":'$3',"config_version":'$4'}')

port=$(echo ${response} | jq '.port')
id=$(echo ${response} | jq '.pipeline_id')

curl -s -X GET http://localhost:${port}/start

echo Started pipeline ${id} on port ${port}
