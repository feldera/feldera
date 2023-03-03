#!/bin/bash

# Compile project.

set -e

if [ "$#" -ne 2 ]; then
    echo "Usage '$0 <project_id> <project_version>'"
    exit 1
fi

curl -X POST http://"${DBSP_MANAGER:-localhost:8080}"/projects/compile  -H 'Content-Type: application/json' -d '{"project_id":'$1',"version":'$2'}'
