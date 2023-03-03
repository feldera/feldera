#!/bin/bash

# List project pipelines.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <project_id>'"
    exit 1
fi

curl http://"${DBSP_MANAGER:-localhost:8080}"/projects/$1/pipelines
