#!/bin/bash

# Delete a project.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <project_id>'"
    exit 1
fi

curl -X POST http://localhost:8080/delete_project  -H 'Content-Type: application/json' -d '{"project_id":'$1'}'
