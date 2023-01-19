#!/bin/bash

# Retrieve project status.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <project_id>'"
    exit 1
fi

# echo $ESCAPED_CODE

curl http://localhost:8080/project_status/$1
