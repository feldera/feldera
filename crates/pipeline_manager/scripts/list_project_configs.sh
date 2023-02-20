#!/bin/bash

# List project configs.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <project_id>'"
    exit 1
fi

curl http://localhost:8080/projects/$1/configs
