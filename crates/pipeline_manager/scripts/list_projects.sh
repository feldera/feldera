#!/bin/bash

# List projects in the database.

set -e

if [ "$#" -ne 0 ]; then
    echo "Usage '$0'"
    exit 1
fi

# echo $ESCAPED_CODE

curl http://"${DBSP_MANAGER:-localhost:8080}"/projects
