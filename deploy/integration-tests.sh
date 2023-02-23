#!/bin/bash

set -e

host=$1

curl -X 'POST' \
  "http://${host}/projects" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "code": "CREATE TABLE Example(name varchar);",
  "description": "Example description",
  "name": "Example project"
}'
