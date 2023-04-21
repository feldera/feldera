#!/bin/bash

set -e 
docker compose -f docker-compose-dev.yml up -d
sleep 20
docker run --network `docker inspect dbsp -f "{{json .NetworkSettings.Networks }}" | jq -r 'keys[0]'` --name test --rm -it dbspmanager-dev bash -c "python3 python/test.py http://dbsp:8080 http"
