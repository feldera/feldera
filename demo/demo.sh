#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="${THIS_DIR}/../"
SERVER_DIR="${ROOT}/crates/pipeline_manager/"

# docker kill redpanda
# docker rm redpanda
# docker run --name redpanda -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v23.2.3 || { echo 'Failed to start RedPanda container' ; exit 1; }

docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-debezium.yml up redpanda connect mysql -d --force-recreate -V

export REDPANDA_BROKERS=localhost:19092

# Kill the entire process group (including processes spawned by prepare_demo_data.sh) on shutdown.
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

(
    # wait for the manager
    while true; do
        if curl --output /dev/null --silent --head --fail http://localhost:8080; then
            echo "Pipeline manager is up and running"
            break
        else
            sleep 1
        fi
    done
    DBSP_MANAGER="http://localhost:8080" ${THIS_DIR}/create_demo_projects.sh
    DBSP_MANAGER="http://localhost:8080" ${THIS_DIR}/prepare_demo_data.sh
) &

prepare_subshell_pid=$!

${ROOT}/scripts/start_manager.sh ${THIS_DIR}/pipeline_data

# kill -9 $prepare_subshell_pid
