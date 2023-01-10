#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="${THIS_DIR}/../"
SERVER_DIR="${ROOT}/pipeline_manager/"

# Kill previous server instance if any.
pkill -9 dbsp_pipeline_server

# Kill prometheus
pkill -9 prometheus

set -e

# We want non-matching wildcards to return an empty list
# instead of a non-expanded wildcard.
shopt -s nullglob

# Re-create DB
dropdb --if-exists -h localhost -U postgres dbsp
psql -h localhost -U postgres -f "${ROOT}/pipeline_manager/create_db.sql"

# Start server
cd "${SERVER_DIR}" && cargo build --release
cd "${SERVER_DIR}" && cargo run --release -- --static-html=static 2> "${THIS_DIR}/server.log"&

TIMEOUT=10

# Wait for the server to start listening on 8080.
while true; do
  # Use `nc` to check if the port is open
  if nc -z localhost 8080; then
    break
  fi

  sleep 1

  TIMEOUT=$((TIMEOUT - 1))

  if [ "$TIMEOUT" -eq 0 ]; then
    echo "Timed out waiting for the server"
    exit 1
  fi
done

printf "\nPipeline server is running\n\n"

project_prefix=${THIS_DIR}/project_

# for each demo directory
for project_dir in "${project_prefix}"*
do
    project_name=${project_dir#$project_prefix}

    printf "Creating project '${project_name}'... "
    project_id=$("${SERVER_DIR}/scripts/new_project.sh" "${project_name}" < "${project_dir}/project.sql" | jq '.project_id')
    printf "OK(project_id=$project_id)\n"

    for config_file in "${project_dir}/"*.yaml
    do
        config_name=$(basename "${config_file}" .yaml)
        printf "  Adding project config '${config_name}'... "
        config_id=$("${SERVER_DIR}/scripts/new_config.sh" "${project_id}" "${config_name}"  < "${config_file}" | jq '.config_id')
        printf "OK(config_id=$config_id)\n"
    done
done

for project_dir in "${project_prefix}"*
do
    project_name=${project_dir#$project_prefix}

    if test -f "${project_dir}/prepare.sh"; then
        echo "  Running custom project setup script"
        "${project_dir}"/prepare.sh
    fi
done
