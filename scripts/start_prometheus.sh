#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [ "$#" -lt 1 ]; then
    echo "Usage '$0 <working_directory_path> <bind address (optional)>'"
    exit 1
fi

WORKING_DIR=$(realpath "${1}")

# Kill prometheus.
pkill -9 prometheus

set -e

prometheus_config="global:
  scrape_interval: 5s

scrape_configs:
  - job_name: dbsp
    file_sd_configs:
    - files:
      - '${WORKING_DIR}/prometheus/pipeline*.yaml'"

printf "$prometheus_config" > "${WORKING_DIR}/prometheus.yaml"

prometheus --config.file "${WORKING_DIR}/prometheus.yaml" 2> "${WORKING_DIR}/prometheus.log" &
