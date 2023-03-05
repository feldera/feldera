#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

set -e

export REDPANDA_BROKERS=localhost:19092

DBSP_MANAGER="localhost:8085" ${THIS_DIR}/create_demo_projects.sh
