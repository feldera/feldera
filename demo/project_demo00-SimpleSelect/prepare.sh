#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rpk topic delete null_demo_input
rpk topic delete null_demo_output

rpk topic create null_demo_input -c retention.ms=-1 -c retention.bytes=-1
rpk topic create null_demo_output

# Push test data to topic.
cat "${THIS_DIR}"/data.csv | rpk topic produce null_demo_input -f '%v'
