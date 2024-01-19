#!/bin/bash

# This script runs inside the demo container and brings up a subset of Feldera demos,
# currently the SecOps demo and the Supply Chain Tutorial demo.

# In CI mode, it passes an argument ($SECOPS_DEMO_ARGS) to the SecOps simulator to make
# sure that it exits after generating a fixed number of outputs.

# Start the two demos in parallel.  Compilation will still happen serially, but
# this way the user will see the two programs and pipelines straight away.
# Make sure the script returns an error if one of the demo scripts fails.

(cd demo/project_demo00-SecOps && python3 -u run.py --api-url http://pipeline-manager:8080 ${SECOPS_DEMO_ARGS})&
pid1=$!
echo pid1 $pid1
(cd demo/project_demo06-SupplyChainTutorial && python3 -u run.py --api-url http://pipeline-manager:8080)&
pid2=$!
wait $pid1
status1=$?
echo SecOps demo script exited with status $status1
wait $pid2
status2=$?
echo SupplyChainTutorial demo script exited with status $status2
if [ $status1 -ne 0 ]; then exit $status1; fi
if [ $status2 -ne 0 ]; then exit $status2; fi
