#!/bin/bash

cd "$(dirname "$0")"
OUTPUT="../../flink_results.csv"

TESTS=( "q0" "q1" "q2" "q3" "q4" "q5" "q7" "q8" "q9" "q11" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q20" "q21" "q22" )
for TEST in "${TESTS[@]}"
  do 
  docker compose -f docker-compose.yml -p nexmark up --build --force-recreate --renew-anon-volumes -d
  ./get-memory.bash &
  docker exec -i nexmark-jobmanager-1 run.sh --queries $TEST 2>&1 | tee flink_log.txt
  LASTLINE=$(../run-nexmark.sh --runner flink --parse < flink_log.txt)
  docker compose -f docker-compose.yml -p nexmark down
  kill %% 
  MAXMEM=$(cat maxmem)
  echo "$LASTLINE,$MAXMEM" >> $OUTPUT
done