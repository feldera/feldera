#!/bin/bash
#
# Usage (to test): $ SMOKE=true bash scripts/bench.bash
#
# - `SMOKE`: Run benchmarks quickly but not representative
#
set -ex

RESULTS_DIR=benchmark-run-data
NEXMARK_RESULTS_DIR=$RESULTS_DIR/nexmark
GALEN_RESULTS_DIR=$RESULTS_DIR/galen
LDBC_RESULTS_DIR=$RESULTS_DIR/ldbc

NEXMARK_CSV_FILE='nexmark_results.csv'
NEXMARK_DRAM_CSV_FILE='dram_nexmark_results.csv'
NEXMARK_PERSISTENCE_CSV_FILE='persistence_nexmark_results.csv'
GALEN_CSV_FILE='galen_results.csv'
LDBC_CSV_FILE='ldbc_results.csv'
rm -f crates/dbsp/${GALEN_CSV_FILE} crates/dbsp/${LDBC_CSV_FILE} crates/nexmark/${NEXMARK_DRAM_CSV_FILE}
rm -rf ${RESULTS_DIR}
mkdir -p ${RESULTS_DIR}

# Run nexmark benchmark
MAX_EVENTS=100000000
if [ "$SMOKE" != "" ]; then
  MAX_EVENTS=1000000
fi

if [ "$CLOUD" = "" ]; then
  GENERATORS=8
  CORES=6
  FILES=( "q0" "q1" "q2" "q3" "q4" "q5" "q6" "q7" "q8" "q9" "q12" "q13" "q14" "q15" "q16" "q17" "q18" "q19" "q20" "q21" "q22" )
  for FILE in "${FILES[@]}"
    do cargo bench --bench nexmark -- --max-events=${MAX_EVENTS} --cpu-cores ${CORES}  --num-event-generators ${GENERATORS} --source-buffer-size 10000 --input-batch-size 40000 --csv ${NEXMARK_CSV_FILE} --query $FILE
  done
  mkdir -p ${NEXMARK_RESULTS_DIR}
  mv crates/nexmark/${NEXMARK_CSV_FILE} $NEXMARK_RESULTS_DIR
fi

# Run SQL benchmarks
# These require a running instance of redpanda (if they don't use nexmark connector) and pipeline-manager.

KAFKA_BROKER=localhost:9092
FELDERA_API=http://localhost:8080

if [ "$CLOUD" != "" ]; then
  FELDERA_API=$API_URL
  KAFKA_BROKER='${secret:demo-bootstrap-servers}'
  CLOUD_OPTIONS='-O security.protocol=${secret:demo-security-protocol}
      -O ssl.ca.pem=${secret:demo-ssl-ca-pem}
      -O ssl.certificate.pem=${secret:demo-ssl-certificate-pem}
      -O ssl.key.pem=${secret:demo-ssl-key-pem}
      -O ssl.key.password=${secret:demo-ssl-key-password}
      -O ssl.endpoint.identification.algorithm=${secret:demo-ssl-endpoint-identification-algorithm}
      -O sasl.mechanism=${secret:demo-sasl-mechanism}
      -O sasl.username=${secret:demo-sasl-username}
      -O sasl.password=${secret:demo-sasl-password}
      --api-key '${API_KEY}
fi

sql_benchmark() {
    mkdir -p $RESULTS_DIR/$name
    local csv=$1 metrics=$2; shift; shift
    python3 benchmark/feldera-sql/run.py \
      --api-url $FELDERA_API \
      --events $MAX_EVENTS \
      -O bootstrap.servers=$KAFKA_BROKER \
      --csv "$RESULTS_DIR/$name/$csv" \
      --csv-metrics "$RESULTS_DIR/$name/$metrics" \
      --metrics-interval 1 \
      --poller-threads 10 \
      "$@"
}

DIR="benchmark/feldera-sql/benchmarks/"
if [[ -z "$CLOUD" ]]; then
    TESTS="nexmark"
else
    TESTS="nexmark"
fi

for test in ${TESTS}; do
  if [[ -e ${test}/generate.bash ]]; then
      rpk topic -X brokers=$KAFKA_BROKER delete -r '.*'
      ${test}/generate.bash
  fi
  name=$(basename $test)
  sql_benchmark "sql_${name}_results.csv" "sql_${name}_metrics.csv" --folder benchmarks/${name}
  # We are currently skipping running storage benchmarks on cloud until we can get a
  # better disk for cloud.
  if [ "$CLOUD" = "" ]; then
    sql_benchmark "sql_storage_${name}_results.csv" "sql_storage_${name}_metrics.csv" --storage --folder benchmarks/${name}
  fi
done

if [ "$CLOUD" = "" ]; then
  # Run galen benchmark
  cargo bench --bench galen -- --workers 10 --csv ${GALEN_CSV_FILE}
  mkdir -p ${GALEN_RESULTS_DIR}
  mv crates/dbsp/${GALEN_CSV_FILE} ${GALEN_RESULTS_DIR}

  # Run ldbc benchmarks
  DATASET_SMALL='graph500-22'
  DATASET_MEDIUM='datagen-8_4-fb'
  if [ "$SMOKE" != "" ]; then
      DATASET_SMALL='wiki-Talk'
      DATASET_MEDIUM='kgs'
  fi

  # Run nexmark benchmark with persistence
  MAX_EVENTS=3000000
  CORES=1
  if [ "$SMOKE" != "" ]; then
    MAX_EVENTS=100000
  fi
  cargo bench --bench nexmark -- --max-events=${MAX_EVENTS} --cpu-cores ${CORES} --num-event-generators 6 --source-buffer-size 10000 --input-batch-size 40000 --csv ${NEXMARK_DRAM_CSV_FILE}
  mv crates/nexmark/${NEXMARK_DRAM_CSV_FILE} $NEXMARK_RESULTS_DIR
fi
