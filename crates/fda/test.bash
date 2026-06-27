#!/bin/bash
set -ex

FDA_BINARY=${FDA_BINARY:-../../target/debug/fda}

fda() {
    ${FDA_BINARY} "$@"
}


fail_on_success() {
  # Run the command with `|| true` to prevent `set -e` from exiting the script
  set +e
  "$@"

  # Check the exit status
  if [ $? -eq 0 ]; then
    echo "Command succeeded but was expected to fail"
    exit 1
  fi
  set -e
}

compare_output() {
  # Capture the output of both commands
  output1=`$1`; status1=$?
  output2=`$2`; status2=$?

  # Compare the outputs
  if [ "$output1" != "$output2" ]; then
    echo "The outputs are different."
    exit 1
  fi
  if [ $status1 != $status2 ]; then
    echo "Status values differ: $status1 != $status2"
    exit 1
  fi
  return 0
}

curl_opts=()
if [[ "${FELDERA_TLS_INSECURE:-}" == "1" || "${FELDERA_TLS_INSECURE:-}" == "true" ]]; then
  curl_opts+=(--insecure)
fi
EDITION=`curl "${curl_opts[@]}" -H "Authorization: Bearer ${FELDERA_API_KEY}" "${FELDERA_HOST%/}/v0/config" | jq .edition | sed s/\"//g`
echo "Edition: $EDITION"
case $EDITION in
    Enterprise) enterprise=true ;;
    "Open source") enterprise=false ;;
    *)
	echo "Unknown edition '$EDITION'" >&2
	exit 1
	;;
esac

fda pipelines

# Cleanup, these commands might fail if the resources do not exist yet
fda shutdown p1 || true
fda delete --force p1 || true
fda delete --force p2 || true
fda delete --force pbad || true
fda clear pudf && fda delete pudf || true
fda delete --force pclock || true
fda delete unknown || true
fda delete --force punknown || true
fda apikey delete a || true

# Tests
fda apikey create a
fda apikey list
fda apikey delete a

echo "base64 = '0.22.1'" > udf.toml
echo "use feldera_sqllib::F32;" > udf.rs
cat > program.sql <<EOF
CREATE TABLE example ( id INT NOT NULL PRIMARY KEY ) WITH ('materialized' = 'true', 'connectors' = '[{ "name": "c", "transport": { "name": "datagen", "config": { "plan": [{ "limit": 1 }] } } }]');
CREATE VIEW example_count WITH ('connectors' = '[{ "name": "c", "transport": { "name": "file_output", "config": { "path": "bla" } }, "format": { "name": "csv" } }]') AS ( SELECT COUNT(*) AS num_rows FROM example );
EOF

fda create p1 program.sql
fda copy p1 p2
compare_output "fda program get p1" "fda program get p2"
fda program set-config p1 --profile dev
fda program set-config p1 --profile optimized
fda program config p1
fda program status p1
fda events p1
fda events p1 status
fda events p1 all
fda event p1 latest
fda event p1 latest status
fda event p1 latest all

cat > bad-program.sql <<EOF
SELECT invalid
EOF
fda create pbad bad-program.sql
for _ in {1..60}; do
  if fda program status pbad | grep -q "SqlError"; then
    break
  fi
  sleep 1
done
fda program status pbad | grep "SqlError"
fda program errors pbad | grep -i "error:"
fda --format json program errors pbad | jq -e '.sql_compilation.messages | length > 0'
fda delete --force pbad

fda create pudf program.sql --udf-toml udf.toml --udf-rs udf.rs
compare_output "fda program get pudf --udf-toml" "cat udf.toml"
compare_output "fda program get pudf --udf-rs" "cat udf.rs"

fda set-config p1 storage true
fda program set p1 --udf-toml udf.toml --udf-rs udf.rs
fda program set p2 --udf-rs udf.rs
compare_output "fda program get p1" "fda program get p2"
compare_output "fda program get p1 --udf-rs" "fda program get p2 --udf-rs"

fda config p1

fda dismiss-error p1
fda start p1 --no-dismiss-error
fda start p1
fda --format json stats p1 | jq '.metrics'
fda log p1
fda logs p1
fda connector p1 example c stats
fda connector p1 example_count c stats
fda connector p1 example unknown stats || true
# Connectors
fda connector p1 example c pause
fda connector p1 example_count c pause || true
fda connector p1 example c start
fda connector p1 example unknown start || true

# Adhoc queries
fda query p1 "SELECT * FROM example"
# Arrow IPC is the recommended format; verify it produces output and the
# text-mode pretty-printer downstream of the parser handles it.
fda --format arrow_ipc query p1 "SELECT * FROM example"

# Runtime errors during query execution must surface as a non-zero exit
# code in WebSocket mode, otherwise scripts have no way to detect a
# failure.
fail_on_success fda query p1 "SELECT 1/0"
fail_on_success fda --format arrow_ipc query p1 "SELECT 1/0"
fail_on_success fda --format json query p1 "SELECT 1/0"

# Intermediate `SELECT`s still error today; we plan to support
# `select; select; ...` once a streaming protocol can frame multiple
# result sets back to the caller.
fail_on_success fda query p1 "SELECT 1; SELECT 2"

# Transaction tests
echo "Testing transaction commands..."
fail_on_success fda commit-transaction p1
fda start-transaction p1
fail_on_success fda commit-transaction p1 --tid 999
fda commit-transaction p1
fda start-transaction p1
fda commit-transaction p1 --timeout 10
fda start-transaction p1
fda commit-transaction p1 --no-wait

fda shutdown p1

# Clock advance smoke test (testing knob).  Uses a dedicated pipeline
# `pclock` whose SQL references NOW(); the clock connector is only
# registered when the program contains a NOW() stream.
echo "Testing clock advance..."
fda shutdown pclock || true
fda delete --force pclock || true
cat > clock.sql <<EOF
CREATE MATERIALIZED VIEW v AS SELECT NOW() AS t;
EOF
fda create pclock clock.sql

# Without `now_http_driven` the pipeline rejects with 400.
fda start pclock
sleep 2
fail_on_success fda clock-advance pclock
fda shutdown pclock

# Reconfigure for http-driven mode anchored at a fixed RFC 3339 instant.
fda set-config pclock dev_tweaks '{"now_http_driven": true, "now_offset": "2030-01-01T00:00:00Z"}'
fda start pclock
sleep 2

# delta_ms = 0 is a read; pin the anchor (2030-01-01 == 1893456000000 ms).
fda --format json clock-advance pclock --delta-ms 0 | jq -e '.now_ms == 1893456000000'
# Explicit forward advance.
fda clock-advance pclock --delta-ms 60000
# Omitting --delta-ms advances by one clock_resolution.
fda clock-advance pclock
# Aliases resolve to the same command.
fda set-clock pclock --delta-ms 0
fda clock-set pclock --delta-ms 0
# Negative delta is rejected by clap (`u64`).
fail_on_success fda clock-advance pclock --delta-ms -1

fda shutdown pclock
fda clear pclock
fda delete pclock
rm clock.sql

if $enterprise; then
    enterprise_only=
else
    enterprise_only=fail_on_success
fi
fda clear p1
$enterprise_only fda set-config p1 fault_tolerance true
fda set-config p1 fault_tolerance false
fda set-config p1 fault_tolerance none
$enterprise_only fda set-config p1 fault_tolerance at_least_once
$enterprise_only fda set-config p1 fault_tolerance exactly_once
fail_on_success fda set-config p1 fault_tolerance exactly_one
fail_on_success fda program set-config p1 -p optimized --runtime-version invalid-version

# Test dev_tweaks setting
fda set-config p1 dev_tweaks '{"x": 2}'
fail_on_success fda set-config p1 dev_tweaks 'invalid-json'

fda delete p1
fda delete p2
fda delete pudf
fda shell unknown || true

# Test support bundle with all collections skipped
echo "Testing support bundle with all collections skipped..."
fda create p1 program.sql
fda start p1
sleep 2  # Give pipeline time to start

# Test support bundle
echo "Testing support bundle with all collections enabled..."
fda support-bundle p1 -o test-support-bundle-full.zip
# Verify the ZIP file exists
if [ ! -f test-support-bundle-full.zip ]; then
    echo "Support bundle file was not created"
    exit 1
fi
# Count files in the ZIP and verify we have multiple files (manifest + data files)
# this counts one more than what is in the archive
file_count=$(unzip -l test-support-bundle-full.zip | grep -E "^\s*[0-9]+" | wc -l)
if [ "$file_count" -lt 9 ]; then
    echo "Expected at least 8 files in support bundle (manifest.txt + data files +/- the heap profile which doesnt work on macos), found $file_count"
    unzip -l test-support-bundle-full.zip
    exit 1
fi

# Create support bundle with all collections skipped
fda support-bundle p1 \
  --no-circuit-profile \
  --no-heap-profile \
  --no-metrics \
  --no-logs \
  --no-stats \
  --no-pipeline-config \
  --no-system-config \
  --no-dataflow-graph \
  --no-pipeline-events \
  -o test-support-bundle-none.zip

# Verify the ZIP file exists and check its contents
if [ ! -f test-support-bundle-none.zip ]; then
    echo "Support bundle file was not created"
    exit 1
fi
# With every collection disabled only metadata.txt and metadata.json remain.
# grep matches each file row plus the trailing totals line, so 2 files => 3.
file_count=$(unzip -l test-support-bundle-none.zip | grep -E "^\s*[0-9]+" | wc -l)
if [ "$file_count" -ne 3 ]; then
    echo "Expected 2 files in support bundle (metadata.txt, metadata.json), found $file_count"
    unzip -l test-support-bundle-none.zip
    exit 1
fi

fda shutdown p1

# Verify argument conflicts, these invocations should fail
fail_on_success fda create set punknown --stdin
fail_on_success fda program set punknown file-path --stdin
fail_on_success fda program set p1 --udf-toml udf.toml --stdin
fail_on_success fda program set p1 --udf-rs udf.toml --stdin
fail_on_success fda program set p1 --udf-rs udf.toml --udf-toml udf.toml --stdin
# --tls-cert and --insecure are mutually exclusive: supplying both must fail
# before any request is attempted.
fail_on_success fda --insecure --tls-cert /tmp/does-not-matter.pem pipelines
fail_on_success fda -k --tls-cert /tmp/does-not-matter.pem pipelines

rm program.sql
rm bad-program.sql
rm udf.toml
rm udf.rs
rm -f test-support-bundle-full.zip
rm -f test-support-bundle-none.zip

# Cluster events
fda cluster events
fda cluster event latest
fda cluster event latest status
fda cluster event latest all
