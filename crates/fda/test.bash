#!/bin/bash
set -ex

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
  output1=`$1`
  output2=`$2`

  # Compare the outputs
  if [ "$output1" = "$output2" ]; then
    return 0
  else
    echo "The outputs are different."
    exit 1
  fi
}

#export FELDERA_HOST=http://localhost:8080
BINARY_PATH=../../target/debug/fda

cargo build

$BINARY_PATH pipelines

# Cleanup, these commands might fail if the resources do not exist yet
$BINARY_PATH shutdown p1 || true
$BINARY_PATH delete p1 || true
$BINARY_PATH delete p2 || true
$BINARY_PATH delete pudf || true
$BINARY_PATH delete unknown || true
$BINARY_PATH delete punknown || true
$BINARY_PATH apikey delete a || true

# Tests
$BINARY_PATH apikey create a
$BINARY_PATH apikey list
$BINARY_PATH apikey delete a

echo "base64 = '0.22.1'" > udf.toml
echo "use feldera_sqllib::F32;" > udf.rs
cat > program.sql <<EOF
CREATE TABLE example ( id INT NOT NULL PRIMARY KEY ) WITH ('connectors' = '[{ "name": "c", "transport": { "name": "datagen", "config": { "plan": [{ "limit": 1 }] } } }]');
CREATE VIEW example_count WITH ('connectors' = '[{ "name": "c", "transport": { "name": "file_output", "config": { "path": "bla" } }, "format": { "name": "csv" } }]') AS ( SELECT COUNT(*) AS num_rows FROM example );
EOF

$BINARY_PATH create p1 program.sql
$BINARY_PATH program get p1 | $BINARY_PATH create p2 -s
compare_output "${BINARY_PATH} program get p1" "${BINARY_PATH} program get p2"
$BINARY_PATH program set-config p1 dev
$BINARY_PATH program config p1
$BINARY_PATH program status p1

$BINARY_PATH create pudf program.sql --udf-toml udf.toml --udf-rs udf.rs
compare_output "${BINARY_PATH} program get pudf --udf-toml" "cat udf.toml"
compare_output "${BINARY_PATH} program get pudf --udf-rs" "cat udf.rs"

$BINARY_PATH set-config p1 storage true
$BINARY_PATH program set p1 --udf-toml udf.toml --udf-rs udf.rs
$BINARY_PATH program set p2 --udf-rs udf.rs
compare_output "${BINARY_PATH} program get p1" "${BINARY_PATH} program get p2"
compare_output "${BINARY_PATH} program get p1 --udf-rs" "${BINARY_PATH} program get p2 --udf-rs"

$BINARY_PATH config p1

$BINARY_PATH start p1
$BINARY_PATH --format json stats p1 | jq '.metrics'
$BINARY_PATH log p1
$BINARY_PATH logs p1
$BINARY_PATH connector p1 example c stats
$BINARY_PATH connector p1 example_count c stats
$BINARY_PATH connector p1 example unknown stats || true
$BINARY_PATH connector p1 unknown c stats || true
$BINARY_PATH connector unknown example c stats || true
$BINARY_PATH connector p1 example c pause
$BINARY_PATH connector p1 example_count c pause || true
$BINARY_PATH connector p1 example c start
$BINARY_PATH connector p1 example unknown start || true
$BINARY_PATH shutdown p1

$BINARY_PATH delete p1
$BINARY_PATH delete p2
$BINARY_PATH delete pudf
$BINARY_PATH shell unknown || true

# Verify argument conflicts, these invocations should fail
fail_on_success $BINARY_PATH create set punknown --stdin
fail_on_success $BINARY_PATH program set punknown file-path --stdin
fail_on_success $BINARY_PATH program set p1 --udf-toml udf.toml --stdin
fail_on_success $BINARY_PATH program set p1 --udf-rs udf.toml --stdin
fail_on_success $BINARY_PATH program set p1 --udf-rs udf.toml --udf-toml udf.toml --stdin

rm program.sql
rm udf.toml
rm udf.rs