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

fda() {
    ../../target/debug/fda "$@"
}
#export FELDERA_HOST=http://localhost:8080

cargo build

EDITION=`curl "${FELDERA_HOST%/}/v0/config" | jq .edition | sed s/\"//g`
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
fda delete p1 || true
fda delete p2 || true
fda delete pudf || true
fda delete unknown || true
fda delete punknown || true
fda apikey delete a || true

# Tests
fda apikey create a
fda apikey list
fda apikey delete a

echo "base64 = '0.22.1'" > udf.toml
echo "use feldera_sqllib::F32;" > udf.rs
cat > program.sql <<EOF
CREATE TABLE example ( id INT NOT NULL PRIMARY KEY ) WITH ('connectors' = '[{ "name": "c", "transport": { "name": "datagen", "config": { "plan": [{ "limit": 1 }] } } }]');
CREATE VIEW example_count WITH ('connectors' = '[{ "name": "c", "transport": { "name": "file_output", "config": { "path": "bla" } }, "format": { "name": "csv" } }]') AS ( SELECT COUNT(*) AS num_rows FROM example );
EOF

fda create p1 program.sql
fda program get p1 | fda create p2 -s
compare_output "fda program get p1" "fda program get p2"
fda program set-config p1 dev
fda program config p1
fda program status p1

fda create pudf program.sql --udf-toml udf.toml --udf-rs udf.rs
compare_output "fda program get pudf --udf-toml" "cat udf.toml"
compare_output "fda program get pudf --udf-rs" "cat udf.rs"

fda set-config p1 storage true
fda program set p1 --udf-toml udf.toml --udf-rs udf.rs
fda program set p2 --udf-rs udf.rs
compare_output "fda program get p1" "fda program get p2"
compare_output "fda program get p1 --udf-rs" "fda program get p2 --udf-rs"

fda config p1

fda start p1
fda --format json stats p1 | jq '.metrics'
fda log p1
fda logs p1
fda connector p1 example c stats
fda connector p1 example_count c stats
fda connector p1 example unknown stats || true
fda connector p1 unknown c stats || true
fda connector unknown example c stats || true
fda connector p1 example c pause
fda connector p1 example_count c pause || true
fda connector p1 example c start
fda connector p1 example unknown start || true
fda shutdown p1

if $enterprise; then
    enterprise_only=
else
    enterprise_only=fail_on_success
fi
$enterprise_only fda set-config p1 fault_tolerance true
fda set-config p1 fault_tolerance false
fda set-config p1 fault_tolerance none
$enterprise_only fda set-config p1 fault_tolerance at_least_once
$enterprise_only fda set-config p1 fault_tolerance exactly_once
fail_on_success fda set-config p1 fault_tolerance exactly_one

fda delete p1
fda delete p2
fda delete pudf
fda shell unknown || true

# Verify argument conflicts, these invocations should fail
fail_on_success fda create set punknown --stdin
fail_on_success fda program set punknown file-path --stdin
fail_on_success fda program set p1 --udf-toml udf.toml --stdin
fail_on_success fda program set p1 --udf-rs udf.toml --stdin
fail_on_success fda program set p1 --udf-rs udf.toml --udf-toml udf.toml --stdin

rm program.sql
rm udf.toml
rm udf.rs
