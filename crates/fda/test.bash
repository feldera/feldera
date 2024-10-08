#export FELDERA_HOST=http://localhost:8080
BINARY_PATH=../../target/debug/fda

cargo build

$BINARY_PATH pipelines

# Cleanup
$BINARY_PATH delete p1
$BINARY_PATH delete p2
$BINARY_PATH delete unknown
$BINARY_PATH delete punknown
$BINARY_PATH apikey delete a

# Tests
$BINARY_PATH apikey create a
$BINARY_PATH apikey list
$BINARY_PATH apikey delete a

echo "CREATE TABLE example ( id INT NOT NULL PRIMARY KEY );
CREATE VIEW example_count AS ( SELECT COUNT(*) AS num_rows FROM example );" > program.sql
$BINARY_PATH create p1 program.sql
$BINARY_PATH program p1 | $BINARY_PATH create p2 -s
$BINARY_PATH program set-config p1 dev
$BINARY_PATH program config p1
$BINARY_PATH program status p1

$BINARY_PATH set-config p1 storage true
$BINARY_PATH config p1

$BINARY_PATH start p1
$BINARY_PATH stats p1 | jq '.metrics'
$BINARY_PATH log p1
$BINARY_PATH logs p1
$BINARY_PATH shutdown p1

$BINARY_PATH delete p1
$BINARY_PATH delete p2
$BINARY_PATH shell unknown || true

$BINARY_PATH create set punknown --stdin || true # error: the argument '[PROGRAM_PATH]' cannot be used with '--stdin'
$BINARY_PATH program set punknown file-path --stdin || true # error: the argument '[PROGRAM_PATH]' cannot be used with '--stdin'

rm program.sql
