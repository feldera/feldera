#!/bin/bash

set -e

pushd ../temp; cargo update; popd

# mvn test
echo "Running sqllogictest tests"
mvn compile exec:java -Dexec.mainClass="org.dbsp.sqllogictest.Main" -Dexec.args="-inc -v -e hybrid"
