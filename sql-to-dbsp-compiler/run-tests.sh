#!/bin/bash

set -e

pushd temp; cargo update; popd

mvn clean
./build.sh
mvn test
echo "Running sqllogictest tests"
java -jar ./slt/target/slt-jar-with-dependencies.jar -inc -v -e hybrid

