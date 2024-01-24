#!/bin/bash

set -e

pushd temp; cargo update; popd

mvn clean
mvn -DskipTests package
mvn test
echo "Running sqllogictest tests"
java -jar ./slt/target/slt-jar-with-dependencies.jar -inc -v -e hybrid

