#!/bin/bash

set -e

pushd ../temp; cargo update; popd

if [ ! -d ../../sqllogictest ]; then
    echo "I expected that the SQL logic tests are installed in ../../"
    echo "You can do that using 'git clone https://github.com/gregrahn/sqllogictest.git'"
    exit 1
fi

mvn clean
mvn -DskipTests package
mvn test
echo "Running sqllogictest tests"
mvn compile exec:java -Dexec.mainClass="org.dbsp.sqllogictest.Main" -Dexec.args="-inc -i -s -e hybrid -d ../../sqllogictest/ ."
