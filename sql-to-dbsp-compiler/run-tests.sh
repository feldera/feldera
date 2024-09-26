#!/bin/bash

set -e

pushd temp; cargo update; popd

mvn clean
./build.sh
mvn test

