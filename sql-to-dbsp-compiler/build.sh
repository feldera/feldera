#!/bin/bash

set -e

# This script builds the SQL compiler
# it has an optional argument which specifies whether to build
# using the current version of Calcite or the next unreleased version

# Default is to use the next version
NEXT='y'

if true; then
    # This is the standard behavior
    CALCITE_REPO="https://github.com/apache/calcite.git"
    CALCITE_BRANCH="main"
    CALCITE_NEXT_COMMIT="c456f7888b6aff3c3de6eda4fc16405b0b98be30"
else
    # Switch to this script when testing a branch in mihaibudiu's fork that
    # hasn't been merged yet
    CALCITE_REPO="https://github.com/mihaibudiu/calcite.git"
    CALCITE_BRANCH="stable-variant"
    CALCITE_NEXT_COMMIT="6317e1cbf1914390a6c1062c8563187b65468729"
fi
CALCITE_NEXT="1.39.0"
CALCITE_CURRENT="1.38.0"

usage() {
    echo "This script builds the sql-to-dbsp compiler"
    echo "known options: -n -c"
    echo "-c use the current released (${CALCITE_CURRENT}) version of Calcite [default]"
    echo "-n use the next (unreleased, ${CALCITE_NEXT}) version of Calcite"
    exit 1
}

update_pom() {
    VERSION=$1
    sed -i -E "s/<calcite.version>(.*)<\/calcite.version>/<calcite.version>${VERSION}<\/calcite.version>/" ./SQL-compiler/pom.xml
}

while getopts ":nc" flag
do
    case "${flag}" in
        c) NEXT='n'
           ;;
        n) NEXT='y'
           ;;
        *) echo "Invalid option: ${OPTARG}"
           usage
           ;;
    esac
done

if [ ${NEXT} = 'y' ]; then
    update_pom ${CALCITE_NEXT}
    pushd /tmp >/dev/null
    git clone --quiet --single-branch --branch ${CALCITE_BRANCH} --depth 50 ${CALCITE_REPO}
    cd calcite
    if [[ ! -z "${CALCITE_NEXT_COMMIT}" ]]; then
        git reset --hard ${CALCITE_NEXT_COMMIT}
    fi

    GROUP=org.apache.calcite
    VERSION=${CALCITE_NEXT}

    ./gradlew build -x test -x checkStyleMain -x autoStyleJavaCheck build --console=plain -Dorg.gradle.logging.level=quiet
    for DIR in core babel server linq4j
    do
        ARTIFACT=calcite-${DIR}
        mvn install:install-file -Dfile=${DIR}/build/libs/${ARTIFACT}-${VERSION}-SNAPSHOT.jar -DgroupId=${GROUP} -DartifactId=${ARTIFACT} -Dversion=${VERSION} -Dpackaging=jar -DgeneratePom=true -q -B
    done
    popd >/dev/null
    rm -rf /tmp/calcite
else
    update_pom ${CALCITE_CURRENT}
fi

mvn package -DskipTests --no-transfer-progress -q -B
