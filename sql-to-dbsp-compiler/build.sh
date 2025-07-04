#!/usr/bin/env bash

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
    CALCITE_NEXT_COMMIT="cf91ec8f36e1f7cf9656e2f794d8f78a2b43d88c"
else
    # Switch to this script when using a branch in mihaibudiu's fork that
    # hasn't been merged yet
    CALCITE_REPO="https://github.com/mihaibudiu/calcite.git"
    CALCITE_BRANCH="save-branch"
    CALCITE_NEXT_COMMIT="225933d485130a65f1846f0d0c69b048195bbb56"
fi
CALCITE_NEXT="1.41.0"
CALCITE_CURRENT="1.40.0"

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
    if [[ ! -z "${CALCITE_NEXT_COMMIT}" ]]; then
        GIT_ARGS="--no-checkout"
    else
        GIT_ARGS="--depth 1"
    fi
    git clone --quiet --single-branch --branch ${CALCITE_BRANCH} ${GIT_ARGS} ${CALCITE_REPO}
    cd calcite
    if [[ ! -z "${CALCITE_NEXT_COMMIT}" ]]; then
        git fetch origin ${CALCITE_NEXT_COMMIT}
        git checkout ${CALCITE_NEXT_COMMIT}
    fi

    GROUP=org.apache.calcite
    VERSION=${CALCITE_NEXT}

    ./gradlew build -x test -x checkStyleMain -x autoStyleJavaCheck build --console=plain -Dorg.gradle.logging.level=quiet
    for DIR in core server linq4j
    do
        ARTIFACT=calcite-${DIR}
        mvn install:install-file -Dfile=${DIR}/build/libs/${ARTIFACT}-${VERSION}-SNAPSHOT.jar -DgroupId=${GROUP} -DartifactId=${ARTIFACT} -Dversion=${VERSION} -Dpackaging=jar -DgeneratePom=true -q -B
    done
    popd >/dev/null
    rm -rf /tmp/calcite
else
    update_pom ${CALCITE_CURRENT}
fi

mvn package -DskipTests --no-transfer-progress -DargLine="-ea" -q -B
