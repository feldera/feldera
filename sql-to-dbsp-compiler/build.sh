#!/usr/bin/env bash

set -e

: "${CALCITE_BUILD_DIR:=/tmp/calcite}"
: "${CALCITE_CURRENT:=1.41.0}"

# Load environment overrides from calcite_version.env file, if present
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/calcite_version.env"
if [ -f "${ENV_FILE}" ]; then
    echo "Loading environment from ${ENV_FILE}"
    source "${ENV_FILE}"
fi

usage() {
    echo "This script builds the sql-to-dbsp compiler"
    echo "known options: -n -c"
    echo "-c use the current released (${CALCITE_CURRENT}) version of Calcite"
    echo "-n use the next (unreleased, ${CALCITE_NEXT}) version of Calcite"
    exit 1
}

update_pom() {
    VERSION=$1
    sed -i -E "s|<calcite.version>(.*)</calcite.version>|<calcite.version>${VERSION}</calcite.version>|" ./SQL-compiler/pom.xml
}

if [ "${CALCITE_BUILD_NEXT}" = "y" ]; then
    update_pom "${CALCITE_NEXT}"

    if [ ! -d "${CALCITE_BUILD_DIR}" ]; then
        echo "Cloning Calcite into ${CALCITE_BUILD_DIR}"
        git clone --depth 1 --quiet --single-branch --branch "${CALCITE_BRANCH}" ${GIT_ARGS} "${CALCITE_REPO}" "${CALCITE_BUILD_DIR}"
    else
        echo "Using existing Calcite source in ${CALCITE_BUILD_DIR}"
    fi

    if [[ -n "${CALCITE_NEXT_COMMIT}" ]]; then
        pushd "${CALCITE_BUILD_DIR}" >/dev/null
        git fetch --prune "${CALCITE_REPO}" "${CALCITE_NEXT_COMMIT}"
        echo "Checking out commit ${CALCITE_NEXT_COMMIT}"
        git checkout --force "${CALCITE_NEXT_COMMIT}"
        popd >/dev/null
    fi

    pushd "${CALCITE_BUILD_DIR}" >/dev/null
    ./gradlew build -x test -x checkStyleMain -x autoStyleJavaCheck build --console=plain -Dorg.gradle.logging.level=quiet

    for DIR in core server linq4j; do
        ARTIFACT=calcite-${DIR}
        mvn install:install-file \
            -Dfile="${DIR}/build/libs/${ARTIFACT}-${CALCITE_NEXT}-SNAPSHOT.jar" \
            -DgroupId=org.apache.calcite \
            -DartifactId="${ARTIFACT}" \
            -Dversion="${CALCITE_NEXT}" \
            -Dpackaging=jar \
            -DgeneratePom=true -q -B
    done
    popd >/dev/null
else
    update_pom "${CALCITE_CURRENT}"
fi

mvn package -DskipTests --no-transfer-progress -DargLine="-ea" -q -B "$@"
