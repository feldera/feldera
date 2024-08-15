#!/bin/bash

set -e

# This script builds the SQL compiler
# it has an optional argument which specifies whether to build
# using the current version of Calcite or the next unreleased version

# Default is to use the next version
NEXT='y'

CALCITE_NEXT="1.38.0"
CALCITE_NEXT_COMMIT="b60598b47e86b2b21639d193b374f1d6d4dcd139"
CALCITE_CURRENT="1.37.0"

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
    echo "Building calcite"
    pushd /tmp >/dev/null
    git clone --quiet https://github.com/apache/calcite.git
    cd calcite
    git reset --hard ${CALCITE_NEXT_COMMIT}

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
