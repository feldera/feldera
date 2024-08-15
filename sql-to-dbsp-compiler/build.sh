#!/bin/bash

# This script builds the SQL compiler
# it has an optional argument which specifies whether to build
# using the current version of Calcite or the next unreleased version

# Default is to use the current version
NEXT='n'

CALCITE_NEXT="1.38.0"
CALCITE_NEXT_COMMIT="7fa73c0079b03b8cd9657df0058a5743b80c1be9"
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
    pushd /tmp
    git clone git@github.com:apache/calcite.git
    cd calcite
    git reset --hard ${CALCITE_NEXT_COMMIT}

    GROUP=org.apache.calcite
    VERSION=${CALCITE_NEXT}

    ./gradlew build -x test -x checkStyleMain -x autoStyleJavaCheck build --console=plain -Dorg.gradle.logging.level=quiet
    for DIR in core babel server linq4j
    do
        ARTIFACT=calcite-${DIR}
        mvn install:install-file -Dfile=${DIR}/build/libs/${ARTIFACT}-${VERSION}-SNAPSHOT.jar -DgroupId=${GROUP} -DartifactId=${ARTIFACT} -Dversion=${VERSION} -Dpackaging=jar -DgeneratePom=true
    done
    popd
    rm -rf /tmp/calcite
else
    update_pom ${CALCITE_CURRENT}
fi

mvn package -DskipTests --no-transfer-progress -q -B



