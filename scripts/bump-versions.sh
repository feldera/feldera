#!/bin/bash

set -ex

#
# To use this function, make sure you have maven installed, and run `cargo install cargo-edit` first
#
# When pushing commits created by this script, don't forget to use the --follow-tags argument to `git push`
#
release() {
    if [ $# -ne 1 ]; then 
        echo "Illegal number of parameters. Usage: ./bump-versions.sh major|minor|patch"
        exit 1
    fi

    if [ $1 = "major" ]; then
        cargo set-version --bump major
    fi

    if [ $1 = "minor" ]; then
        cargo set-version --bump minor
    fi

    if [ $1 = "patch" ]; then
        cargo set-version --bump patch
    fi

    version=`cargo run --bin=api-server -- -V | awk '{print $NF}'`

    cd ../sql-to-dbsp-compiler/SQL-compiler

    mvn versions:set -DnewVersion=$version

    # Commit the new version
    git commit -am "release: bump project version to $version" -s
    git tag -a v$version -m "v$version"

    # Start next development cycle. Bump the SQL compiler to a
    # SNAPSHOT release for the next patch version.
    mvn release:update-versions --batch-mode
    git commit -am "release: start post-$version release cycle" -s
}

release "$@"
