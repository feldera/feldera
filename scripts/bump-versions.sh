#!/bin/bash

set -ex

#
# Run `cargo install cargo-edit` once before you use this function
#
# When pushing commits created by this script, don't forget to use the --follow-tags argument to `git push`
#
release() {
    if [ $# -ne 1 ]; then 
        echo "Illegal number of parameters. Usage: ./bump-versions.sh major|minor|patch"
        exit 1
    fi

    if [ $1 = "major" ]; then
        cargo set-version --bump major -p pipeline-manager
    fi

    if [ $1 = "minor" ]; then
        cargo set-version --bump minor -p pipeline-manager
    fi

    if [ $1 = "patch" ]; then
        cargo set-version --bump patch -p pipeline-manager
    fi

    version=`cargo run --bin=api-server -- -V | awk '{print $NF}'`

    # Commit the new version
    git commit -am "release: bump project version to $version" -s
    git tag -a v$version -m "v$version"
}

release "$@"
