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

    if ! (cargo set-version --version) >/dev/null 2>&1; then
        echo >&2 "$0: cargo-edit not installed, please run 'cargo install cargo-edit'"
        exit 1
    fi

    case $1 in
        major|minor|patch) cargo set-version --bump $1 -p pipeline-manager ;;
        *) echo >&2 "Argument must be 'major' or 'minor' or 'patch'"; exit 1 ;;
    esac

    version=`cargo run --bin=api-server -- -V | awk '{print $NF}'`

    # Commit the new version
    git commit -am "release: bump project version to $version" -s
    git tag -a v$version -m "v$version"
}

release "$@"
