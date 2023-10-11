#!/bin/bash

set -ex

#
# Install cargo-edit (via cargo install cargo-edit) and jq before you use this function
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

    if ! (jq --version) >/dev/null 2>&1; then
        echo >&2 "$0: jq not installed"
        exit 1
    fi

    old_version=`cargo metadata --no-deps | jq -r '.packages[]|select(.name == "pipeline-manager")|.version'`
    case $1 in
        major|minor|patch) cargo set-version --bump $1 -p pipeline-manager ;;
        *) echo >&2 "Argument must be 'major' or 'minor' or 'patch'"; exit 1 ;;
    esac
    new_version=`cargo metadata --no-deps | jq -r '.packages[]|select(.name == "pipeline-manager")|.version'`
    (cd .. && cargo make --cwd crates/pipeline_manager openapi_python)

    # Commit the new version
    tag="v$new_version"
    if git rev-parse "$tag" >/dev/null 2>&1; then
        echo "Error: tag $tag already exists. Aborting";
        exit 1;
    fi
    git commit -am "release: bump project version to $new_version" -s
    git tag -a v$new_version -m "v$new_version"

    # Patch the docker-compose.yml file with the new version. Check this
    # change in only after we confirm the new containers are available.
    sed "s/\:\-${old_version}/\:\-${new_version}/g" ../deploy/docker-compose.yml > /tmp/docker-compose-bumped.yml
    mv /tmp/docker-compose-bumped.yml ../deploy/docker-compose.yml
    sed "s/\:\-${old_version}/\:\-${new_version}/g" ../deploy/docker-compose-debezium.yml > /tmp/docker-compose-bumped.yml
    mv /tmp/docker-compose-bumped.yml ../deploy/docker-compose-debezium.yml
}

release "$@"
