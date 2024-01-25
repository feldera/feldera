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

    # Update pipeline manager version
    old_version=`cargo metadata --no-deps | jq -r '.packages[]|select(.name == "pipeline-manager")|.version'`
    case $1 in
        major|minor|patch) cargo set-version --bump $1 -p pipeline-manager ;;
        *) echo >&2 "Argument must be 'major' or 'minor' or 'patch'"; exit 1 ;;
    esac

    # Retrieve new version
    new_version=`cargo metadata --no-deps | jq -r '.packages[]|select(.name == "pipeline-manager")|.version'`

    # Regenerate OpenAPI JSON to have updated version
    (cd .. && cargo make --cwd crates/pipeline_manager openapi_json)

    # Update changelog
    release_date=`date +"%Y-%m-%d"`
    sed -i.backup "s/\[Unreleased\]/\[Unreleased\]\n\n\#\# \[$new_version\] - $release_date/" ../CHANGELOG.md

    # Commit the new version
    release_branch="release-v$new_version"
    if git rev-parse "$release_branch" >/dev/null 2>&1; then
        echo "Error: branch $release_branch already exists. Aborting";
        exit 1;
    fi
    git checkout -b $release_branch
    git commit -am "release: bump project version to $new_version" -s

    # Patch the docker-compose.yml file with the new version. Check this
    # change in only after we confirm the new containers are available.
    sed -i.backup "s/\:\-${old_version}/\:\-${new_version}/g" ../deploy/docker-compose.yml
    sed -i.backup "s/\:\-${old_version}/\:\-${new_version}/g" ../deploy/docker-compose-debezium.yml

    # Patch the last stable pipeline manager version in the Earthlfile
    sed -i.backup "s/FELDERA_VERSION=${old_version}/FELDERA_VERSION=${new_version}/g" ../Earthfile
    sed -i.backup "s/pipeline-manager\:${old_version}/pipeline-manager\:${new_version}/g" ../Earthfile
    sed -i.backup "s/demo-container\:${old_version}/demo-container\:${new_version}/g" ../Earthfile
}

release "$@"
