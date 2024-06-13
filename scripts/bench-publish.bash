#!/bin/bash
#
# Usage (to test): $ SMOKE=true CI_MACHINE_TYPE='skylake-2x' bash scripts/bench-publish.bash
# For CI env: Don't set `SMOKE`
#
set -ex

if [ -v ${CI_MACHINE_TYPE} ]; then
    echo "Need to set CI_MACHINE_TYPE"
    exit 1
fi

# Clean-up old results
if [ "$SMOKE" = "" ]; then
rm -rf gh-pages
fi

NEXMARK_CSV_FILE='nexmark_results.csv'
NEXMARK_DRAM_CSV_FILE='dram_nexmark_results.csv'
NEXMARK_SQL_CSV_FILE='sql_nexmark_results.csv'
NEXMARK_PERSISTENCE_CSV_FILE='persistence_nexmark_results.csv'
GALEN_CSV_FILE='galen_results.csv'
LDBC_CSV_FILE='ldbc_results.csv'
rm -f nexmark_comment.txt

# Clone repo
if [ ! -d "gh-pages" ]; then
    git clone --depth 1 -b main git@github.com:feldera/benchmarks.git gh-pages
fi
pip3 install -r gh-pages/requirements.txt

# If you change this, adjust the command also in the append_csv function in utils.py:
DATE_PREFIX=`date +"%Y-%m-%d-%H-%M"`
if [ -z "${PR_COMMIT_SHA}" ]; then
    # So we can run the script in non-runner environments and not pull request events
    PR_COMMIT_SHA=`git rev-parse HEAD`
fi

# Add nexmark results to repo
DEPLOY_DIR="gh-pages/nexmark/${CI_MACHINE_TYPE}/${PR_COMMIT_SHA}/"
if [ -d "${DEPLOY_DIR}" ]; then
    # If we already have results for this SHA (the directory exists),
    # we will add the new results in a subdir
    DEPLOY_DIR=${DEPLOY_DIR}${DATE_PREFIX}
fi

# Copy nexmark results
mkdir -p ${DEPLOY_DIR}
mv ${NEXMARK_CSV_FILE} ${NEXMARK_DRAM_CSV_FILE} ${NEXMARK_SQL_CSV_FILE} ${DEPLOY_DIR}
gzip -f ${DEPLOY_DIR}/${NEXMARK_CSV_FILE}
#gzip -f ${DEPLOY_DIR}/${NEXMARK_PERSISTENCE_CSV_FILE}
gzip -f ${DEPLOY_DIR}/${NEXMARK_DRAM_CSV_FILE}
gzip -f ${DEPLOY_DIR}/${NEXMARK_SQL_CSV_FILE}

# Add galen results to repo
DEPLOY_DIR="gh-pages/galen/${CI_MACHINE_TYPE}/${PR_COMMIT_SHA}/"
if [ -d "${DEPLOY_DIR}" ]; then
    # If we already have results for this SHA (the directory exists),
    # we will add the new results in a subdir
    DEPLOY_DIR=${DEPLOY_DIR}${DATE_PREFIX}
fi
# Copy galen results
mkdir -p ${DEPLOY_DIR}
mv ${GALEN_CSV_FILE} ${DEPLOY_DIR}
gzip -f ${DEPLOY_DIR}/${GALEN_CSV_FILE}

# Add ldbc results to repo
#DEPLOY_DIR="gh-pages/ldbc/${CI_MACHINE_TYPE}/${PR_COMMIT_SHA}/"
#if [ -d "${DEPLOY_DIR}" ]; then
    # If we already have results for this SHA (the directory exists),
    # we will add the new results in a subdir
#    DEPLOY_DIR=${DEPLOY_DIR}${DATE_PREFIX}
#fi
# Copy ldbc results
#mkdir -p ${DEPLOY_DIR}
#mv ${LDBC_CSV_FILE} ${DEPLOY_DIR}
#gzip -f ${DEPLOY_DIR}/${LDBC_CSV_FILE}

# Update CI history plots
python3 gh-pages/_scripts/ci_history.py --append --machine $CI_MACHINE_TYPE

# Push results to gh-pages repo
if [ "$SMOKE" = "" ]; then
    cd gh-pages
    if [ "$CI" = true ] ; then
        git config user.email "no-reply@dbsp.systems"
        git config user.name "dbsp-ci"
    fi
    git add .
    git commit -a -m "Added benchmark results for $PR_COMMIT_SHA."
    git push origin main
    cd ..
    python3 gh-pages/_scripts/compare_nexmark.py > nexmark_comment.txt
    rm -rf gh-pages
    git clean -f
else
    python3 gh-pages/_scripts/compare_nexmark.py > nexmark_comment.txt
fi
