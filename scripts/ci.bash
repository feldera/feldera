#!/bin/bash
#
# Usage: $ CI_MACHINE_TYPE='skylake-2x' bash scripts/ci.bash
#
set -ex

if [ -v ${CI_MACHINE_TYPE} ]; then
    echo "Need to set CI_MACHINE_TYPE"
    exit 1
fi

# Clean-up old results
rm -rf gh-pages
NEXMARK_CSV_FILE='nexmark_results.csv'
rm -f ${NEXMARK_CSV_FILE}
rm -f nexmark_comment.txt

# Run benchmarks
cargo bench --bench nexmark --features with-nexmark -- --first-event-rate=10000000 --max-events=100000000 --cpu-cores 8  --num-event-generators 6 --source-buffer-size 10000 --input-batch-size 40000 --csv

# Clone repo
rm -rf gh-pages
git clone --depth 1 -b main git@github.com:gz/dbsp-benchmarks.git gh-pages
pip3 install -r gh-pages/requirements.txt

# If you change this, adjust the command also in the append_csv function in utils.py:
DATE_PREFIX=`date +"%Y-%m-%d-%H-%M"`
if [ ! -v GITHUB_SHA ]; then
    # So we can run the script in non-runner environments:
    GITHUB_SHA=`git rev-parse HEAD`
fi

DEPLOY_DIR="gh-pages/nexmark/${CI_MACHINE_TYPE}/${GITHUB_SHA}/"
if [ -d "${DEPLOY_DIR}" ]; then
    # If we already have results for this SHA (the directory exists),
    # we will add the new results in a subdir
    DEPLOY_DIR=${DEPLOY_DIR}${DATE_PREFIX}
fi

# Copy nexmark results
mkdir -p ${DEPLOY_DIR}
mv ${NEXMARK_CSV_FILE} ${DEPLOY_DIR}
gzip ${DEPLOY_DIR}/${NEXMARK_CSV_FILE}

# Update CI history plots
python3 gh-pages/_scripts/ci_history.py --append --machine $CI_MACHINE_TYPE

# Push results to gh-pages repo
cd gh-pages
if [ "$CI" = true ] ; then
    git config user.email "no-reply@dbsp.systems"
    git config user.name "dbsp-ci"
fi
git add .
git commit -a -m "Added benchmark results for $GITHUB_SHA."
git push origin main
cd ..
python3 gh-pages/_scripts/compare_nexmark.py > nexmark_comment.txt
rm -rf gh-pages
git clean -f