#!/bin/bash

# Push results to gh-pages repo
cd "$(dirname "$0")"

cd ../../gh-pages
if [ "$CI" = true ] ; then
    git config user.email "no-reply@dbsp.systems"
    git config user.name "dbsp-ci"
fi
git add .
git commit -a -m "Refreshed Flink benchmark results."
git push origin main
cd ..
rm -rf gh-pages
git clean -f
