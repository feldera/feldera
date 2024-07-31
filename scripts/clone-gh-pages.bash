#!/bin/bash
#
# For CI env: Don't set `SMOKE`
#
set -ex

# Clean-up old results
if [ "$SMOKE" = "" ]; then
rm -rf gh-pages
fi

rm -f nexmark_comment.txt

# Clone repo
if [ ! -d "gh-pages" ]; then
    git clone --depth 1 -b main git@github.com:feldera/benchmarks.git gh-pages
fi