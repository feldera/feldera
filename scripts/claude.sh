#!/bin/bash

set -e

FILES=$(git ls-tree -r --name-only claude-context | grep 'CLAUDE\.md')
git checkout claude-context -- $FILES
git restore --staged $FILES
echo "✅ Pulled Claude context files from claude-context branch."