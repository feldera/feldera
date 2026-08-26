#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# The Kafka connectors link librdkafka dynamically, so the workspace does not
# build until it is installed. Ubuntu's package is older than rdkafka-sys
# requires, and the script builds the matching version against AWS-LC; see the
# script header.
sudo "$repo_root/scripts/install-librdkafka.sh"

cd "$repo_root/js-packages/web-console"

# Install Playwright system dependencies
bunx playwright install-deps
# Install Playwright browsers
bunx playwright install
