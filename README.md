[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![CI workflow](https://github.com/vmware/database-stream-processor/actions/workflows/main.yml/badge.svg)](https://github.com/vmware/database-stream-processor/actions)
[![codecov](https://codecov.io/gh/vmware/database-stream-processor/branch/main/graph/badge.svg?token=0wZcmD11gt)](https://codecov.io/gh/vmware/database-stream-processor)

# Database Stream Processor

Streaming and Incremental Computation Framework

## Setting up git hooks

* Execute the following command to make `git commit` check the code before commit:
```
GITDIR=$(git rev-parse --git-dir)
ln -sf $(pwd)/tools/pre-push ${GITDIR}/hooks/pre-push
```
