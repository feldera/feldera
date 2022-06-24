[![codecov](https://codecov.io/gh/vmware/database-stream-processor/branch/main/graph/badge.svg?token=0wZcmD11gt)](https://codecov.io/gh/vmware/database-stream-processor)

# Database Stream Processor

Streaming and Incremental Computation Framework

## Setting up git hooks

* Execute the following command to make `git commit` check the code before commit:
```
GITDIR=$(git rev-parse --git-dir)
ln -sf $(pwd)/tools/pre-push ${GITDIR}/hooks/pre-push
```
