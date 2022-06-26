[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![CI workflow](https://github.com/vmware/database-stream-processor/actions/workflows/main.yml/badge.svg)](https://github.com/vmware/database-stream-processor/actions)
[![codecov](https://codecov.io/gh/vmware/database-stream-processor/branch/main/graph/badge.svg?token=0wZcmD11gt)](https://codecov.io/gh/vmware/database-stream-processor)

# Database Stream Processor

Database Stream Processor (DBSP) is a framework for computing over data streams
that aims to be more expressive and performant than existing streaming engines.

## Why DBSP?

Computing over streaming data is hard.  Streaming computations operate over
changing inputs and must continuously update their outputs as new inputs arrive.
They must do so in real time, processing new data and producing outputs with a
bounded delay.

Existing streaming engines have limited expressive power, which is why their use
in the modern data stack is restricted to data preparation and visualization.
Complex business intelligence tasks are left to traditional database systems
where they run as periodic (e.g., daily) batch jobs.  This is inadequate in the
world that increasingly relies on real-time alerts, recommendations, control, etc.

DBSP intends to change the situation by offering a simple promise: to **execute
any query workload that can run in batch mode in streaming mode**.

## Overview

DBSP unifies two kinds of streaming data: time series data and change data.

- **Time series data** can be thought of as an infinitely growing log indexed by
  time.

- **Change data** represents updates (insertions, deletions, modifications) to
  some state modeled as a table of records.

In DBSP, a time series is just a table where records are only ever added and
never removed or modified.  As a result, this table can grow unboundedly; hence
most queries work with subsets of the table within a bounded time window.  DBSP
does not need to wait for all data within a window to become available before
evaluating a query (although the user may choose to do so): like all queries,
time window queries are updated on the fly as new inputs become available.  This
means that DBSP can work with arbitrarily large windows as long as they fit
within available storage.

DBSP queries are composed of the following classes of operators that apply to
both time series and change data:

1. **Per-record operators** that parse, validate, filter, transform data streams
one record at a time.

1. The complete set of **relational operators**: select, project, join,
aggregate, etc.

1. **Recursion**: Recursive queries express iterative computations, e.g.,
partitioning a graph into strongly connected components.  Like all DPSP queries,
recursive queries update their outputs incrementally as new data arrives.

In addition, DBSP supports **windowing operators** that group time series data
into time windows, including various forms of tumbling and sliding windows,
windows driven by watermarks, etc.

## Theory

Delivering this functionality with strong performance and correctness guarantees
requires building on a solid foundation.  The theory behind DBSP is described in
the accompanying paper:

- [Budiu, McSherry, Ryzhyk, Tannen. DBSP: Automatic Incremental View Maintenance
  for Rich Query Languages](https://arxiv.org/abs/2203.16684)

## Applications

*TODO*

## Documentation

The project is still in its early days.  API and internals documentation is
coming soon.

*TODO*

# Contributing

## Setting up git hooks

Execute the following command to make `git commit` check the code before commit:

```
GITDIR=$(git rev-parse --git-dir)
ln -sf $(pwd)/tools/pre-push ${GITDIR}/hooks/pre-push
```
