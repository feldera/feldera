# Contributing to Feldera

The Feldera team welcomes contributions from the community. Before you start working with Feldera, please
read our [Developer Certificate of Origin (DCO)](https://developercertificate.org/).
To acknowledge the DCO, sign your commits by adding `Signed-off-by: Your Name <your@email.com>` to the last
line of each Git commit message. Your signature certifies that you wrote the patch or have the right to pass
it on as an open-source patch. The e-mail address used to sign must match the e-mail address of the Git
author. If you set your `user.name` and `user.email` git config values, you can sign your commit automatically
with `git commit -s`.

## Dependencies

Our team develops and tests using Linux and MacOS. Windows Subsystem for Linux works fine too.

The Feldera container images and CI workflows use Linux. You can see our setup in
our [Dockerfile](deploy/Dockerfile).

Our dependencies are:

- Runtime
    - a Rust tool chain (install rustup and the default toolchain)
        - this will need a C and C++ compiler installed (e.g., gcc, gcc++)
    - cmake
    - libssl-dev
    - libsasl2-dev
- SQL Compiler
    - a Java Virtual Machine (at least Java 19)
    - maven
    - graphviz
- Cloud
    - Python 3
    - Redpanda or Kafka
- Web Console
    - Bun (https://bun.sh/docs/installation)

Additional dependencies are automatically installed by the Rust,
maven, Python, and TypeScript build tools.

## Contribution Flow

### Forking

We recommend forking the Feldera repository and contributing from a fork.
This [page](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
has instructions on how to fork a repository. After forking do not
forget to add Feldera as a remote repository:

```shell
git remote add upstream https://github.com/feldera/feldera.git
```

### Workflow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work
- Make commits of logical units
- Make sure your commit messages are in the proper format (see below)
- Push your changes to a topic branch in the repository (push to your fork if
  you don't have commit access to the Feldera repository --- pushing directly
  to the repo is preferred because then CI will be able to add benchmark
  results to the PR in the comments).
- Submit a pull request

Example:

``` shell
git checkout -b my-new-feature main
git commit -a
git push origin my-new-feature
```

### Staying In Sync With Upstream

When your branch gets out of sync with the feldera/main branch, use the following to update:

``` shell
git checkout my-new-feature
git fetch -a
git pull --rebase upstream main
git push --force-with-lease upstream my-new-feature
```

If you don't have permissions replace the last command with

```
git push --force-with-lease origin my-new-feature
```

### Updating pull requests

If your PR fails to pass CI or needs changes based on code review, you'll most likely want to squash these changes into
existing commits.

If your pull request contains a single commit or your changes are related to the most recent commit, you can simply
amend the commit.

``` shell
git add <files to add>
git commit --amend
git push --force-with-lease origin my-new-feature
```

If you need to squash changes into an earlier commit, you can use:

``` shell
git add <files to add>
git commit --fixup <commit>
git rebase -i --autosquash main
git push --force-with-lease origin my-new-feature
```

Be sure to add a comment to the PR indicating your new changes are ready to review, as GitHub does not generate a
notification when you git push.

### Merging a pull request

Since we run benchmarks as part of the CI, it's a good practice to preserve the commit IDs of the feature branch
we've worked on (and benchmarked).
Unfortunately, [the github UI does not have support for this](https://github.com/community/community/discussions/4618)
(it only allows rebase, squash and merge commits to close PRs).
Therefore, it's recommended to merge PRs using the following git CLI invocation:

```shell
git checkout main
git merge --ff-only feature-branch-name
git push upstream main
```

### Code Style

Execute the following command to make `git push` check the code for formatting issues.

```shell
GITDIR=$(git rev-parse --git-dir)
ln -sf $(pwd)/scripts/pre-push ${GITDIR}/hooks/pre-push
```

### Formatting Commit Messages

We follow the conventions on [How to Write a Git Commit Message](http://chris.beams.io/posts/git-commit/).

Be sure to include any related GitHub issue references in the commit message. See
[GFM syntax](https://guides.github.com/features/mastering-markdown/#GitHub-flavored-markdown) for referencing issues
and commits.

## Reporting Bugs and Creating Issues

When opening a new issue, try to roughly follow the commit message format conventions above.

# For developers

## Building Feldera from sources

Feldera is implemented in Rust and uses Rust's `cargo` build system. The SQL
to DBSP compiler is implemented in Java and uses `maven` as its build system.

You can build the rust sources by runnning the following at the top level of this tree.

```
cargo build
```

To build the SQL to DBSP compiler, run the following from `sql-to-dbsp-compiler`:

```
./build.sh
```

If you want to develop Feldera without installing the required toolchains
locally, you can use Github Codespaces; from
https://github.com/feldera/feldera, click on the green `<> Code` button,
then select Codespaces and click on "Create codespace on main".

## Learning the DBSP Rust code

DBSP is a key crate that powers Feldera's pipelines.
To learn how the DBSP core works, we recommend starting with the tutorial.

From the project root:

```
cargo doc --open
```

Then search for `dbsp::tutorial`.

Another good place to start is the `circuit::circuit_builder` module documentation,
or the examples folder. For more sophisticated examples, try looking
at the `nexmark` benchmark in the `benches` directory.

## Running Benchmarks against DBSP

The repository has a number of benchmarks available in the `benches` directory that provide a comparison of DBSP's
performance against a known set of tests.

Each benchmark has its own options and behavior, as outlined below.

### Nexmark Benchmark

You can run the complete set of Nexmark queries, with the default settings, with:

```shell
cargo bench --bench nexmark
```

By default this will run each query with a total of 100 million events emitted at 10M per second (by two event generator
threads), using 2 CPU cores for processing the data.

To run just the one query, q3, with only 10 million events, but using 8 CPU cores to process the data and 6 event
generator threads, you can run:

```shell
cargo bench --bench nexmark -- --query q3 --max-events 10000000 --cpu-cores 8 --num-event-generators 6
```

For further options that you can use with the Nexmark benchmark,

```shell
cargo bench --bench nexmark -- --help
```

An extensive blog post about the implementation of Nexmark in DBSP:
<https://liveandletlearn.net/post/vmware-take-3-experience-with-rust-and-dbsp/>

## Updating the pipeline manager database schema

The pipeline manager serves as the API server for Feldera. It persists API state in a Postgres DB instance.
Here are some guidelines when contributing code that affects this database's schema.

* We use SQL migrations to apply the schema to a live database to facilitate upgrades. We
  use [refinery](https://github.com/rust-db/refinery) to manage migrations.
* The migration files can be found in `crates/pipeline-manager/migrations`
* Do not modify an existing migration file. If you want to evolve the schema, add a new SQL or rust file to the
  migrations folder
  following [refinery's versioning and naming scheme](https://docs.rs/refinery/latest/refinery/#usage). The migration
  script should update an existing schema as opposed to assuming a clean slate. For example, use `ALTER TABLE` to add a
  new column to an existing table and fill that column for existing rows with the appropriate defaults.
* If you add a new migration script `V{i}`, add tests for migrations from `V{i-1}` to `V{i}`. For example, add tests
  that invoke the pipeline manager APIs before and after the migration.

## Logging

By default, the pipeline-manager and pipelines create an `env_logger` which logs
the Feldera crates at INFO level and all other crates at WARN level.
This can be overridden by setting the `RUST_LOG` environment variable.
For example, the following would be the same as the default with additionally
backtrace enabled:

```bash
RUST_BACKTRACE=1 RUST_LOG=warn,pipeline_manager=info,feldera_types=info,project=info,dbsp=info,dbsp_adapters=info,dbsp_nexmark=info cargo run --package=pipeline-manager --features pg-embed --bin pipeline-manager -- --dev-mode
```

## Release process

The release process is done through github actions. Launch the "Create a release" action
manually from the github actions UI. You have to provide a Git SHA which you want to release as
the new version. The release CI scripts will then run in this order:

* ci-release.yml
  - Publishes a new release on github for the commit
      - Adds the binaries that we built during the merge queue
  - Tags the docker image that we also built during the merge queue as $version and `latest`
* ci-post-release.yml
  - Releases the python library to pypi
  - Releases the rust crates to crates.io
  - Determines the next version (this is controlled by a `RELEASE_NEXT_VERSION` variable in the repo settings)
  - Bumps the versions in Cargo.toml and pyproject.toml and openapi.yaml to the next version
  - Commits and pushes the changes to main

Note that the release process requires that the commit you want to release was merged into main
through the merge queue, otherwise the build artifacts will not be available.
