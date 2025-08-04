# Command line tool (fda)

`fda` is a command line utility for interacting with the Feldera Manager's REST API.
It allows you to create, manage, and monitor pipelines. It also features an interactive
[shell](#shell) for inspecting and modifying the state of tables and views using SQL commands.

## Installation

In order to install `fda`, you need a working Rust environment. You can install Rust by following the instructions on
the [Rust website](https://www.rust-lang.org/tools/install).

### Using Cargo

Install `fda` with Cargo by running the following command:

```bash
cargo install fda
```

Alternatively, to install the latest `fda` revision from our main git branch, run the following command:

```bash
cargo install --git https://github.com/feldera/feldera fda
```

To install from the sources in your local feldera repository, you can install `fda` with the
following commands:

```bash
cd crates/fda
cargo install --path .
```

### Binary installation

We supply pre-built binaries for `fda` as part of our release artifacts. You can find them in the
`feldera-binaries` ZIP file in the [github release page](https://github.com/feldera/feldera/releases/latest).
Note that currently only Linux binaries for amd64 and aarch64 architectures are provided.

### Optional: Shell completion

Once the `fda` binary is installed, you can enable shell command completion for `fda`
by adding the following line to your shell init script.

* Bash

```bash
echo "source <(COMPLETE=bash fda)" >> ~/.bashrc
```

* Elvish

```bash
echo "eval (COMPLETE=elvish fda)" >> ~/.elvish/rc.elv
```

* Fish

```bash
echo "source (COMPLETE=fish fda | psub)" >> ~/.config/fish/config.fish
```

* Powershell

```bash
echo "COMPLETE=powershell fda | Invoke-Expression" >> $PROFILE
```

* Zsh

```bash
echo "source <(COMPLETE=zsh fda)" >> ~/.zshrc
```

## Connecting & Authentication

To connect to the Feldera manager, you need to provide the URL of the manager. You can either provide the URL as an
environment variable or as a command line argument. The environment variable is called `FELDERA_HOST` and the
command line argument is called `--host`.

If your Feldera instance requires authentication (not needed in the local docker form factor), you'll also need to
provide an API key. You can either set the API key as an environment variable or as a command line argument.
The environment variable is called `FELDERA_API_KEY` and the command line argument is called `--auth`.
It is recommended to use an environment variable configured in your shell init script to avoid storing the API
key in your shell history.

:::info

You can create a new API key by logging into the Feldera WebConsole. Once logged in, click on your profile in the top
right. Go to **Manage API Keys** and click **Generate new key**.

:::

## Examples

Specify the host and API key as command line arguments or environment variables:

```bash
fda --host https://try.feldera.com --auth apikey:0aKFj50iE... pipelines
export FELDERA_HOST=https://try.feldera.com
export FELDERA_API_KEY=apikey:0aKFj50iE...
fda pipelines
```

Create a new pipeline `p1` from a `program.sql` file:

```bash
echo "CREATE TABLE example ( id INT NOT NULL PRIMARY KEY );
CREATE VIEW example_count AS ( SELECT COUNT(*) AS num_rows FROM example );" > program.sql
fda create p1 program.sql
```

Retrieve the program for `p1` and create a new pipeline `p2` from it:

```bash
fda program get p1 | fda create p2 -s
```

Enable storage for `p1`:

```bash
fda set-config p1 storage true
```

Add Rust UDF code to `p1`:

```bash
fda program set p1 --udf-toml udf.toml --udf-rs udf.rs
```

Run the pipeline `p1`:

```bash
fda start p1
```

Start a transaction for `p1`:

```bash
fda start-transaction p1
```

Commit a transaction for `p1`:

```bash
fda commit-transaction p1
```

Retrieve the stats for `p1`:

```bash
fda stats p1
```

Retrieve the latest log messages for `p1`:

```bash
fda logs p1
```

Download the support bundle for `p1`:

```bash
fda support-bundle p1
```

Shutdown and delete the pipeline `p1`:

```bash
fda shutdown p1
fda delete p1
```

Execute [ad-hoc SQL query](/sql/ad-hoc):

```bash
fda exec pipeline-name "SELECT * FROM materialized_view;"
cat query.sql | fda exec pipeline-name -s
```

## Shell

You can enter the `fda` shell for a pipeline by running the following command:

```bash
fda shell p1
```

Within the shell, you can interact with the pipeline `p1` by sending [ad-hoc SQL queries](/sql/ad-hoc) to it.

The shell also lets you execute certain CLI commands like `start`, `restart`, `shutdown` without having to provide the
pipeline name every time. Type `help` for more information.
