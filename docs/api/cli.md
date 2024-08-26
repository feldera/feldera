# Command line utility (fda)

`fda` is a command line utility for interacting with the Feldera Manager's REST API.

## Installation

In order to install `fda`, you need a working Rust environment. You can install Rust by following the instructions on
the [Rust website](https://www.rust-lang.org/tools/install).

### Using Cargo

Install `fda` with Cargo by running the following command:

```commandline
cargo install fda
```

Alternatively, to install the latest `fda` revision from our main git branch, run the following command:

```commandline
cargo install --git https://github.com/feldera/feldera fda
```

To install from the sources in your local feldera repository, you can install `fda` with the
following commands:

```commandline
cd crates/feldera-cli
cargo install --path .
```

### Optional: Shell completion

Once the `fda` binary is installed, you can enable shell command completion for `fda` by generating a completion script
for your shell and copying it to the appropriate location.

```commandline
fda shell-completion --help
fda shell-completion
```

Currently, Bash, Elvish, Fish, Powershell, and Zsh are supported.

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

## Usage Examples

Specify the host and API key as command line arguments or environment variables:

```commandline
fda --host https://try.feldera.com --auth apikey:0aKFj50iE... pipelines
export FELDERA_HOST=https://try.feldera.com
export FELDERA_API_KEY=apikey:0aKFj50iE...
fda pipelines
```

Create a new pipeline `p1` from a `program.sql` file:
```commandline
echo "CREATE TABLE example ( id INT NOT NULL PRIMARY KEY );
CREATE VIEW example_count AS ( SELECT COUNT(*) AS num_rows FROM example );" > program.sql
fda pipeline p1 create program.sql
```

Retrieve the program for `p1` and create a new pipeline `p2` from it:
```commandline
fda pipeline p1 program | fda pipeline p2 create -s -
```

Enable storage for `p1`:
```commandline
fda pipeline p1 set-config storage true
```

Run the pipeline `p1`:
```commandline
fda pipeline p1 start
```

Retrieve the stats for `p1`:
```commandline
fda pipeline p1 stats
```

Shutdown and delete the pipeline `p1`:
```commandline
fda pipeline p1 shutdown
fda pipeline p1 delete
```
