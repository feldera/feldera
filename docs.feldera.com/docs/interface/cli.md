import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Command line tool (fda)

`fda` is a command line utility for interacting with the Feldera Manager's REST API.
It allows you to create, manage, and monitor pipelines. It also features an interactive
[shell](#shell) for inspecting and modifying the state of tables and views using SQL commands.

## Installation

### Quick Install

<Tabs groupId="os">
  <TabItem value="linux" label="Linux">

```bash
curl -fsSL https://feldera.com/install-fda | bash
```

| Supported platforms |
|---|
| linux-x86_64 |
| linux-aarch64 |

Requires glibc >= 2.39 (Ubuntu 24.04+, Debian 13+, Fedora 40+, RHEL 10+).

  </TabItem>
  <TabItem value="windows" label="Windows">

```powershell
powershell -ExecutionPolicy Bypass -NoProfile -c "irm https://feldera.com/install-fda.ps1 | iex"
```

| Supported platforms |
|---|
| windows-x86_64 |
| windows-arm64 (via emulation) |

Requires Windows 10 or later with PowerShell 5.1+.
Installs `fda.exe` to `%USERPROFILE%\.feldera\bin` and adds it to user's `PATH`.

:::note
Corporate environments may block `irm | iex` via network or execution policy restrictions.
In that case, download the zip directly from the
[GitHub releases page](https://github.com/feldera/feldera/releases) and extract `fda.exe` manually.
:::

  </TabItem>
</Tabs>

### Installing a Specific Version

Since `fda` is a single binary, you can update or install older versions by re-running the installer script.

To install a specific version, pass the release git tag to the install script:

<Tabs groupId="os">
  <TabItem value="linux" label="Linux">

```bash
curl -fsSL https://feldera.com/install-fda | FDA_VERSION=v0.270.0 bash
```

  </TabItem>
  <TabItem value="windows" label="Windows">

```powershell
powershell -c "$env:FDA_VERSION='v0.270.0'; irm https://feldera.com/install-fda.ps1 | iex"
```

  </TabItem>
</Tabs>

To install to a custom directory:

<Tabs groupId="os">
  <TabItem value="linux" label="Linux">

```bash
curl -fsSL https://feldera.com/install-fda | FDA_VERSION=v0.270.0 FELDERA_INSTALL=/opt/feldera bash
```

  </TabItem>
  <TabItem value="windows" label="Windows">

```powershell
powershell -c "$env:FELDERA_INSTALL='C:\tools\feldera'; irm https://feldera.com/install-fda.ps1 | iex"
```

  </TabItem>
</Tabs>

### Using Cargo (macOS, other platforms)

To install `fda` with Cargo, you need a working Rust environment. You can install Rust by following
the instructions on the [Rust website](https://www.rust-lang.org/tools/install).

Run the following command to install `fda` as a Rust crate:

```bash
cargo install fda
```

Alternatively, to build and install the latest `fda` revision from our main git branch, run the following command:

```bash
cargo install --git https://github.com/feldera/feldera fda
```

To build from the sources in your local feldera repository, you can install `fda` with the
following commands:

```bash
cd crates/fda
cargo install --path .
```

### Optional: Shell completion

Once the `fda` binary is installed, you can enable shell command completion for `fda`
by adding the following line to your shell init script.

<Tabs groupId="os">
  <TabItem value="linux" label="Linux">

```bash
# Bash
echo "source <(COMPLETE=bash fda)" >> ~/.bashrc

# Elvish
echo "eval (COMPLETE=elvish fda)" >> ~/.elvish/rc.elv

# Fish
echo "source (COMPLETE=fish fda | psub)" >> ~/.config/fish/config.fish

# Zsh
echo "source <(COMPLETE=zsh fda)" >> ~/.zshrc
```

  </TabItem>
  <TabItem value="windows" label="Windows">

```powershell
# Powershell
mkdir -Force (Split-Path $PROFILE)
'$env:COMPLETE="powershell"; (fda | Out-String) | Invoke-Expression; Remove-Item Env:\COMPLETE -ErrorAction SilentlyContinue' >> $PROFILE
# To activate autocomplete without restarting the terminal:
. $PROFILE
```

  </TabItem>
</Tabs>

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

### Connecting to HTTPS with a custom CA

When the Feldera manager is served over HTTPS with a certificate issued by a private CA, or with a self-signed
certificate, `fda` needs to trust that CA before it can talk to the manager. The environment variable
`FELDERA_HTTPS_TLS_CERT` and the command line argument `--tls-cert` both take a path to a PEM-encoded certificate
file. The file may contain one or more certificates; every certificate in the bundle is added to the client's set
of trusted roots:

```bash
fda --host https://feldera.internal --tls-cert /etc/ssl/ca-bundle.pem pipelines
# or via environment:
export FELDERA_HTTPS_TLS_CERT=/etc/ssl/ca-bundle.pem
fda --host https://feldera.internal pipelines
```

`--tls-cert` extends the trust store while keeping certificate verification on, so it is the preferred option for
reaching HTTPS endpoints with custom CAs. The separate `--insecure` / `-k` flag disables verification entirely and
should only be used for local testing. `--tls-cert` and `--insecure` are mutually exclusive; passing both on the
same invocation is rejected by `fda`.

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
