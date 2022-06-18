[![codecov](https://codecov.io/gh/vmware/database-stream-processor/branch/main/graph/badge.svg?token=0wZcmD11gt)](https://codecov.io/gh/vmware/database-stream-processor)

# Database Stream Processor

Streaming and Incremental Computation Framework

## Development tools

Ideally this code should run fine in Linux, MacOs, and Windows.
The code is written in Rust.  Here are some tools we found useful for development:

See <https://www.twelve21.io/getting-started-with-rust-on-windows-and-visual-studio-code> about
using Rust in VSCode.

* Rust `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
* Tools for Rust
  * tarpaulin for code coverage (Linux only): `cargo install cargo-tarpaulin`
* Vscode: <https://code.visualstudio.com/Download>
* Vscode extensions for Rust:
  * crates <https://marketplace.visualstudio.com/items?itemName=serayuzgur.crates>
  * Rust <https://marketplace.visualstudio.com/items?itemName=rust-lang.rust>
  * Even Better TOML <https://marketplace.visualstudio.com/items?itemName=tamasfe.even-better-toml>
  * Power Header <https://marketplace.visualstudio.com/items?itemName=epivision.vscode-file-header>
    (Enables you to insert a copyright header in VSCode by pressing CTRL+ALT+H)
  * Rust Test Explorer <https://marketplace.visualstudio.com/items?itemName=swellaby.vscode-rust-test-adapter>
  * Rust debugger CodeLLDB <https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb>
  * Coverage Gutters <https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters>

## Check test coverage

* Run on the command line `cargo tarpaulin --out Lcov` to generate coverage information
* Use the Coverage Gutters VSCode extension to watch the lcov file (CTRL+SHIFT+P, Coverage Gutters: Watch)

## Set-up git hooks

* Execute the following command to make `git commit` check the code before commit:
```
GITDIR=$(git rev-parse --git-dir)
ln -sf $(pwd)/tools/prepush.sh ${GITDIR}/hooks/pre-push
```
