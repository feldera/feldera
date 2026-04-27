# Feldera I/O adapter library

This crate provides the **stable plugin ABI** for building I/O connectors for Feldera.
A connector crate depends only on `feldera-adapterlib` (plus `feldera-types` for config
types) and never on the internal `dbsp` crate.

See the [crate-level documentation](https://docs.rs/feldera-adapterlib) for the full
plugin ABI reference, the fault-tolerance contract, and example connector skeletons.

## SemVer policy

`feldera-adapterlib` follows [Semantic Versioning](https://semver.org/).

- **Minor version** bumps with each Feldera release (new features, additive changes).
- **Major version** bumps for any breaking change to the plugin ABI (trait method
  additions, parameter type changes, removed items).

CI runs [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) on
every merge to catch accidental breaking changes before they are released.

Connector crates should declare a dependency such as:

```toml
[dependencies]
feldera-adapterlib = "0"   # or the current major at time of writing
```
