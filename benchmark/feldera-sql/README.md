# Feldera Nexmark benchmark in SQL

Feldera includes two versions of the Nexmark benchmark: one
implemented in Rust and the other in SQL.  This is the SQL version of
the benchmark, which is less mature and less complete than the Rust
version.  To run the Rust version of the benchmark, use `cargo bench
--bench nexmark`.

To run the SQL version of the benchmark, first bring up a Feldera
instance, e.g. by running (in a separate terminal):

```
cargo run -p pipeline-manager
```

Once you've done that, you can run the benchmark itself with a command
like this:

```
python3 benchmark/feldera-sql/run.py --api-url http://localhost:8080
```

The `--api-url` above is correct if you started the pipeline manager
locally with a command like the `cargo` one above.  If you are running
against a remote Feldera instance or if you started it to listen on a
different port, supply the correct URL instead.
