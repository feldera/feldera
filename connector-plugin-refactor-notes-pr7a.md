# Connector Plugin Refactor — PR 7a Notes

## Discrepancies and gaps

### 1. PR 4 never added the per-connector `connector_by_name` unit tests

The PR 4 checklist includes "Add a unit test that resolves `file_input`, `file_output`,
`clock` via `connector_by_name()` and asserts direction / kind / build_* slots."
This was skipped — no such tests exist anywhere in the codebase. PR 7a fixed the
gap for `s3_input` and `url_input` (tests added in `s3.rs::test::s3_input_descriptor`
and `url.rs::test::url_input_descriptor`). The file/clock tests remain missing.

**Plan update**: add `connector_by_name` descriptor tests for `file_input`,
`file_output`, and `clock` in PR 7b or as a cleanup item in PR 7g. The pattern is now
established in s3.rs and url.rs.

### 2. `secrets_dir` is unused by both connectors — pattern for 7b–7e

`build_s3_input` and `build_url_input` both accept `_secrets_dir` and ignore it. S3
reads credentials from config fields or the AWS credential chain; URL has no
credentials. This will hold for nats, pubsub, redis, and nexmark as well. Kafka is
likely the only bundled connector that actually reads from `secrets_dir`.

**Plan update**: the connector-authoring guide should note that `secrets_dir` is only
relevant for connectors that store credential file paths in config. `_secrets_dir` is
the correct declaration for all others.
