# dbt-feldera

The [dbt](https://www.getdbt.com/) adapter for
[Feldera](https://www.feldera.com/). Feldera's DBSP engine automatically
incrementalizes every SQL query -- no watermarks, scans, or `MERGE`.

## Installation

```bash
pip install dbt-feldera
```

## Configuration

Add a Feldera target to your `profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: feldera
      host: "http://localhost:8080"
      api_key: "apikey:..."          # optional
      database: "default"
      schema: "my_pipeline"          # pipeline name
      compilation_profile: dev       # dev | unoptimized | optimized
      workers: 4
      timeout: 300
```

## Materializations

| Materialization      | Feldera object             |
| -------------------- | -------------------------- |
| `view`               | `CREATE VIEW`              |
| `table`              | `CREATE TABLE` (input)     |
| `incremental`        | `CREATE MATERIALIZED VIEW` |
| `streaming_pipeline` | Full pipeline program      |
| `seed`               | Table + HTTP ingress push  |

## Development

Requires Python 3.10+, [uv](https://github.com/astral-sh/uv), and Docker.

All development tasks go through a single script — see [`.scripts/run.sh`](.scripts/run.sh)

```bash
cd python/dbt-feldera

.scripts/run.sh all              # run everything in sequence
.scripts/run.sh venv             # fresh venv + install deps
.scripts/run.sh build            # build wheel to dist/*.whl
.scripts/run.sh lint             # ruff check + format
.scripts/run.sh unit-test        # pytest unit tests
.scripts/run.sh integration-test # pytest integration
.scripts/run.sh e2e              # dbt CLI end-to-end test
```

### Environment variables

| Variable              | Default                                              | Used by                   | Description                                                             |
| --------------------- | ---------------------------------------------------- | ------------------------- | ----------------------------------------------------------------------- |
| `FELDERA_URL`         | `http://localhost:8080`                              | `run.sh`, e2e             | Feldera API base URL                                                    |
| `FELDERA_SKIP_DOCKER` | _(unset)_                                            | `run.sh integration-test` | Set to `1` to skip Docker start/stop (use an external Feldera instance) |
| `FELDERA_IMAGE`       | `images.feldera.com/feldera/pipeline-manager:latest` | docker-compose            | Docker image for the Feldera container                                  |
| `FELDERA_PORT`        | `8080`                                               | docker-compose            | Host port mapped to the Feldera container                               |
| `RUST_LOG`            | `info`                                               | docker-compose            | Log level inside the Feldera container                                  |
| `SKIP_TEARDOWN`       | _(unset)_                                            | e2e (`run-dbt-local.sh`)  | Set to `1` to keep Feldera running after the e2e test and print UI URLs |
| `DBT_DOCS_PORT`       | `18081`                                              | e2e (`run-dbt-local.sh`)  | Host port suggested for `dbt docs serve`                                |
