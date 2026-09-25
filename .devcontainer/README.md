# Feldera dev container

An Ubuntu 24.04 workspace with the Rust toolchain, Bun, Java, Python and
librdkafka, plus a Postgres instance for the tests that need one.

VS Code and any other editor that supports the Dev Containers specification read
`devcontainer.json` and start everything on their own. The rest of this file
covers driving the same containers by hand with podman.

## Services

| Service        | Purpose                            | Host port |
| -------------- | ---------------------------------- | --------- |
| `workspace`    | Build and test environment         | -         |
| `postgres-dev` | Database for the Postgres CDC tests | 5435      |
| `db-admin`     | Adminer web UI for `postgres-dev`  | 5436      |

## Launching with podman

Give the podman machine enough memory first. The Rust builds link large debug
binaries with one codegen/link job per CPU, so the default 2 GiB gets the
linker killed by the OOM killer.

```bash
podman machine stop
podman machine set --memory 32768 --cpus 8
podman machine start
```

`podman compose` delegates to an external provider, so install one. Prefer
`podman-compose`, which names containers `<project>_<service>_<index>` and
therefore produces `devcontainer_workspace_1`.

```bash
brew install podman-compose        # or your platform's package manager
```

Start the three services from the repository root. Pass both compose files, as
`devcontainer.json` does, so that the services of `deploy/docker-compose.yml`
join the same project and network and can be started alongside the workspace
later. The `workspace` environment points `REDPANDA_BROKERS` at the host
`redpanda`, which only resolves once that service runs; see the Redpanda note
below.

```bash
podman-compose -p devcontainer \
    -f .devcontainer/docker-compose.devcontainer.yml -f deploy/docker-compose.yml \
    up -d workspace postgres-dev db-admin
```

`podman-compose` ignores the `postCreateCommand` of `devcontainer.json`, so run
the provisioning script yourself. It installs librdkafka, which the Kafka
connectors link against, and the Playwright browsers used by the web console
tests. Re-run it after every `up --force-recreate`.

```bash
podman exec --user user devcontainer_workspace_1 \
    bash /workspaces/feldera/.devcontainer/postCreate.sh
```

Then open a shell. The repository is bind-mounted at `/workspaces/feldera`, so
edits on the host appear immediately in the container.

```bash
podman exec -it --user user devcontainer_workspace_1 bash
```

Tear the stack down with `podman-compose -p devcontainer -f
.devcontainer/docker-compose.devcontainer.yml -f deploy/docker-compose.yml
down`.

## Notes

- The dev container takes its Rust version from the `RUST_VERSION` build
  argument in `Dockerfile`. That argument governs this container only. A
  version bump also has to reach `Cargo.toml` (`rust-version`),
  `.pre-commit-config.yaml`, `deploy/Dockerfile` and `deploy/build.Dockerfile`,
  or local builds drift from the pre-commit hook and the release images.
- `docker compose` works too, but it names the container
  `devcontainer-workspace-1` with hyphens.
- Rust links with mold, configured in `~/.cargo/config.toml` inside the image.
  It applies to the workspace and to the crates the manager compiles per
  pipeline. To compare against GNU ld, run `cargo build` with
  `-C link-arg=-fuse-ld=bfd` instead.
- The Kafka connector tests need Redpanda, which sits behind the `redpanda`
  profile in `deploy/docker-compose.yml` and stays off otherwise. `--profile`
  is a global option, so it goes before the subcommand:

  ```bash
  podman-compose -p devcontainer --profile redpanda \
      -f .devcontainer/docker-compose.devcontainer.yml -f deploy/docker-compose.yml \
      up -d redpanda
  ```

- The `prometheus` and `grafana` profiles do not work from this invocation.
  They mount `./config/...`, which Compose resolves against the directory of
  the first `-f` file, so the paths land in `.devcontainer/config/` instead of
  `deploy/config/`. Start those two from `deploy/docker-compose.yml` on its
  own.
- SELinux is handled in the compose file, which sets `label=disable` on the
  workspace service. Without it an enforcing host denies the container access
  to the bind-mounted repository.
