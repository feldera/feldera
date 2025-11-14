# Deploy using Docker Compose

## Requirements

The following are required to build images and deploy them:

* [**docker**](https://docs.docker.com/engine/install/)
  (ideally, configured to run [rootless](https://docs.docker.com/engine/security/rootless/)):
  `docker --version`

* [**docker compose**](https://docs.docker.com/compose/install/):
  `docker compose version`

## Development

First, clone the `feldera` repository and navigate into it.
All commands below should be run from the root directory of the repository.

- **Latest release (same as Quickstart):**
  ```bash
  docker compose -f deploy/docker-compose.yml up
  ```

- **Built using local sources:** this can take a while as it both builds the pipeline
  manager and performs pre-compilation.
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 up --build
  ```
  As this section is for development, below we specify `-dev` configuration file each time.
  To use only the releases, remove the `-f ...-dev.yml` arguments.

- **Change logging level:**
  ```bash
  RUST_LOG=debug docker compose -f deploy/docker-compose.yml \
                                -f deploy/docker-compose-dev.yml \
                                up --build
  ```

- **Monitoring with Prometheus and Grafana:**
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 --profile grafana up --build
  ```
  ... after which you can view Prometheus at http://localhost:9090
  and Grafana at http://localhost:3000

- **Demo:**
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 -f deploy/docker-compose-extra.yml \
                 -f deploy/docker-compose-demo.yml \
                 --profile demo-debezium-mysql up --build
  ```

  The demo profiles can be found within: `deploy/docker-compose-demo.yml`

- **Redpanda:**
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 --profile redpanda up --build
  ```

- **Kafka Connect:**
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 -f deploy/docker-compose-extra.yml \
                 --profile kafka-connect up --build
  ```

- **Kafka Connect with MySQL:**
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 -f deploy/docker-compose-extra.yml \
                 --profile mysql --profile kafka-connect up --build
  ```

- **Kafka Connect with Postgres:**
  ```bash
  docker compose -f deploy/docker-compose.yml \
                 -f deploy/docker-compose-dev.yml \
                 -f deploy/docker-compose-extra.yml \
                 --profile postgres --profile kafka-connect up --build
  ```

# How to add a new package to the Docker images

Let's say you discover that a new dependency (that can be installed
using `apt-get`) is needed for Feldera.  Here's what you have to do to
get it included:

1. Edit `build.Dockerfile` and `Dockerfile` in this directory and add
  the dependency to the list of dependencies; add a comment explaining
  why it is needed.

  Note that the these Dockerfiles serve slightly different use-cases
  depending on what you're trying to do: `build.Dockerfile` is used to
  create a uniform CI environment to build the binaries we run (e.g.,
  pipeline manager, fda etc.). `Dockerfile` is used to create the
  _runtime_ image that users of feldera download and execute (it runs
  the pipeline manager and compiles pipelines). Most of the
  dependencies will need to be in both files, given the fact that we
  also run tests in the environment of `build.Dockerfile`, but in rare
  cases it can happen that a dependency or system tool would only need
  to be in one of the files.

2. Create a PR with this change, and submit it to some upstream branch

3. Now you need to build and save a new Docker image to be used in CI.
   This is done using the github UI

   * Navigate to https://github.com/feldera/feldera/actions

   * Click on `Docker CI Image` on the left (you can also get there directly
   with https://github.com/feldera/feldera/actions/workflows/build-docker-dev.yml)

   * Run the workflow (button "Run workflow" on top-right) on the branch you
   used in step 2 above.

   * When the workflow is finished it will publish the last Docker image here:
   https://github.com/feldera/feldera-ci-test/pkgs/container/feldera-dev
   You need to copy the long hash number corresponding to the new image and use
   it in the next step.

4. Change all the CI scripts to use the new Docker image.
   These scripts are in .github/workflows/*.yaml

   * For each script that contains a line similar to:

```
   container:
    image: ghcr.io/feldera/feldera-dev:sha-2151999cad3499a8e87ba804ffa6750f925309d9
```

  you need to replace the hash value after the colon `:` with the new hash value.

  At the time of writing these instructions there were 8 files involved, but this
  may change.  You can get the list of files by `grep`-ing for the old hash value.

  A command like the one below will do the trick if you substitute the
  correct new hash into it:

  ```
  sed -i 's/feldera-dev:sha-[0-9a-f]*/feldera-dev:sha-a6c448f6eaa832d34bd5d2f6b2b4167245a8de36/' .github/workflows/*.yml
  ```

  * `build-java.yml`
  * `build-rust.yml`
  * `ci-post-release.yml`
  * `publish-crates.yml`
  * `test-integration.yml`
  * `test-java-nightly.yml`
  * `test-java.yml`
  * `test-unit.yml`
