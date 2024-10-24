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
