Bringing up a local instance of Feldera Platform
===================================

First, install [Docker compose](https://docs.docker.com/compose/install/).

Next, to bring up a local Feldera Platform instance run the following:

```
curl -L https://github.com/feldera/feldera/releases/latest/download/docker-compose.yml | docker compose -f - up
```

This will bring up a DBSP and Postgres container.

Open your browser and you should now be able to see the Feldera Console UI
on `localhost:8080`. If you don't, double check that there are no
port conflicts on your system (you can view and modify the port mappings in
`deploy/docker-compose.yml`).

## Demo

If you'd like to prepopulate the Feldera Platform instance with a demo project,
run:

```
docker compose -f deploy/docker-compose.yml --profile demo up
```

This brings up an additional Redpanda container, followed by a container that
creates a sample SQL program and pipeline, loads some Redpanda topics with
data, and runs the resulting pipeline.

## Controlling rust logging levels

You can tune the DBSP container's log level using
[`RUST_LOG`](https://docs.rs/env_logger/0.10.0/env_logger/).

```
RUST_LOG=info docker compose -f deploy/docker-compose.yml up
```

## Feldera containers from sources

To bring up a local instance of Feldera Plaform from sources, run the following
from the project root:

```
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-dev.yml up --build
```

## Update the DBSP container

To update the DBSP container while it is already running to a newer image, run
the following. As of now, it will only preserve the API state (pipelines,
connectors and programs), but not the state of the individual pipelines. You
will have to reingest your data again via any configured connectors:

```
docker compose -f deploy/docker-compose.yml up -d dbsp --pull always
```
