Bringing up a local instance of DBSP
===================================


First, install [Docker compose](https://docs.docker.com/compose/install/).

Next, to bring up a local DBSP instance and a Redpanda container, run the following:

```
curl https://raw.githubusercontent.com/feldera/dbsp/main/deploy/docker-compose.yml | docker compose -f - up
```

This will bring up three containers: `dbsp`, `postgres` and `redpanda`.

Open your browser and you should now be able to see the pipeline manager dashboard on `localhost:8085`.
If you don't, double check that there are no port conflicts on your system (you can view and modify
the port mappings in `deploy/docker-compose.yml`).

If you want to bring up a local instance of DBSP from sources, run the following from the `deploy/` folder:

```
docker compose -f docker-compose.yml -f docker-compose-dev.yml up
```

If you'd like to prepopulate the DBSP instance with a demo, run:

```
docker compose -f docker-compose.yml --profile demo up

# for developers
docker compose -f docker-compose.yml -f docker-compose-dev.yml --profile demo up
```
