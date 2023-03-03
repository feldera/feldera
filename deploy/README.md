Bringing up a local instance of DBSP
===================================

First, install [Docker compose](https://github.com/docker/compose).

Next, to bring up a local DBSP instance and a Redpanda container, run the following from the `deploy/` folder:

```docker compose up``

This will bring up two containers: `dbsp` and `redpanda`.

Open your browser and you should now be able to see the pipeline manager dashboard on `localhost:8081`.
If you don't, double check that there are no port conflicts on your system (you can view and modify
the port mappings in `deploy/docker-compose.yml`).
