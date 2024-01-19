# Demo

This directory contains the demos.

## Getting started

**Running a demo:** each demo is put into its own folder. Run a demo by going into
the directory and executing the main `run.py` Python script. For example:

```
cd hello-world
python3 run.py --help
python3 run.py --api-url=http://localhost:8080
```

**Demo setup:** each demo might have certain specific requirements to run such as
additional running services, certain network setup, or installed packages. The
following expectations are there for services:

* Feldera API: reachable at `http://localhost:8080` for the script
* Redpanda: reachable at `redpanda:9092` for the pipeline.
  For the script, reachable by setting environment variable
  `REDPANDA_BROKERS` to `redpanda:9092` if inside Docker Compose,
  else if outside `localhost:19092`.
* Kafka Connect (Debezium): reachable at `localhost:8083` for the script
* MySQL: reachable at `mysql:3306` for Kafka Connect (Debezium)

**Starting a pipeline:** the pipeline does not start automatically by default for all demos.
Moreover, some demo scripts have a `--start` flag argument to start the pipeline.
Alternatively, to do it manually, navigate to the web console (http://localhost:8080),
or issue the REST API commands directly:
```
curl -X POST http://localhost:8080/v0/pipelines/demo-name-of-pipeline/shutdown
curl -X POST http://localhost:8080/v0/pipelines/demo-name-of-pipeline/start
```

**Seeing progress:** either see the progress in the web console (http://localhost:8080)
or issue a REST API command directly:

```
curl -X GET http://localhost:8080/v0/pipelines/demo-name-of-pipeline/stats
```

## Docker container demos

The script `demo-container.sh` is started by the **demo** Docker container upon launch.
This script runs a subset of the demos. Edit this script if different demos need to
be run with the demo container (e.g., during development of a new demo).
