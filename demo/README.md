# DBSP demo

A demo script that creates several DBSP projects and prepares test input data for them.

## Prerequisites

* Clone the SQL-to-dbsp compiler repo to a parallel directory to
  `database-stream-processor`

* Install and configure Postgres
  - Configure Postgres to trust connections from localhost without
    authentication. Edit `pg_hba.conf` file, replacing the following line:
    ```
    host    all             all             127.0.0.1/32            scram-sha-256
    ```
    with
    ```
    host    all             all             127.0.0.1/32            trust
    ```

  - Create Postgres user named `dbsp`:
    ```
    $ createuser dbsp
    ```

* Install RedPanda

* Install `gdown` (used to download large datasets from Google drive):
  ```
  pip install gdown
  ```

* [Install Prometheus](https://prometheus.io/download/)

* [Install Grafana and add Prometheus server as a data source](https://prometheus.io/docs/tutorials/visualizing_metrics_using_grafana/)

* Open Grafana (`localhost:3000`) and import Grafana dashboard in `pipeline_server/grafana_dashboard.json`.

## Running the demo

```
./demo/demo.sh
```

It can take a few minutes to populate RedPanda topics with data.  No need to
wait for the process to complete.  Navigate to `localhost:8080`.
