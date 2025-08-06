# Monitoring and Profiling

A Feldera instance and its pipelines can be monitored using various tools. This tutorial
will guide you through setting up monitoring for your Feldera instance.

## Metrics with Grafana and Prometheus

Metrics are helpful to check the health of your Feldera instance and to identify resource
bottlenecks. Feldera exposes a metrics endpoint that can be scraped by Prometheus. Grafana
is then used to visualize these metrics.

See [Pipeline Metrics] for a reference to the Prometheus metrics that
Feldera exports.

[Pipeline Metrics]: /operations/metrics

### Setup

1. **Prometheus:** You must have [Prometheus installed](https://prometheus.io).
2. **Connect Prometheus to Feldera:**
    - Add the following to your `prometheus.yml` configuration file, usually located
      in `/etc/prometheus/prometheus.yml`:
    ```yaml
    - job_name: 'feldera'
      scrape_interval: 1s
      metrics_path: '/v0/metrics'
      static_configs:
        - targets: ['127.0.0.1:8080']
    ```
    - Replace `127.0.0.1:8080` with the address of your Feldera instance.
    - Restart Prometheus.
3. **Grafana:** You must have [Grafana installed](https://grafana.com).
4. **Add Prometheus To Grafana:**
    - If you are using a local prometheus instance, the URL for the Prometheus data source will
      be http://127.0.0.1:9090.
    - Follow the steps in the [Grafana
      documentation](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source)
      to add Prometheus as a data source.

### Setup with Docker

Alternatively, with docker, you can avoid installing Prometheus and Grafana to your local machine.
To run Feldera with both Prometheus and Grafana:

```sh
docker compose -f deploy/docker-compose.yml up grafana --force-recreate
```

This spins up:
1. Feldera
2. Prometheus
3. Grafana

If you want to run Prometheus without Grafana:

```sh
docker compose -f deploy/docker-compose.yml up prometheus --force-recreate
```

### Set up the monitoring Dashboard

1. **Copy the JSON of
   the [Feldera template dashboard](https://raw.githubusercontent.com/feldera/feldera/main/deploy/grafana_dashboard.json)
   **
2. **Import the dashboard into Grafana**
    - Under Dashboards, click the "New" icon, then click "Import Dashboard".
    - Paste the JSON copied from the template in the text-box and click "Load".

A Feldera Instance Monitoring dashboard will be created in Grafana.
The dashboard is a template and may need to be adjusted to fit your specific needs.
Look for the `feldera_*` metrics in Grafana to add more metrics to the dashboard.

## Tracing with Jaeger

A Feldera pipeline can be traced using Jaeger. Tracing is useful if you need to analyze
throughput or latency bottlenecks of your pipeline, as it instruments every step of the
pipeline execution and provides detailed information about the execution time of each
step.

### Setup

1. **Jaeger:** You must have [Jaeger installed](https://www.jaegertracing.io).
2. Start Jaeger using the `jaeger-all-in-one` script:
    ```bash
    ./jaeger-all-in-one
    ```
3. **Tracing a Feldera pipeline**
    - Enable/disable tracing and specify the Jaeger endpoint in the pipeline `runtime_config`:
   ```bash
   curl -i -X PATCH http://127.0.0.1:8080/v0/pipelines/tracing-pipeline \
   -H 'Content-Type: application/json' \
   -d '{
     "runtime_config": {"tracing": true, "tracing_endpoint_jaeger": "host.docker.internal:6831", <other config settings> }
   }'
   ```
    - Use `host.docker.internal:6831` if you are running Feldera in docker or
      `127.0.0.1:6831` if you run Feldera directly.
    - Make sure to specify other settings you changed from non-default values in the `runtime_config` as well.
      `runtime_config` can be retrieved with:
   ```bash
   curl -X 'GET' \
   'http://127.0.0.1:8080/v0/pipelines/tracing-pipeline' \
   -H 'accept: application/json'
   ```

## DBSP Profiles

A DBSP profile is a graph of the pipeline's circuit where each node represents an
operator and each edge represents a data flow between operators. The profile includes
information about how much CPU time or memory each operator consumes.

The API endpoint `/v0/<pipeline_name>/circuit_profile` can be used to download the DBSP
profile of a running pipeline. It returns a zip file containing multiple profiles (one
for each worker) as `.dot` files, and a `Makefile` to transform the `.dot` files into
`.pdf` files.
