# Monitoring & Performance

A Feldera instance and its pipelines can be monitored using various tools. This tutorial
will guide you through setting up monitoring for your Feldera instance.

## Metrics with Grafana and Prometheus

Metrics are helpful to check the health of your Feldera instance and to identify resource
bottlenecks. Feldera exposes a metrics endpoint that can be scraped by Prometheus. Grafana
is then used to visualize these metrics.

### Setup

1. **Prometheus:** You must have [Prometheus installed](https://prometheus.io).
2. **Connect Prometheus to Feldera:**
    - Add the following to your `prometheus.yml` configuration file, usually located
      in `/etc/prometheus/prometheus.yml`:
    ```yaml
    - job_name: 'feldera'
      scrape_interval: 1s
      static_configs:
        - targets: ['localhost:8081']
    ```
    - Replace `localhost:8081` with the address of your Feldera instance. Note that `8081` is the default port for
      metrics.
    - Restart Prometheus.
3. **Grafana:** You must have [Grafana installed](https://grafana.com).
4. **Add Prometheus To Grafana:**
    - If you are using a local prometheus instance, the URL for the Prometheus data source will
      be `http://localhost:9090`.
    - Follow the steps in the [Grafana
      documentation](https://prometheus.io/docs/visualization/grafana/#creating-a-prometheus-data-source)
      to add Prometheus as a data source.

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
3. **Enable tracing in a Feldera pipeline**
    - You can also enable/disable tracing and specify the Jaeger endpoint in the pipeline configuration:
   ```bash
   curl -i -X PUT http://localhost:8080/v0/pipelines/tracing-pipeline \
      -H 'Content-Type: application/json' \
      -d '{
        "description": "Example pipeline",
        "program_name": "example-program",
        "config": {"tracing": true, "tracing_endpoint_jaeger": "tracing.feldera.io:6831"},
        "connectors": [] 
      }'
   ```
    - The default tracing endpoint will send data to `localhost:6381`.

## DBSP Profiles

A DBSP profile is a graph of the pipeline's circuit where each node represents an
operator and each edge represents a data flow between operators. The profile includes
information about how much CPU time or memory each operator consumes.

The API endpoint `/v0/<pipeline_name>/dump_profile` can be used to download the DBSP
profile of a running pipeline. It returns a zip file containing multiple profiles (one
for each worker) as `.dot` files, and a `Makefile` to transform the `.dot` files into
`.pdf` files.