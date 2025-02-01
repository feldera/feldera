# Ingress

To push data into Feldera, we configure the OpenTelemetry (OTel) Collector to send data to Feldera via HTTP.

#### OTel Collector Configuration:

```yml
exporters:

  # Send traces to the Feldera pipeline.
  # Currently only uncompressed JSON is supported.
  otlphttp/feldera_traces:
     traces_endpoint: http://feldera:8080/v0/pipelines/otel/ingress/otel_traces?format=json&update_format=raw
     encoding: json
     compression: none

  otlphttp/feldera_logs:
     logs_endpoint: http://feldera:8080/v0/pipelines/otel/ingress/otel_logs?format=json&update_format=raw
     encoding: json
     compression: none

  otlphttp/feldera_metrics:
     metrics_endpoint: http://feldera:8080/v0/pipelines/otel/ingress/otel_metrics?format=json&update_format=raw
     encoding: json
     compression: none

service:
   pipelines:
     traces:
       exporters: [spanmetrics, otlphttp/feldera_traces]

     logs:
       receivers: [otlp]
       exporters: [otlphttp/feldera_logs]

     metrics:
       receivers: [hostmetrics, docker_stats, httpcheck/frontend-proxy, otlp, prometheus, redis, spanmetrics]
       exporters: [otlphttp/feldera_metrics]
```

Notice that we have the query params: `format=json&update_format=raw`
- `format=json`: Specifies that the input data is JSON-formatted.
- `update_format=raw`: Indicates that the request body contains raw data, which is processed as an insertion into the table.

## Preprocessing

Next we perform a series of `UNNEST` operations to extract individual rows of metrics, logs and traces from a nested arrays of them.

#### Step 1: Extract Resource-Level Data

```sql
-- (ResouceMetrics[N]) -> (Resource, ScopeMetrics[N])
CREATE LOCAL VIEW rsMetrics AS SELECT resource, scopeMetrics
FROM otel_metrics, UNNEST(resourceMetrics) as t (resource, scopeMetrics);

-- (ResouceSpans[N]) -> (Resource, ScopeSpans[N])
CREATE LOCAL VIEW rsSpans AS SELECT resource, scopeSpans
FROM otel_traces, UNNEST(resourceSpans) as t (resource, scopeSpans);

-- (ResouceLogs[N]) -> (Resource, ScopeLogs[N])
CREATE LOCAL VIEW rsLogs AS SELECT resource, scopeLogs
FROM otel_logs, UNNEST(resourceLogs) as t (resource, scopeLogs);
```

Here we `UNNEST`  arrays of `ResourceSpans`, `ResourceMetrics` and `ResourceLogs` into separate rows.

#### Step 2: Extract Scope-Level Data

```sql
-- (ScopeMetrics[N]) -> (ScopeMetrics) x N
CREATE LOCAL VIEW metrics_array AS
SELECT metrics From rsMetrics, UNNEST(rsMetrics.scopeMetrics) as t(_, metrics);

-- (ScopeLogs[N]) -> (ScopeLogs) x N
CREATE LOCAL VIEW logs_array AS
SELECT logs FROM rsLogs, UNNEST(rsLogs.scopeLogs) as t(_, logs);

-- (ScopeSpans[N]) -> (ScopeSpans) x N
CREATE LOCAL VIEW spans_array AS
SELECT spans FROM rsSpans, UNNEST(rsSpans.scopeSpans) as t(_, spans);
```

Similarly, we `UNNEST` the `ScopeMetrics`, `ScopeLogs` and `ScopeSpans` into separate rows.

#### Step 3: Extract Final Metrics and Logs

```sql
-- (Metrics[N]) -> (_, Metric) x N
CREATE MATERIALIZED VIEW metrics AS
SELECT
	name,
	description,
	unit,
	data,
	metadata
FROM metrics_array, UNNEST(metrics_array.metrics);

-- (Logs[N]) -> (_, Logs) x N
CREATE MATERIALIZED VIEW logs AS
SELECT
	attributes,
	timeUnixNano,
	observedTimeUnixNano,
	severityNumber,
	severityText,
	flags,
	traceId,
	spanId,
	eventName,
	body
FROM logs_array, UNNEST(logs_array.logs);
```

Finally, we extract individual `Metric` and `LogRecords` to extract individual records.

#### Step 4: Process Spans and Add Derived Fields

For spans, we not only extract individual records but also compute useful derived fields:
- `elapsedTimeMillis`: The duration of the span in milliseconds.
- `eventTime`: Timestamp for when the span started. Note that in Feldera, the `TIMESTAMP` type doesn't include time zone information.

```sql
-- Convert nanoseconds to seconds
CREATE FUNCTION NANOS_TO_SECONDS(NANOS BIGINT) RETURNS BIGINT AS
(NANOS / 1000000000::BIGINT);

-- Convert nanoseconds to milliseconds
CREATE FUNCTION NANOS_TO_MILLIS(NANOS BIGINT) RETURNS BIGINT AS
(NANOS / 1000000::BIGINT);

-- Convert to TIMESTAMP type from a BIGINT that represents time in nanoseconds
CREATE FUNCTION MAKE_TIMESTAMP_FROM_NANOS(NANOS BIGINT) RETURNS TIMESTAMP AS
TIMESTAMPADD(SECOND, NANOS_TO_SECONDS(NANOS), DATE '1970-01-01');

-- (Spans[N]) -> (Span, elapsedTimeMillis, eventTime) x N
CREATE MATERIALIZED VIEW spans AS
SELECT
	traceId,
	spanId,
	tracestate,
	parentSpanId,
	flags,
	name,
	kind,
	startTimeUnixNano,
	endTimeUnixNano,
	attributes,
	events,
	NANOS_TO_MILLIS(endTimeUnixNano::BIGINT - startTimeUnixNano::BIGINT) as elapsedTimeMillis,
MAKE_TIMESTAMP_FROM_NANOS(startTimeUnixNano) as eventTime
FROM spans_array, UNNEST(spans_array.spans);
```

