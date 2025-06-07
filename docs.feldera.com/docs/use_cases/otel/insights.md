# Extracting High-Level Metrics

In the previous step, we pre-processed the data to make it easier to analyze in SQL. Next, we want to generate valuable insights from it.

### P95 Latency

P95 latency is a common statistical measure used for network performance analysis.
It means that 95% of requests were served faster than this value, while 5% took longer than this.

#### Step 1: Define a Tumbling Window

To compute P95 latency over time, we first define a **10-second tumbling window** on the `spans` view
we defined previously.

```sql
-- continuation of the SQL program from the previous page

CREATE LOCAL VIEW spans_tumble_10s AS
SELECT * FROM TABLE(
	TUMBLE(
		TABLE spans,
		DESCRIPTOR(eventTime),
		INTERVAL '10' SECOND
	)
);
```

This groups spans into non-overlapping 10-second time windows.

#### Step 2: Define a Rust UDF for P95 Calculation

Typically, the SQL function `PERCENTILE` can be used to calculate the P95 latency. However, Feldera SQL
doesn't support it yet. This presents us an opportunity to use a Rust [UDFs](https://docs.feldera.com/sql/udf).
This UDF will be a function that given an array of integers, returns the 95th percentile value from it.

##### SQL Definition for P95 Rust UDF

```sql
-- UDF to calculate p95 value given an integer array
CREATE FUNCTION p95(x BIGINT ARRAY NOT NULL) RETURNS BIGINT;
```

##### Rust Implementation of P95

```rust
// udf.rs

pub fn p95(x: Vec<Option<i64>>) -> Result<Option<i64>, Box<dyn std::error::Error>> {
	let mut x: Vec<i64> = x.into_iter().filter_map(|x| x).collect();

	if x.is_empty() {
		return Ok(None);
	}

	x.sort_unstable();

	let rank = 0.95 * x.len() as f64 - 1.0;
	let lower = rank.floor() as usize;
	let upper = rank.ceil() as usize;

	if lower == upper {
		Ok(Some(x[lower]))
	} else {
		let weight = rank - lower as f64;
		Ok(Some((x[lower] as f64 * (1.0 - weight) + x[upper] as f64 * weight) as i64))
	}
}
```

This function:
1. Filters out null values and sorts the values.
2. Finds the 95th percentile index.
3. Interpolates and returns the P95 value.

#### Step 3: Compute P95 Latency per 10 Second Window

```sql
-- Calculate the p95 latency in milliseconds
CREATE MATERIALIZED VIEW p95_latency AS
SELECT
	p95(array_agg(elapsedTimeMillis)) as latencyMs,
	window_start as 'time'
FROM spans_tumble_10s
WHERE
	parentSpanId = '' -- Only consider top-level requests
GROUP BY
	window_start;
```

This filters only the top level traces and aggregates `elapsedTimeMillis` values into an array and calls the `p95` function.

### Throughput

Throughput measures the number of top-level requests processed per second.
In this demo however, we calculate throughput on 10 second time buckets instead of per second.

```sql
CREATE MATERIALIZED VIEW throughput AS
SELECT
	COUNT(*) as throughput,
	window_start as 'time'
FROM
	spans_tumble_10s
WHERE
	parentSpanId = ''
GROUP BY
	window_start;
```

### Operation Execution Time

Operation execution time measures how long each operation actually took, excluding time spent in child spans.

```sql
CREATE MATERIALIZED VIEW operation_execution_time as
SELECT
    s.name,
    SUM(
        s.elapsedTimeMillis -
        coalesce(
            select
                sum(elapsedTimeMillis)
                FROM spans k
                WHERE k.traceId = s.traceId
                AND k.parentSpanId = s.spanId,
            0
        )
    ) as elapsed
FROM spans s
GROUP BY s.name;
```

Explanation:
4. `s.elapsedTimeMillis`: Total execution time of the operation.
5. Subtracts time spent in child spans to compute actual execution time.
6. Groups results by operation name `s.name`.
