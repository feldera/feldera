# Memory Usage

Feldera pipelines store most of their state in persistent storage (enabled by default).
To achieve high performance, pipelines use available physical memory to cache recently or
frequently accessed state and to accelerate index lookups.

For optimal performance, the system aims to fully utilize available memory without exceeding
it. This behavior is controlled by the `max_rss_mb` setting in the pipeline [Runtime configuration].
When configured, the pipeline dynamically applies *backpressure* to the storage layer, flushing
in-memory state to disk more aggressively as memory usage approaches the limit.

If `max_rss_mb` is not set but `memory_mb_max` is configured in the `resources` section of
[Runtime configuration], the latter is used as the effective memory cap. We **strongly recommend**
setting at least one of these parameters to prevent out-of-memory failures.

When either `max_rss_mb` or `resources.memory_mb_max` is configured, the pipeline reports its current
memory pressure level (`low`, `moderate`, `high`, or `critical`) via the
[`memory_pressure` metric](/operations/metrics). High or critical pressure indicates that memory usage
is nearing the configured limit and may result in significant performance degradation. In such cases,
increasing available memory is recommended.

## Pipeline memory breakdown

This section breaks down pipelines' memory usage in more detail.

- **Input** records ingested by connectors, but not yet processed by
  the circuit.

  The maximum queue length is controlled by the per-connector
  [`max_queued_records`] property.  This should be large enough to
  hide the latency of communication, but small enough to avoid wasting
  memory.  The default of 1,000,000 limits the memory used by a
  connector to 1 GB for input records that average 1 kB in size.  This
  is ordinarily a good compromise, but it can be too high if records
  are very large or if there are many input connectors.  In those
  cases, reduce the values.

  The web console shows the total number of records buffered across
  all connectors, which is also exposed as the
  [`records_input_buffered`] metric.  The number of bytes buffered is
  exposed as [`records_input_buffered_bytes`].  The web console also
  show the number of records and bytes buffered by individual connectors,
  which are also exposed as [`input_connector_buffered_records`] and
  [`input_connector_buffered_records_bytes`], respectively.

  Some input connectors can use substantial additional memory, beyond
  that needed to buffer records, for their internal operations.  This
  is particularly true of the [Kafka input connector].  These
  connectors report their additional memory use as
  [`input_connector_extra_memory_bytes`].

  [`max_queued_records`]: /connectors#max_queued_records
  [Kafka input connector]: /connectors/sources/kafka.md
  [`records_input_buffered`]: metrics.md#records_input_buffered
  [`records_input_buffered_bytes`]: metrics.md#records_input_buffered_bytes
  [`input_connector_buffered_records`]: metrics.md#input_connector_buffered_records
  [`input_connector_buffered_records_bytes`]: metrics.md#input_connector_buffered_records_bytes
  [`input_connector_extra_memory_bytes`]: metrics.md#input_connector_extra_memory_bytes

- **Output** records produced by the circuit, but not yet processed by
  connectors.  Output records can be in memory or on storage.

  As for input connectors, [`max_queued_records`] limits the maximum
  number of records buffered.  This should be large enough to avoid
  stalling the pipeline.  The value applies to output records whether
  in memory or on storage.  The default is 1,000,000.

  The number of batches of buffered output records is exposed as
  [`output_buffered_batches`], but since each batch contains a
  variable number of records, this does not directly relate to memory
  use.  The number of records buffered by individual connectors are
  exposed as [`output_connector_buffered_records`].

  [`output_buffered_batches`]: metrics.md#output_buffered_batches
  [`output_connector_buffered_records`]: metrics.md#output_connector_buffered_records

- **Cache of index batches on storage**.  When a merge flushes an
  index batch to storage, it can later be cached in memory.  A
  pipeline's memory usage is mostly independent of the size of its
  state, so that a pipeline can have a 1 TB storage footprint, but
  only use a few GB or RAM.

  The maximum size of the cache can be configured with `cache_mib`
  under `storage` in the pipeline [Runtime configuration].  The
  default is 512 MiB per worker, or 4 GiB for the default 8-worker
  configuration.  The default is usually a good choice unless a large
  number of workers would make it too large for the available memory.

  The current and maximum size of the cache are exposed as metrics
  [`storage_cache_usage_bytes`] and
  [`storage_cache_usage_limit_bytes_total`], respectively.

  [`storage_cache_usage_bytes`]: metrics.md#storage_cache_usage_bytes
  [`storage_cache_usage_limit_bytes_total`]: metrics.md#storage_cache_usage_limit_bytes_total

  [Runtime configuration]: /pipelines/configuration#runtime-configuration

- **Bloom filters** for batches on storage.  By default, these use
  approximately 19 bits of memory per key on storage (about 2.2 MiB
  per million keys).  Bloom filters stay in memory, rather than being
  part of the cache, so they can become a large cost when many records
  are in storage.

  The amount of memory used by Bloom filters is visible in circuit
  profiles.

  The number of bits per key can be tuned by setting
  `bloom_false_positive_rate` in `dev_tweaks` in the pipeline [Runtime
  configuration].  This can also be used to disable Bloom filters
  entirely.  Reducing the number of bits per key, or disabling Bloom
  filters, can reduce performance.

- **Index batches in memory**.  The pipeline’s internal state is maintained as
  a set of indexes that are continuously updated as new data is processed. Updates
  are first accumulated in in-memory batches, which are then merged into larger
  batches in the background.

  When storage is enabled (the default), batches exceeding a configured size are written
  to persistent storage. The minimum batch size for spilling to disk is controlled by the
  `min_storage_bytes` parameter under `storage` in the pipeline [Runtime configuration].
  The default value is 10 MiB, which is typically a good balance. Lowering this value can
  reduce memory usage, but may negatively impact performance.

  In addition, users can bound the pipeline’s memory usage via the
  `max_rss_mb` option described above. As memory consumption approaches this
  limit, the pipeline increasingly flushes index batches to storage to stay within the
  configured bound.

  Note that this backpressure mechanism only applies to memory used by state
  indexes and in-flight batches (see below). Other components—input/output buffers,
  Bloom filters, and caches—must be managed using the separate controls described above.

- **In-flight batches**.  As the pipeline processes a particular
  collection of input batches, it passes batches of data from one
  operator to another.  Several data batches can be in flight at any
  given time.  The number and size of these batches depends on the
  data, the number of records passed in by the input connectors, the
  SQL program, the query plan generated by Feldera's SQL compiler, and
  how Feldera schedules execution of the query plan.  Most data
  batches only live as long as it takes for them to be processed by an
  operator; they may be transformed into part of the operator's
  output, or be added to an index (see below), or be passed to an
  output connector, or simply be discarded.

  Since in-flight batches are transient, and because the pipeline
  internally breaks large batches into smaller batches, they do not
  usually become a memory problem.  If they do, one may reduce
  [`max_batch_size`] for input connectors, limiting the size of input
  batches.  Another approach is to set `min_step_storage_bytes` in
  `storage` in the pipeline [Runtime configuration], to force
  in-flight batches to storage, although this is likely to reduce
  performance.

  [`max_batch_size`]: /connectors#max_batch_size

## Summary of pipeline's memory usage

| Component | Description | Metrics | Control knobs |
| --- | --- | --- | --- |
| Input buffering | Records ingested by input connectors but not yet processed by the circuit. | [`records_input_buffered`], [`records_input_buffered_bytes`], [`input_connector_buffered_records`], [`input_connector_buffered_records_bytes`], [`input_connector_extra_memory_bytes`] | Connector [`max_queued_records`] |
| Output buffering | Records produced by the circuit but not yet consumed by output connectors; can be buffered in memory or storage. | [`output_buffered_batches`], [`output_connector_buffered_records`] | Connector [`max_queued_records`] |
| Index batches in memory | Index batches kept in memory before background merges and eventual flush to storage. |  | Runtime config `storage.min_storage_bytes`, [`max_rss_mb`](#max_rss) |
| Storage cache | In-memory cache of index batches stored on disk; decouples working memory from total state size. | [`storage_cache_usage_bytes`], [`storage_cache_usage_limit_bytes_total`] | Runtime config `storage.cache_mib` |
| Bloom filters | In-memory bloom filters for batches on storage; can become significant at large state sizes. | Visible in circuit profiles. | Runtime config `dev_tweaks.bloom_false_positive_rate` |
| In-flight batches | Transient batches moving between operators during execution; usually short-lived but workload-dependent. |  | Connector [`max_batch_size`], runtime config `storage.min_step_storage_bytes` |


<!-- ## Configuring available memory

When using Feldera Enterprise in the Kubernetes environment, the amount of memory available to
the pipeline can be configured via the [`values.yaml` file](/get-started/enterprise/helm-guide)
or via the `resources` section in the pipeline config, e.g.:

```json
"resources": {
    "memory_mb_min": 32000,
    "memory_mb_max": 32000
}
```
 -->
<!-- ## Bounding pipeline's memory footprint `max_rss_mb`
<a id="max_rss"></a>

Memory not used for input/output buffers, Bloom filters, and caches is available for temporary storage
of the pipeline’s state indexes and in-flight batches. For optimal performance, the system aims to utilize
as much of this memory as possible without exceeding available capacity.

This behavior is controlled via the `max_rss_mb` setting in the pipeline [Runtime configuration]. When set,
the pipeline dynamically applies *backpressure* to the storage layer, flushing in-memory batches to disk more
aggressively as memory usage approaches the configured limit.

If `max_rss_mb` is not specified but `resources.memory_mb_max`
is set, the latter is used as the effective
memory cap for the pipeline. We **strongly recommend** configuring at least one of these settings to avoid
out-of-memory failures.

Note that the backpressure mechanism described here only applies to memory used by state indexes and in-flight
batches. Other components—input/output buffers, Bloom filters, and caches—must be managed using the separate
controls described above.

When either `max_rss_mb` or `resources.memory_mb_max` is configured, the pipeline exposes its current memory
pressure level (low, moderate, high, or critical) via the [`memory_pressure` metric](/operations/metrics). High
or critical pressure indicates that memory usage is approaching the configured limit and may lead to significant
performance degradation. In such cases, we recommend increasing the available memory. -->

## Advanced Usage

### jemalloc Configuration

You can fine-tune jemalloc with the `MALLOC_CONF` environment variable.
Possible values are documented in the
[jemalloc reference](https://jemalloc.net/jemalloc.3.html#tuning).

Feldera sets default `MALLOC_CONF` values to collect heap profile data:
`prof:true,prof_active:true,lg_prof_sample:19`.
When setting custom configuration, preserve these defaults and add your
own options.

Environment variables can be overridden by setting them in the pipeline runtime configuration in `env`:

```json
// ...
"env": {
  "MALLOC_CONF": "prof:true,prof_active:true,lg_prof_sample:19,background_thread:true,stats_print:true"
}
// ...
```

Some environment variable names are reserved and cannot be overridden through
`runtime_config.env` (for example names in the `FELDERA_`, `KUBERNETES_`, and
`TOKIO_` namespaces, plus control variables such as `RUST_LOG`).

Python SDK example:

```python
from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig

pipeline = PipelineBuilder(
    client,
    name="my-pipeline",
    sql="CREATE TABLE t(i INT);",
    runtime_config=RuntimeConfig(
        env={
            "MALLOC_CONF": "prof:true,prof_active:true,lg_prof_sample:19,background_thread:true,stats_print:true"
        }
    ),
).create_or_replace()
```
