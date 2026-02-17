# Memory Usage

Feldera pipelines primarily use memory for the following purposes:

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
  exposed as [`records_input_buffered_bytes`].  The number of records
  and bytes buffered by individual connectors are exposed as
  [`input_connector_buffered_records`] and
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

- **Index batches in memory**.  The pipeline initially adds batches of
  records to its in-memory indexes and then merges them into larger
  batches in the background.  When a merged batch is large enough,
  it is written to storage.  The minimum size to write a batch to
  disk is configurable as `min_storage_bytes` under `storage` in the
  pipeline [Runtime configuration].  The default is 10 MiB, which is
  usually a good choice.  Configuring a smaller value may save memory
  but at a performance cost.

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

## Advanced Usage

### jemalloc Configuration

Users can fine-tune the performance of jemalloc with the `MALLOC_CONF` environment variable. Possible configuration values are detailed in the [jemalloc reference](https://jemalloc.net/jemalloc.3.html#tuning). [Feldera sets default `MALLOC_CONF`](https://github.com/feldera/feldera/blob/5bcdf945525c87cbc22d4036609455d8159e029b/sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/backend/rust/BaseRustCodeGenerator.java#L102) values to collect heap profile data. values to collect heap profile data. These defaults are prof:true,prof_active:true,lg_prof_sample:19. When setting custom configuration, preserve Feldera's defaults by taking the union of both sets of values.

- **With Docker**: [Docker Quickstart](/get-started/docker#docker-quickstart) users can set the `MALLOC_CONF` environment variable by passing `-e MALLOC_CONF='prof:true,prof_active:true,lg_prof_sample:19,custom_config_value:true'` where `'custom_config_value:true'` is replaced with the values the user wants to configure.

  If using [Docker Compose](/get-started/docker#optional-docker-compose-quickstart), customize the pipeline manager environment.
  ```yaml
  services:
    pipeline-manager:
      # ...
      environment:
        # prof, prof_active, and lg_prof_sample are Feldera defaults that should be preserved
        MALLOC_CONF: prof:true,prof_active:true,lg_prof_sample:19,background_thread:true,stats_print:true
  ```

- **With Helm (Enterprise Users Only)**: Enterprise users can add the `MALLOC_CONF` environment variable to the `values.yaml` file.

  Example usage:
  ```yaml
  pipeline:
    env:
     - name: MALLOC_CONF
     # prof, prof_active, and lg_prof_sample are Feldera defaults that should be preserved
       value: prof:true,prof_active:true,lg_prof_sample:19,background_thread:true,stats_print:true
  ```
