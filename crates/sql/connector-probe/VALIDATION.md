# Java validation rule inventory

Source: `compiler/frontend/connectors/config` in the SQL compiler. The generated
`java-inventory.json` records all 29 classes, 239 public fields, wire names,
defaults, enum alternatives, and flattened-key behavior. The runner regenerates
it from Java classes and rejects drift. These semantic checks run after successful
Jackson decoding; decoding stops at the first failure.

| Class | Semantic checks, in emission order |
|---|---|
| AvroEncoderConfig | Registry duration and legacy alias; CDC field requires raw format; token excludes username/password. |
| AvroParserConfig | Registry duration and alias; registry URL list cannot be null; schema excludes a nonempty URL list; token excludes username/password. |
| ClockConfig | Resolution and legacy microseconds; minimum one nanosecond. |
| CsvEncoderConfig | Delimiter must be ASCII. |
| CsvParserConfig | Delimiter, quote, escape, comment must be ASCII when present; only if these pass, delimiter and quote must differ. |
| DeltaTableReaderConfig | Missing mode returns immediately; version excludes datetime; snapshot filter restricted to snapshot modes; CDC delete/order fields restricted to CDC; nonempty URI checked only if preceding checks passed. |
| DeltaTableWriterConfig | Threads cannot be zero; retention uses exactly `interval N unit`, lowercase prefix, nonnegative signed-long count, supported singular/plural time unit. |
| DynamoDBWriterConfig | Table nonempty, 3–255 ASCII identifier characters; region nonempty; batch size bounded by mode; nonzero threads; required buffer size at least one byte; nonzero concurrent requests; access key and secret supplied together. |
| FileInputConfig | Buffer size and legacy byte alias. |
| FileOutputConfig | No additional checks. |
| HttpOutputConfig | No additional checks. |
| IcebergReaderConfig | Missing mode returns immediately; snapshot id excludes datetime; catalog versus metadata location; table name requires catalog and vice versa; required Glue/REST/S3 Tables entry and rejection of fields for other catalogs, in that order. |
| JsonEncoderConfig | Key fields require Debezium, returning immediately on failure; tombstones require Debezium, likewise returning. Outer validation also requires an index when tombstones is written. |
| JsonParserConfig | No additional checks. |
| KafkaInputConfig | Group join duration and alias; choose topic or legacy topics; neither may be empty together; both are mutually exclusive; legacy list must contain exactly one element. |
| KafkaOutputConfig | Initialization duration and alias; topic nonempty. |
| NatsInputConfig | Stream name nonempty; inactivity/retry durations and aliases, each at least one second; connection object required, server URL nonempty, nested durations; consumer object and deliver policy required, nested durations. |
| ParquetEncoderConfig | No additional checks. |
| ParquetParserConfig | No additional checks. |
| PostgresCdcReaderConfig | URI, publication, source table nonempty, short-circuiting this sequence; batch memory ratio in (0,1]; required batch size at least one byte; activation threshold in (0,1], resume threshold in [0,1), resume below activation. |
| PostgresReaderConfig | URI then query nonempty, stopping at the first failure. |
| PostgresWriterConfig | Required buffer size at least one byte; CDC column names nonempty and ASCII in CDC mode, conflict-ignore forbidden; nondefault nonblank CDC names forbidden in materialized mode; nonzero threads. |
| PubSubInputConfig | Timeout and connection timeout, each with legacy alias; snapshot excludes timestamp. |
| RawParserConfig | No additional checks. |
| RedisOutputConfig | No additional checks. |
| S2InputConfig | Basin, stream, token nonempty; start position is Beginning/Tail or exactly one supported tagged nonnegative signed-long integer. |
| S2OutputConfig | Basin, stream, token nonempty. |
| S3InputConfig | Region then bucket nonempty. |
| UrlInputConfig | Pause linger and its pause-timeout alias. |

Quantity helpers distinguish missing values from explicitly written nulls. Two
spellings conflict even when both are null. Optional quantities accept one null;
required quantities warn. Bare integral nonnegative numbers, including whole
floating values, are bounded by `u64::MAX`. Duration strings accept chained terms,
round each fractional term to nanoseconds, and enforce the full `Duration` range.
Size strings use the analogous byte-unit parser. The prototype implements duration
checks used by its six classes; it does not implement all quantity users.

The outer validator checks object shapes, required string names, direction, and
config object presence before dispatch. Omitted config blocks return without
validating the class. SET suppression runs before promotion. Full SQL differential
fixtures exercise the actual outer path for the six selected classes and all four
policy combinations.

The other 23 classes are inventoried, not covered by the Rust prototype. Several
still dereference nullable Jackson values without guards (for example DynamoDB
table/region, NATS server URL, Postgres CDC nested objects, and Postgres output
mode). These require generated regressions before extending the oracle population;
their crashes must not become expected rejections.
