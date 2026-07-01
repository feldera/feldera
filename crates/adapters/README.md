# Feldera I/O adapter framework

This crate implements an infrastructure to ingest data into a DBSP
circuit from external data sources and to stream the outputs of the
circuit to external consumers. It also implements a Feldera I/O
controller that controls the execution of a DBSP circuit along with
its input and output adapters, and a server that exposes the
controller API over HTTP and through a web interface.

## Dependencies

The test code has the following dependencies:

- `cmake`:
  > $ sudo apt install cmake

- `redpanda`:

  On Debian or Ubuntu:

  ```sh
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash
  sudo apt install redpanda -y
  sudo systemctl start redpanda
  ```

  Or with Docker:

  ```sh
  docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v24.2.4 redpanda start --smp 2
  ```

- `NATS`:

  The tests for the NATS input connector expect the binary `nats-server` to be available.

  To install on Debian or Ubuntu:

  ```sh
  sudo apt install nats-server -y
  ```

## DBSP application server demo

This directory also contains a demo application runnign a very simple
DBSP pipeline as a service. The service can be controlled using a web
browser. To run the demo you can execute the following command:

```
$ cargo run --example server --features="with-kafka server"
```

Then open a web browser and open the following URL: `http://localhost:8080`

## Connector configuration validation

The SQL compiler validates connector configurations at compile time, reporting
warnings for unknown fields and invalid values before the pipeline ever runs.
This section explains how the validation works and what to do when you add or
change a Rust connector config struct.

### Architecture

The validation lives in the SQL compiler:

```
sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/compiler/frontend/connectors/
├── ConnectorValidator.java   — dispatch: routes format/transport names to POJO classes
├── ConfigReporter.java       — emits positioned warnings via JSON Pointer lookup
├── IValidateConfig.java      — interface: boolean validate(ConfigReporter)
└── config/                   — one POJO class (and helper enums) per Rust config struct
    ├── CsvParserConfig.java
    ├── KafkaInputConfig.java
    └── ...
```

`ConnectorValidator.validateFormatConfig()` and `validateTransportConfig()` are called
once per connector during SQL compilation.  Each method looks at the transport/format
name and deserializes the JSON `config` block into the matching POJO class.  Unknown
fields produce a warning pointing at the exact source location of the typo.  After
deserialization the POJO's `validate()` method checks cross-field constraints.

By default every unknown field is rejected.  A POJO opts out of that check for specific
keys by declaring a `@JsonAnySetter` method:

```java
/** Absorbs flattened extra keys so they are not rejected as unknown. */
@JsonAnySetter
public void setExtra(String key, Object value) {}
```

Jackson calls the method for unknown keys instead of failing.  Since the
method body is empty the key is silently absorbed.  This is used for configs that have
a `#[serde(flatten)] HashMap<String, String>` in Rust (e.g. Kafka, Delta Table, Iceberg),
where any string key is valid and should not be reported as an error.

### Currently validated connectors

#### Format (input)
| Name       | POJO class            |
|------------|-----------------------|
| `avro`     | `AvroParserConfig`    |
| `csv`      | `CsvParserConfig`     |
| `json`     | `JsonParserConfig`    |
| `parquet`  | `ParquetParserConfig` |
| `raw`      | `RawParserConfig`     |

#### Format (output)
| Name       | POJO class             |
|------------|------------------------|
| `avro`     | `AvroEncoderConfig`    |
| `csv`      | `CsvEncoderConfig`     |
| `json`     | `JsonEncoderConfig`    |
| `parquet`  | `ParquetEncoderConfig` |

#### Transport (input)
| Name                 | POJO class                |
|----------------------|---------------------------|
| `clock`              | `ClockConfig`             |
| `delta_table_input`  | `DeltaTableReaderConfig`  |
| `file_input`         | `FileInputConfig`         |
| `iceberg_input`      | `IcebergReaderConfig`     |
| `kafka_input`        | `KafkaInputConfig`        |
| `nats_input`         | `NatsInputConfig`         |
| `rabbitmq_input`      | `RabbitmqInputConfig`     |
| `postgres_input`     | `PostgresReaderConfig`    |
| `postgres_cdc_input` | `PostgresCdcReaderConfig` |
| `pub_sub_input`      | `PubSubInputConfig`       |
| `s3_input`           | `S3InputConfig`           |
| `url_input`          | `UrlInputConfig`          |

#### Transport (output)
| Name                 | POJO class               |
|----------------------|--------------------------|
| `delta_table_output` | `DeltaTableWriterConfig` |
| `dynamodb_output`    | `DynamoDBWriterConfig`   |
| `file_output`        | `FileOutputConfig`       |
| `http_output`        | `HttpOutputConfig`       |
| `kafka_output`       | `KafkaOutputConfig`      |
| `postgres_output`    | `PostgresWriterConfig`   |
| `redis_output`       | `RedisOutputConfig`      |
| `rabbitmq_output`    | `RabbitmqOutputConfig`   |

Not validated by the SQL compiler: `datagen`, `nexmark`.

Note: flattened string-map configs (e.g., `kafka_options`) cannot be
validated to detect typos.

#### Data-carrying enum variants

Some Rust enums have variants that carry data, e.g.:

```rust
pub enum DeliverPolicy {
    All,
    Last,
    ByStartSequence { start_sequence: u64 },
    ByStartTime { start_time: OffsetDateTime },
}
```

Rust's default serde serialization is **externally tagged**: unit variants become
plain strings (`"All"`), struct variants become single-key objects
(`{"ByStartSequence": {"start_sequence": 5}}`).  Jackson has no direct equivalent
without a custom deserializer.  The recommended approach is to declare the field
as `JsonNode` in the Java POJO and skip content validation:

```java
/** Required. Unit variants ("All", "Last", ...) and struct variants
 *  ({"ByStartSequence": {...}}) are accepted as-is. */
@Nullable
@JsonProperty("deliver_policy")
public JsonNode deliverPolicy = null;
```

Presence can still be checked in `validate()` via
`deliverPolicy == null || deliverPolicy.isNull()`.

### How to update the Java code

The Java compiler validates connector configuration, but only gives
warnings.  The ultimate real validation is done by the connector code
itself, in Rust, at runtime.

#### When you add a field to an existing Rust struct

Find the matching POJO class in `config/` and add the field following the
mapping rules below.

#### When you add a new Rust config struct

1. **Create a POJO class** in `config/` implementing `IValidateConfig`:

   ```java
   package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

   import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
   import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;
   import com.fasterxml.jackson.annotation.JsonProperty;
   import javax.annotation.Nullable;

   public class MyTransportConfig implements IValidateConfig {
       @JsonProperty("some_field")
       public String someField = "";

       @Override
       public boolean validate(ConfigReporter reporter) {
           boolean ok = true;
           if (someField.isBlank()) {
               reporter.warnPath("some_field", "Invalid configuration",
                       "required field \"some_field\" is missing or empty");
               ok = false;
           }
           return ok;
       }
   }
   ```

2. **Wire it** in `ConnectorValidator.validateTransportConfig()` (or
   `validateFormatConfig()` for formats):

   ```java
   case "my_transport_input":
       validateConfig(transportConfig, outerJson, configPointer,
               outerStart, MyTransportConfig.class, reporter);
       break;
   ```

3. **Add tests** in `ConnectorTests.java` using the `tableConnectorTest()` /
   `viewConnectorTest()` helpers.

#### When you rename or remove a field

Update the `@JsonProperty` annotation (or remove the field from the POJO).
If the old name should still be accepted as a fallback, add `@JsonAlias("old_name")`.

#### When you add a new enum variant

Add the variant to the Java enum with a matching `@JsonProperty`.  If the Rust
variant has `#[serde(skip)]`, omit it from the Java enum entirely.

### Rust-to-Java field mapping

| Rust                                              | Java                                                              |
|---------------------------------------------------|-------------------------------------------------------------------|
| `field: String`                                   | `@JsonProperty("field") public String field = "";`                |
| `field: Option<String>`                           | `@Nullable @JsonProperty("field") public String field = null;`    |
| `flag: bool` (default false)                      | `@JsonProperty("flag") public boolean flag = false;`              |
| `#[serde(default = "f")] n: u32`                  | `@JsonProperty("n") public int n = /* value of f() */;`           |
| `#[serde(rename = "x")] field: T`                 | `@JsonProperty("x") public T field = ...;`                        |
| `#[serde(alias = "old")] field: T`                | `@JsonAlias("old") @JsonProperty("new") public T field;`          |
| `#[serde(flatten)] extra: HashMap<String, String>`| `@JsonAnySetter public void set(String k, Object v) {}`           |
| `#[serde(flatten)] inner: InnerStruct`            | Inline all fields of `InnerStruct` directly (see below)           |
| enum variant `#[serde(rename = "x")] Foo`         | `@JsonProperty("x") Foo,`                                         |
| enum variant `#[serde(skip)] Bar`                 | Omit from the Java enum                                           |

#### Java field initializers

Java field initializers in POJO classes serve **validation logic only** — they are
not used by the runtime, which has its own Rust defaults.  Set an initializer only
when `validate()` depends on it:

- `String field = ""` — sentinel for a required field; `validate()` checks
  `field.isBlank()` to detect omission.
- `int n = k` — when `validate()` checks a range or relationship involving `n` and
  the Rust default `k` is the boundary (e.g. `n == 0` is invalid but the Rust
  default is `1`, so the initializer must be `1` for the check to fire correctly on
  omission).

Do **not** copy Rust defaults just for documentation — the Rust and Java values are
not kept in sync automatically, so a stale initializer is worse than no initializer.
For fields that `validate()` never inspects, omit the initializer or use the Java
natural default (`0`, `false`, `null`).

#### Flattened structs

Copy all fields from the nested Rust struct directly into the Java POJO class:

```rust
// Rust
pub struct OuterConfig {
    pub name: String,
    #[serde(flatten)]
    pub tls: TlsConfig,   // has fields ssl_ca_pem, verify_hostname, ...
}
```

```java
// Java — inline the TLS fields directly
public class OuterConfig implements IValidateConfig {
    @JsonProperty("name")                       public String name = "";
    // TLS fields (inlined from TlsConfig)
    @Nullable @JsonProperty("ssl_ca_pem")       public String sslCaPem = null;
    @Nullable @JsonProperty("verify_hostname")  public Boolean verifyHostname = null;
    // ...
}
```

### Emitting warnings from `validate()`

```java
// Point at a specific field's key in the source
reporter.warnPath("field_name", "Invalid configuration", "message");

// Point at the whole config block (when the error isn't tied to one field)
reporter.warn("Invalid configuration", "message");
```

`warnPath` accepts a slash-separated JSON Pointer suffix relative to the config
block, e.g. `"field"` or `"nested/subfield"`.

### Running the tests

```bash
cd sql-to-dbsp-compiler/SQL-compiler
mvn test -Dtest=ConnectorTests
```
