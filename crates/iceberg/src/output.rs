//! Engine-independent core of the Iceberg output connector: schema mapping, data file
//! writers, and idempotent commits.

use crate::catalog::{apply_fileio_defaults, CatalogConfig};
use anyhow::{anyhow, bail, Context, Error as AnyError, Result as AnyResult};
use async_trait::async_trait;
use datafusion::arrow::array::{
    new_null_array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use datafusion::arrow::compute::{cast, take};
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use feldera_types::serde_with_context::serde_config::{
    BinaryFormat, DecimalFormat, UuidFormat, VariantFormat,
};
use feldera_types::serde_with_context::{DateFormat, SqlSerdeConfig, TimeFormat, TimestampFormat};
use feldera_types::transport::iceberg::{
    IcebergCatalogType, IcebergWriteMode, IcebergWriterConfig,
};
use futures_util::future::BoxFuture;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataFile, DataFileFormat, ListType, Literal, MapType, NestedField, NestedFieldRef,
    PartitionKey, PrimitiveLiteral, PrimitiveType, Schema, SchemaRef, Struct, StructType,
    TableMetadata, TableProperties, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::{DataFileWriter, DataFileWriterBuilder};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
    ObjectStorageLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

/// Change log column: `i`, `d`, or `u`.
pub const OP_COLUMN: &str = "__feldera_op";

/// Change log column: batch time in microseconds since the epoch.
pub const TS_COLUMN: &str = "__feldera_ts";

/// Snapshot summary key that names the batch a snapshot commits.
const COMMIT_ID_PROPERTY: &str = "feldera.commit-id";

/// Table property that names the open that created the table.
const CREATE_ID_PROPERTY: &str = "feldera.create-id";

/// Lets a catalog remove old `metadata.json` files. Each commit writes one.
const DELETE_AFTER_COMMIT_PROPERTY: &str = "write.metadata.delete-after-commit.enabled";

/// The Parquet writer buffers a whole row group in memory.
const MAX_ROW_GROUP_BYTES: usize = 16 * 1024 * 1024;

/// Rows per Parquet write. The writer checks sizes only between writes.
const WRITE_BATCH_ROWS: usize = 1024;

/// The iceberg-rust catalog clients have no request timeout.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(120);

/// The REST client never refreshes its OAuth2 token. Tokens can expire after 5 minutes.
const CATALOG_MAX_AGE: Duration = Duration::from_secs(4 * 60);

/// Encodes each SQL value in the Arrow type and unit of its Iceberg type.
pub fn iceberg_output_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig {
        timestamp_format: TimestampFormat::MicrosSinceEpoch,
        timestamp_tz_format: TimestampFormat::MicrosSinceEpoch,
        // Time64(us) takes i64.
        time_format: TimeFormat::MicrosSigned,
        date_format: DateFormat::DaysSinceEpoch,
        decimal_format: DecimalFormat::String,
        variant_format: VariantFormat::JsonString,
        binary_format: BinaryFormat::Array,
        uuid_format: UuidFormat::Binary,
        positional_fields: false,
        absent_columns: None,
    }
}

/// Sink error, by how the caller recovers.
#[derive(Debug)]
pub enum SinkError {
    /// Retry.
    Transient(AnyError),
    /// A retry fails the same way.
    Permanent(AnyError),
    /// The table changed under the batch. Write the batch again.
    TableChanged(AnyError),
    /// The table does not fit the view. Its owner can change it to fit.
    Unfit(AnyError),
}

impl SinkError {
    pub fn into_inner(self) -> AnyError {
        match self {
            Self::Transient(e) | Self::Permanent(e) | Self::TableChanged(e) | Self::Unfit(e) => e,
        }
    }

    /// Transient unless the library cannot handle the table: a server can report a temporary
    /// fault with any kind, for example `DataInvalid` from its token endpoint.
    fn iceberg(context: &str, e: iceberg::Error) -> Self {
        let permanent = matches!(
            e.kind(),
            // For example, an encrypted table.
            ErrorKind::PreconditionFailed | ErrorKind::FeatureUnsupported
        );
        let e = anyhow!("{context}: {e}");
        if permanent {
            Self::Permanent(e)
        } else {
            Self::Transient(e)
        }
    }
}

/// Always transient, so that a failed write is retried and not dropped at once.
fn write_error(context: &str, e: iceberg::Error) -> SinkError {
    SinkError::Transient(anyhow!("{context}: {e}"))
}

/// Where a table column gets its values.
#[derive(Debug, Clone)]
enum Source {
    /// View column at this index.
    Row(usize),
    Op,
    Ts,
    Null,
    /// One-row array with the column's write default.
    Default(ArrayRef),
}

type FileWriter = DataFileWriter<ParquetWriterBuilder, Location, DefaultFileNameGenerator>;

/// Data file paths, in the layout that `write.object-storage.enabled` selects.
#[derive(Clone, Debug)]
enum Location {
    Default(DefaultLocationGenerator),
    /// Hashed directories, which spread the files over object store prefixes.
    ObjectStorage(ObjectStorageLocationGenerator),
}

impl Location {
    fn new(metadata: &TableMetadata) -> iceberg::Result<Self> {
        Ok(
            if metadata.table_properties().write_object_storage_enabled()? {
                Self::ObjectStorage(ObjectStorageLocationGenerator::new(metadata)?)
            } else {
                Self::Default(DefaultLocationGenerator::new(metadata)?)
            },
        )
    }
}

impl LocationGenerator for Location {
    fn generate_location(&self, key: Option<&PartitionKey>, file_name: &str) -> String {
        match self {
            Self::Default(location) => location.generate_location(key, file_name),
            Self::ObjectStorage(location) => location.generate_location(key, file_name),
        }
    }
}

/// Maps view rows to the table, as loaded for one batch.
pub struct Layout {
    table: Table,
    schema: SchemaRef,
    arrow_schema: ArrowSchemaRef,
    /// View columns, with the table's types.
    row_schema: ArrowSchemaRef,
    /// One per table column.
    sources: Vec<Source>,
}

impl Layout {
    fn new(table: &Table, relation: &Relation) -> AnyResult<Self> {
        let schema = table.metadata().current_schema().clone();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let columns = schema.as_struct().fields();

        let mut sources = vec![Source::Null; columns.len()];
        let mut row_fields = Vec::with_capacity(relation.fields.len());
        for (position, expected) in iceberg_fields(relation)?.iter().enumerate() {
            let name = expected.name.as_str();
            let Some(index) = columns.iter().position(|f| f.name == name) else {
                let names: Vec<_> = columns.iter().map(|f| f.name.as_str()).collect();
                bail!(
                    "the table has no column '{name}'. Its columns are: {}",
                    names.join(", ")
                );
            };
            let actual = &columns[index];
            sources[index] = match name {
                OP_COLUMN => Source::Op,
                TS_COLUMN => Source::Ts,
                _ => Source::Row(position),
            };
            if let Source::Row(_) = sources[index] {
                check_field(name, expected, actual)?;
                row_fields.push(arrow_schema.field(index).clone());
            } else {
                // Never NULL, so the table column can be required.
                check_type(name, &expected.field_type, &actual.field_type)?;
            }
        }
        for ((field, arrow_field), source) in
            columns.iter().zip(arrow_schema.fields()).zip(&mut sources)
        {
            if !matches!(source, Source::Null) {
                continue;
            }
            if let Some(default) = &field.write_default {
                let value = default_value(default, arrow_field.data_type())
                    .with_context(|| format!("write default of table column '{}'", field.name))?;
                *source = Source::Default(value);
            } else if field.required {
                bail!(
                    "the table column '{}' is required, but the view has no such column",
                    field.name
                );
            }
        }

        Ok(Self {
            table: table.clone(),
            schema,
            arrow_schema,
            row_schema: Arc::new(ArrowSchema::new(row_fields)),
            sources,
        })
    }

    /// Schema of the batches that [BatchWriter::write] takes.
    pub fn row_schema(&self) -> ArrowSchemaRef {
        self.row_schema.clone()
    }

    /// Data file writer for a batch, or for a key range of one.
    pub async fn writer(self: &Arc<Self>) -> Result<BatchWriter, SinkError> {
        let error = |e| write_error("error creating a data file writer", e);
        let metadata = self.table.metadata();

        // Cap the row group size.
        let mut properties = metadata.properties().clone();
        let row_group_bytes = TableProperties::new(&properties)
            .parquet_row_group_size_bytes()
            .map_err(error)?
            .min(MAX_ROW_GROUP_BYTES);
        properties.insert(
            TableProperties::PROPERTY_PARQUET_ROW_GROUP_SIZE_BYTES.to_string(),
            row_group_bytes.to_string(),
        );
        let properties = TableProperties::new(&properties);

        let parquet = ParquetWriterBuilder::from_table_properties(&properties, self.schema.clone())
            .map_err(error)?;
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            properties.write_target_file_size_bytes().map_err(error)?,
            self.table.file_io().clone(),
            Location::new(metadata).map_err(error)?,
            // Unique per writer, so no two writers share a file name.
            DefaultFileNameGenerator::new(
                format!("feldera-{}", Uuid::new_v4()),
                None,
                DataFileFormat::Parquet,
            ),
        );
        // The default spec id is not always 0, and the spec can have void-transform fields.
        let spec = metadata.default_partition_spec();
        let partition = Struct::from_iter(spec.fields().iter().map(|_| None::<Literal>));
        let key = PartitionKey::new(spec.as_ref().clone(), self.schema.clone(), partition);
        let files = DataFileWriterBuilder::new(rolling)
            .build(Some(key))
            .await
            .map_err(error)?;
        Ok(BatchWriter {
            layout: self.clone(),
            files,
        })
    }

    /// View rows as table rows: change log columns added, columns the view lacks filled.
    fn table_batch(&self, rows: &RecordBatch, ops: &[&str], micros: i64) -> AnyResult<RecordBatch> {
        let num_rows = rows.num_rows();
        if ops.len() != num_rows {
            bail!("{num_rows} rows have {} operations", ops.len());
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.sources.len());
        for (field, source) in self.arrow_schema.fields().iter().zip(&self.sources) {
            columns.push(match source {
                Source::Row(position) => rows.column(*position).clone(),
                Source::Op => Arc::new(StringArray::from_iter_values(ops)),
                Source::Ts => Arc::new(Int64Array::from_value(micros, num_rows)),
                Source::Null => new_null_array(field.data_type(), num_rows),
                Source::Default(value) => {
                    take(value.as_ref(), &UInt32Array::from(vec![0; num_rows]), None)?
                }
            });
        }
        Ok(RecordBatch::try_new(self.arrow_schema.clone(), columns)?)
    }
}

/// One-row array of `default` in Arrow type `data_type`.
fn default_value(default: &Literal, data_type: &DataType) -> AnyResult<ArrayRef> {
    let Literal::Primitive(value) = default else {
        bail!("only primitive defaults are supported");
    };
    let array: ArrayRef = match (value, data_type) {
        // A cast would rescale the unscaled value.
        (PrimitiveLiteral::Int128(v), DataType::Decimal128(precision, scale)) => {
            Arc::new(Decimal128Array::from(vec![*v]).with_precision_and_scale(*precision, *scale)?)
        }
        (PrimitiveLiteral::UInt128(v), _) => Arc::new(FixedSizeBinaryArray::try_from_iter(
            std::iter::once(v.to_be_bytes()),
        )?),
        (PrimitiveLiteral::Boolean(v), _) => Arc::new(BooleanArray::from(vec![*v])),
        (PrimitiveLiteral::Int(v), _) => Arc::new(Int32Array::from(vec![*v])),
        (PrimitiveLiteral::Long(v), _) => Arc::new(Int64Array::from(vec![*v])),
        (PrimitiveLiteral::Float(v), _) => Arc::new(Float32Array::from(vec![v.0])),
        (PrimitiveLiteral::Double(v), _) => Arc::new(Float64Array::from(vec![v.0])),
        (PrimitiveLiteral::String(v), _) => Arc::new(StringArray::from(vec![v.as_str()])),
        (PrimitiveLiteral::Binary(v), _) => Arc::new(BinaryArray::from_vec(vec![v.as_slice()])),
        _ => bail!("unsupported default {value:?} for {data_type}"),
    };
    // For example, int to date and long to timestamp.
    Ok(cast(&array, data_type)?)
}

/// Writes rows to data files.
pub struct BatchWriter {
    layout: Arc<Layout>,
    files: FileWriter,
}

impl BatchWriter {
    /// Writes `rows`, in [Layout::row_schema], with one op per row.
    pub async fn write(
        &mut self,
        rows: &RecordBatch,
        ops: &[&str],
        micros: i64,
    ) -> Result<(), SinkError> {
        let batch = self
            .layout
            .table_batch(rows, ops, micros)
            .map_err(|e| SinkError::Permanent(anyhow!("error building a record batch: {e}")))?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let length = WRITE_BATCH_ROWS.min(batch.num_rows() - offset);
            self.files
                .write(batch.slice(offset, length))
                .await
                .map_err(|e| write_error("error writing a data file", e))?;
            offset += length;
        }
        Ok(())
    }

    /// Returns the data files.
    pub async fn close(mut self) -> Result<Vec<DataFile>, SinkError> {
        self.files
            .close()
            .await
            .map_err(|e| write_error("error closing a data file", e))
    }
}

/// The data files of one batch, not yet committed.
#[derive(Debug)]
pub struct PendingCommit {
    /// Kept when the batch is written again.
    id: Uuid,
    /// Current snapshot when the batch was first written. No older snapshot holds it.
    base: Option<i64>,
    files: Vec<DataFile>,
    /// The table that the files were written for.
    table: Table,
    attempted: bool,
}

impl PendingCommit {
    pub fn num_bytes(&self) -> u64 {
        self.files.iter().map(|f| f.file_size_in_bytes()).sum()
    }

    /// The batch written again. Keeps `id` and `base`, so a late commit of the old files
    /// still counts as this batch.
    pub fn rewritten(self, layout: &Layout, files: Vec<DataFile>) -> Self {
        Self {
            files,
            table: layout.table.clone(),
            attempted: false,
            ..self
        }
    }
}

/// What a [CommitGuard] found that ends the commit.
#[derive(Debug)]
enum Found {
    /// An earlier attempt committed the batch.
    Committed,
    Changed(&'static str),
}

/// Checks each table that a commit loads for an earlier attempt of the batch. The commit
/// requires the loaded snapshot to stay current, so no late attempt slips by.
#[derive(Debug)]
struct CommitGuard<'a> {
    catalog: Arc<dyn Catalog>,
    pending: &'a PendingCommit,
    found: Mutex<Option<Found>>,
}

impl CommitGuard<'_> {
    /// What `table` says about the commit, if anything.
    fn check(&self, table: &Table) -> Option<Found> {
        let (metadata, written) = (table.metadata(), self.pending.table.metadata());

        let id = self.pending.id.to_string();
        let mut snapshot = metadata.current_snapshot();
        while let Some(current) = snapshot {
            if Some(current.snapshot_id()) == self.pending.base {
                break;
            }
            if current
                .summary()
                .additional_properties
                .get(COMMIT_ID_PROPERTY)
                == Some(&id)
            {
                return Some(Found::Committed);
            }
            snapshot = current
                .parent_snapshot_id()
                .and_then(|parent| metadata.snapshot_by_id(parent));
        }

        // The files hold one schema's field ids, in one partition spec's layout.
        let changed = if metadata.uuid() != written.uuid() {
            "another table replaced it"
        } else if metadata.current_schema_id() != written.current_schema_id() {
            "its schema changed"
        } else if metadata.default_partition_spec_id() != written.default_partition_spec_id() {
            "its partition spec changed"
        } else {
            return None;
        };
        Some(Found::Changed(changed))
    }

    fn unused<T>() -> iceberg::Result<T> {
        Err(iceberg::Error::new(
            ErrorKind::FeatureUnsupported,
            "a commit does not use this catalog operation",
        ))
    }
}

/// Runs `request` with [CATALOG_TIMEOUT].
async fn catalog_request<T>(
    request: impl std::future::Future<Output = iceberg::Result<T>>,
) -> iceberg::Result<T> {
    timeout(CATALOG_TIMEOUT, request).await.unwrap_or_else(|_| {
        Err(iceberg::Error::new(
            ErrorKind::Unexpected,
            format!("the catalog did not answer in {CATALOG_TIMEOUT:?}"),
        ))
    })
}

#[async_trait]
impl Catalog for CommitGuard<'_> {
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let table = catalog_request(self.catalog.load_table(table)).await?;
        if let Some(found) = self.check(&table) {
            *self.found.lock().unwrap() = Some(found);
            // Not retryable, so the commit stops.
            return Err(iceberg::Error::new(
                ErrorKind::PreconditionFailed,
                "the commit is settled",
            ));
        }
        Ok(table)
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        catalog_request(self.catalog.update_table(commit)).await
    }

    async fn list_namespaces(
        &self,
        _: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        Self::unused()
    }
    async fn create_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        Self::unused()
    }
    async fn get_namespace(&self, _: &NamespaceIdent) -> iceberg::Result<Namespace> {
        Self::unused()
    }
    async fn namespace_exists(&self, _: &NamespaceIdent) -> iceberg::Result<bool> {
        Self::unused()
    }
    async fn update_namespace(
        &self,
        _: &NamespaceIdent,
        _: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        Self::unused()
    }
    async fn drop_namespace(&self, _: &NamespaceIdent) -> iceberg::Result<()> {
        Self::unused()
    }
    async fn list_tables(&self, _: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        Self::unused()
    }
    async fn create_table(&self, _: &NamespaceIdent, _: TableCreation) -> iceberg::Result<Table> {
        Self::unused()
    }
    async fn drop_table(&self, _: &TableIdent) -> iceberg::Result<()> {
        Self::unused()
    }
    async fn purge_table(&self, _: &TableIdent) -> iceberg::Result<()> {
        Self::unused()
    }
    async fn table_exists(&self, _: &TableIdent) -> iceberg::Result<bool> {
        Self::unused()
    }
    async fn rename_table(&self, _: &TableIdent, _: &TableIdent) -> iceberg::Result<()> {
        Self::unused()
    }
    async fn register_table(&self, _: &TableIdent, _: String) -> iceberg::Result<Table> {
        Self::unused()
    }
}

/// Makes a catalog client.
type Connect = Box<dyn Fn() -> BoxFuture<'static, AnyResult<Arc<dyn Catalog>>> + Send + Sync>;

struct Connection {
    catalog: Arc<dyn Catalog>,
    since: tokio::time::Instant,
    /// A request failed. Reconnect before the next one.
    failed: bool,
}

/// An Iceberg table that the connector appends to.
pub struct IcebergSink {
    connect: Connect,
    connection: tokio::sync::Mutex<Connection>,
    /// `None` in tests.
    catalog_type: Option<IcebergCatalogType>,
    ident: TableIdent,
    relation: Relation,
}

impl IcebergSink {
    /// Opens or creates the table. With `resume`, the pipeline restarts from a checkpoint and
    /// an existing table is its own. Every attempt of one open passes the same `create_id`.
    pub async fn open(
        config: &IcebergWriterConfig,
        relation: &Relation,
        resume: bool,
        create_id: &str,
    ) -> Result<Self, SinkError> {
        let mut config = config.clone();
        apply_fileio_defaults(&mut config.fileio_config);
        let (table_name, mode) = (config.table_name.clone(), config.mode);
        let catalog_type = Some(config.catalog_type.clone());
        let connect: Connect = Box::new(move || {
            let config = config.clone();
            Box::pin(async move {
                CatalogConfig {
                    catalog_type: &config.catalog_type,
                    glue: &config.glue_catalog_config,
                    rest: &config.rest_catalog_config,
                    s3tables: &config.s3tables_catalog_config,
                    fileio: &config.fileio_config,
                }
                .load()
                .await
            })
        });
        Self::open_with(
            connect,
            catalog_type,
            &table_name,
            mode,
            relation,
            resume,
            create_id,
        )
        .await
    }

    /// [Self::open] with a catalog client.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) async fn open_with_catalog(
        catalog: Arc<dyn Catalog>,
        table_name: &str,
        mode: IcebergWriteMode,
        relation: &Relation,
        resume: bool,
        create_id: &str,
    ) -> Result<Self, SinkError> {
        let connect: Connect = Box::new(move || {
            let catalog = catalog.clone();
            Box::pin(async move { Ok(catalog) })
        });
        Self::open_with(connect, None, table_name, mode, relation, resume, create_id).await
    }

    async fn open_with(
        connect: Connect,
        catalog_type: Option<IcebergCatalogType>,
        table_name: &str,
        mode: IcebergWriteMode,
        relation: &Relation,
        resume: bool,
        create_id: &str,
    ) -> Result<Self, SinkError> {
        let ident = TableIdent::from_strs(table_name.split('.'))
            .map_err(|e| SinkError::Permanent(anyhow!("invalid table name '{table_name}': {e}")))?;
        let catalog = Self::connect(&connect).await?;
        let sink = Self {
            connect,
            connection: tokio::sync::Mutex::new(Connection {
                catalog: catalog.clone(),
                since: tokio::time::Instant::now(),
                failed: false,
            }),
            catalog_type,
            ident,
            relation: relation.clone(),
        };

        let no_namespace = |e: &iceberg::Error| {
            SinkError::Permanent(anyhow!(
                "the namespace '{}' of the table does not exist in the catalog: {e}",
                sink.ident.namespace().join(".")
            ))
        };
        let exists = || async {
            match catalog_request(catalog.table_exists(&sink.ident)).await {
                Ok(exists) => Ok(exists),
                Err(e) if e.kind() == ErrorKind::NamespaceNotFound => Err(no_namespace(&e)),
                Err(e) => Err(SinkError::iceberg("error looking up the table", e)),
            }
        };
        let mut created = false;
        if !exists().await? {
            let schema = Schema::builder()
                .with_fields(iceberg_fields(relation).map_err(SinkError::Permanent)?)
                .build()
                .map_err(|e| {
                    SinkError::Permanent(anyhow!("error building the table schema: {e}"))
                })?;
            if let Some(column) = sink.glue_timestamptz([&schema]) {
                return Err(SinkError::Permanent(anyhow!(
                    "the view column '{column}' has type TIMESTAMP WITH TIME ZONE, which the \
                     connector cannot write through the AWS Glue catalog"
                )));
            }
            let creation = TableCreation::builder()
                .name(sink.ident.name().to_string())
                .schema(schema)
                .properties(HashMap::from([
                    (CREATE_ID_PROPERTY.to_string(), create_id.to_string()),
                    (DELETE_AFTER_COMMIT_PROPERTY.to_string(), "true".to_string()),
                ]))
                .build();
            let create = catalog.create_table(sink.ident.namespace(), creation);
            match catalog_request(create).await {
                Ok(_) => created = true,
                // Some catalogs report a missing namespace only on create.
                Err(e) if e.kind() == ErrorKind::NamespaceNotFound => return Err(no_namespace(&e)),
                // Not every catalog reports `TableAlreadyExists`. Another writer can have
                // created the table since the lookup.
                Err(e) => {
                    if !exists().await? {
                        return Err(SinkError::iceberg("error creating the table", e));
                    }
                }
            }
        }
        if !created && !resume && mode == IcebergWriteMode::ErrorIfExists {
            // Our create can land with its response lost, in this attempt or an earlier one.
            let table = catalog_request(catalog.load_table(&sink.ident))
                .await
                .map_err(|e| SinkError::iceberg("error loading the table", e))?;
            let creator = table.metadata().properties().get(CREATE_ID_PROPERTY);
            if creator.map(String::as_str) != Some(create_id) {
                return Err(SinkError::Permanent(anyhow!(
                    "the table '{table_name}' exists, and with `mode` set to `error_if_exists` \
                     the connector writes only to a table that it creates"
                )));
            }
            created = true;
        }

        // Check a table that we did not create now, not at the first batch.
        if !created {
            sink.begin().await.map_err(|e| match e {
                SinkError::Unfit(e) => SinkError::Permanent(e),
                e => e,
            })?;
        }
        Ok(sink)
    }

    async fn connect(connect: &Connect) -> Result<Arc<dyn Catalog>, SinkError> {
        match timeout(CATALOG_TIMEOUT, connect()).await {
            Ok(catalog) => catalog.map_err(SinkError::Transient),
            Err(_) => Err(SinkError::Transient(anyhow!(
                "the catalog did not answer in {CATALOG_TIMEOUT:?}"
            ))),
        }
    }

    /// Reconnects after a failed request or after [CATALOG_MAX_AGE].
    async fn catalog(&self) -> Result<Arc<dyn Catalog>, SinkError> {
        let mut connection = self.connection.lock().await;
        if connection.failed || connection.since.elapsed() >= CATALOG_MAX_AGE {
            let catalog = Self::connect(&self.connect).await?;
            *connection = Connection {
                catalog,
                since: tokio::time::Instant::now(),
                failed: false,
            };
        }
        Ok(connection.catalog.clone())
    }

    /// A `timestamptz` column in `schemas`, on Glue. The Glue client converts every schema of
    /// the table on each commit, and cannot convert `timestamptz`.
    fn glue_timestamptz<'a>(
        &self,
        schemas: impl IntoIterator<Item = &'a Schema>,
    ) -> Option<String> {
        if self.catalog_type != Some(IcebergCatalogType::Glue) {
            return None;
        }
        schemas
            .into_iter()
            .find_map(|schema| timestamptz_column("", schema.as_struct().fields()))
    }

    /// Marks the client failed on a transient error.
    async fn seen<T>(&self, result: Result<T, SinkError>) -> Result<T, SinkError> {
        if let Err(SinkError::Transient(_)) = &result {
            self.connection.lock().await.failed = true;
        }
        result
    }

    /// Loads the table for the next batch. Vended storage credentials expire, and the schema
    /// can change.
    pub async fn begin(&self) -> Result<Arc<Layout>, SinkError> {
        let catalog = self.catalog().await?;
        let table = catalog_request(catalog.load_table(&self.ident))
            .await
            .map_err(|e| match e.kind() {
                // The owner can create it again.
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound => {
                    SinkError::Unfit(anyhow!("error loading the table: {e}"))
                }
                _ => SinkError::iceberg("error loading the table", e),
            });
        let table = self.seen(table).await?;
        let metadata = table.metadata();
        // The Glue client fails on every commit.
        if let Some(column) = self.glue_timestamptz(metadata.schemas_iter().map(AsRef::as_ref)) {
            return Err(SinkError::Permanent(anyhow!(
                "the table '{}' has, or had in an earlier schema, the `timestamptz` column \
                 '{column}', which the connector cannot write through the AWS Glue catalog",
                self.ident
            )));
        }
        // The owner can evolve the spec to an unpartitioned one.
        if !metadata.default_partition_spec().is_unpartitioned() {
            return Err(SinkError::Unfit(anyhow!(
                "the table '{}' is partitioned, which the connector does not support yet. Write \
                 to a table that is not partitioned",
                self.ident
            )));
        }
        let layout = Layout::new(&table, &self.relation).map_err(|e| {
            SinkError::Unfit(anyhow!(
                "the table '{}' does not fit the view: {e}",
                self.ident
            ))
        })?;
        Ok(Arc::new(layout))
    }

    /// Starts the commit of the data files of one batch.
    pub fn prepare(&self, layout: &Layout, files: Vec<DataFile>) -> PendingCommit {
        PendingCommit {
            id: Uuid::new_v4(),
            base: layout.table.metadata().current_snapshot_id(),
            files,
            table: layout.table.clone(),
            attempted: false,
        }
    }

    /// Appends the files as one snapshot. Retry a transient error with the same `pending`:
    /// an attempt that failed can still land, and the retry then finds it.
    pub async fn commit(&self, pending: &mut PendingCommit) -> Result<(), SinkError> {
        let retry = std::mem::replace(&mut pending.attempted, true);
        let guard = CommitGuard {
            catalog: self.catalog().await?,
            pending,
            found: Mutex::new(None),
        };
        let result = async {
            // The credentials of the table from `begin` can have expired.
            let table = if retry {
                guard.load_table(&self.ident).await?
            } else {
                pending.table.clone()
            };
            let transaction = Transaction::new(&table);
            transaction
                .fast_append()
                // It reads every manifest. File names are unique, and the guard dedups.
                .with_check_duplicate(false)
                .set_snapshot_properties(HashMap::from([(
                    COMMIT_ID_PROPERTY.to_string(),
                    pending.id.to_string(),
                )]))
                .add_data_files(pending.files.iter().cloned())
                .apply(transaction)?
                .commit(&guard)
                .await
        }
        .await;
        let found = guard.found.lock().unwrap().take();
        let result = match (found, result) {
            (Some(Found::Committed), _) | (None, Ok(_)) => Ok(()),
            (Some(Found::Changed(change)), _) => Err(SinkError::TableChanged(anyhow!(
                "the table '{}' changed while a batch was written for it: {change}",
                self.ident
            ))),
            // Giving up loses the batch, and any commit error can clear.
            (None, Err(e)) => Err(SinkError::Transient(anyhow!(
                "error committing to the table: {e}"
            ))),
        };
        self.seen(result).await
    }
}

/// Path of the first `timestamptz` field, nested ones included.
fn timestamptz_column(path: &str, fields: &[NestedFieldRef]) -> Option<String> {
    fields.iter().find_map(|field| {
        let path = format!("{path}{}", field.name);
        match field.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs) => {
                Some(path)
            }
            Type::Primitive(_) | Type::Variant(_) => None,
            Type::Struct(nested) => timestamptz_column(&format!("{path}."), nested.fields()),
            Type::List(list) => timestamptz_column(
                &format!("{path}."),
                std::slice::from_ref(&list.element_field),
            ),
            Type::Map(map) => timestamptz_column(
                &format!("{path}."),
                &[map.key_field.clone(), map.value_field.clone()],
            ),
        }
    })
}

/// The view columns and the change log columns. The catalog assigns the final field ids.
fn iceberg_fields(relation: &Relation) -> AnyResult<Vec<NestedFieldRef>> {
    let mut next_id = 0;
    let mut fields = Vec::with_capacity(relation.fields.len() + 2);
    for field in &relation.fields {
        fields.push(iceberg_field(field, &field.name.name(), &mut next_id)?);
    }
    for (name, primitive) in [
        (OP_COLUMN, PrimitiveType::String),
        (TS_COLUMN, PrimitiveType::Long),
    ] {
        if fields.iter().any(|f| f.name == name) {
            bail!("the view has a column '{name}', which is the name of a change log column");
        }
        next_id += 1;
        fields.push(Arc::new(NestedField::optional(
            next_id,
            name,
            Type::Primitive(primitive),
        )));
    }
    Ok(fields)
}

fn iceberg_field(field: &Field, path: &str, next_id: &mut i32) -> AnyResult<NestedFieldRef> {
    *next_id += 1;
    let id = *next_id;
    let field_type = iceberg_type(&field.columntype, path, next_id)?;
    Ok(Arc::new(NestedField::new(
        id,
        field.name.name(),
        field_type,
        !field.columntype.nullable,
    )))
}

/// `path` names the column in errors.
fn iceberg_type(column: &ColumnType, path: &str, next_id: &mut i32) -> AnyResult<Type> {
    let unsupported = |hint: &str| {
        anyhow!(
            "column '{path}' has type {:?}, which has no Iceberg type{hint}",
            column.typ
        )
    };
    fn nested<'a>(
        path: &str,
        what: &str,
        part: &'a Option<Box<ColumnType>>,
    ) -> AnyResult<&'a ColumnType> {
        part.as_deref()
            .ok_or_else(|| anyhow!("column '{path}' has no {what} type"))
    }
    let primitive = match column.typ {
        SqlType::Boolean => PrimitiveType::Boolean,
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int => PrimitiveType::Int,
        SqlType::UTinyInt | SqlType::USmallInt => PrimitiveType::Int,
        SqlType::BigInt | SqlType::UInt => PrimitiveType::Long,
        SqlType::UBigInt => {
            return Err(unsupported(
                " that holds all its values. Cast the column to DECIMAL(20, 0)",
            ))
        }
        SqlType::Real => PrimitiveType::Float,
        SqlType::Double => PrimitiveType::Double,
        SqlType::Decimal => PrimitiveType::Decimal {
            precision: u32::try_from(column.precision.unwrap_or(0))?,
            scale: u32::try_from(column.scale.unwrap_or(0))?,
        },
        SqlType::Char | SqlType::Varchar => PrimitiveType::String,
        // JSON text: Iceberg `variant` needs a v3 table.
        SqlType::Variant => PrimitiveType::String,
        SqlType::Binary | SqlType::Varbinary => PrimitiveType::Binary,
        SqlType::Time => PrimitiveType::Time,
        SqlType::Date => PrimitiveType::Date,
        SqlType::Timestamp => PrimitiveType::Timestamp,
        SqlType::TimestampTz => PrimitiveType::Timestamptz,
        SqlType::Uuid => PrimitiveType::Uuid,
        SqlType::Interval(_) | SqlType::Null => return Err(unsupported("")),
        SqlType::Array => {
            let component = nested(path, "element", &column.component)?;
            *next_id += 1;
            let id = *next_id;
            let element = iceberg_type(component, &format!("{path}.element"), next_id)?;
            return Ok(Type::List(ListType::new(Arc::new(
                NestedField::list_element(id, element, !component.nullable),
            ))));
        }
        SqlType::Map => {
            let key = nested(path, "key", &column.key)?;
            let value = nested(path, "value", &column.value)?;
            *next_id += 1;
            let key_id = *next_id;
            let key_type = iceberg_type(key, &format!("{path}.key"), next_id)?;
            *next_id += 1;
            let value_id = *next_id;
            let value_type = iceberg_type(value, &format!("{path}.value"), next_id)?;
            return Ok(Type::Map(MapType::new(
                Arc::new(NestedField::map_key_element(key_id, key_type)),
                Arc::new(NestedField::map_value_element(
                    value_id,
                    value_type,
                    !value.nullable,
                )),
            )));
        }
        SqlType::Struct => {
            let fields = column
                .fields
                .as_ref()
                .ok_or_else(|| anyhow!("column '{path}' has no fields"))?
                .iter()
                .map(|f| iceberg_field(f, &format!("{path}.{}", f.name.name()), next_id))
                .collect::<AnyResult<Vec<_>>>()?;
            return Ok(Type::Struct(StructType::new(fields)));
        }
    };
    Ok(Type::Primitive(primitive))
}

/// Checks that values of `expected`, the column we would create, fit table column `actual`.
fn check_field(path: &str, expected: &NestedField, actual: &NestedField) -> AnyResult<()> {
    if actual.required && !expected.required {
        bail!("the table column '{path}' is required, but the view column can be NULL");
    }
    check_type(path, &expected.field_type, &actual.field_type)
}

fn check_type(path: &str, expected: &Type, actual: &Type) -> AnyResult<()> {
    match (expected, actual) {
        (Type::Primitive(expected), Type::Primitive(actual)) => {
            if expected != actual {
                bail!(
                    "the table column '{path}' has type {actual}, but the view column needs type {expected}"
                );
            }
            Ok(())
        }
        (Type::Struct(expected), Type::Struct(actual)) => {
            // The Arrow encoder writes NULL to an optional field.
            for field in actual.fields() {
                let lacked = !expected.fields().iter().any(|f| f.name == field.name);
                if lacked && (field.required || field.write_default.is_some()) {
                    bail!(
                        "the table column '{path}' has the field '{}', which the view column \
                         lacks and which is required or has a write default",
                        field.name
                    );
                }
            }
            for field in expected.fields() {
                let path = format!("{path}.{}", field.name);
                let Some(actual) = actual.fields().iter().find(|f| f.name == field.name) else {
                    bail!("the table has no field '{path}'");
                };
                check_field(&path, field, actual)?;
            }
            Ok(())
        }
        (Type::List(expected), Type::List(actual)) => check_field(
            &format!("{path}.element"),
            &expected.element_field,
            &actual.element_field,
        ),
        (Type::Map(expected), Type::Map(actual)) => {
            check_field(
                &format!("{path}.key"),
                &expected.key_field,
                &actual.key_field,
            )?;
            check_field(
                &format!("{path}.value"),
                &expected.value_field,
                &actual.value_field,
            )
        }
        _ => bail!(
            "the table column '{path}' has type {actual}, but the view column needs type {expected}"
        ),
    }
}

/// Memory catalog and local-disk store for sink tests.
#[cfg(any(test, feature = "testing"))]
pub mod test_support {
    use super::*;
    use futures_util::TryStreamExt;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    pub use iceberg::spec::PrimitiveType;
    use iceberg::transaction::AddColumn;
    use iceberg::CatalogBuilder;
    pub use iceberg::ErrorKind;
    use std::collections::VecDeque;
    use tempfile::TempDir;

    /// The namespace that [TestWarehouse::new] creates.
    pub const NAMESPACE: &str = "ns";

    /// A catalog in memory over a temporary directory.
    pub struct TestWarehouse {
        pub catalog: Arc<dyn Catalog>,
        _dir: TempDir,
    }

    impl TestWarehouse {
        pub async fn new() -> Self {
            let dir = TempDir::new().unwrap();
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        dir.path().to_str().unwrap().to_string(),
                    )]),
                )
                .await
                .unwrap();
            catalog
                .create_namespace(&NamespaceIdent::new(NAMESPACE.to_string()), HashMap::new())
                .await
                .unwrap();
            Self {
                catalog: Arc::new(catalog),
                _dir: dir,
            }
        }

        /// Opens table `table` of [NAMESPACE] as the connector does.
        pub async fn sink(
            &self,
            table: &str,
            mode: IcebergWriteMode,
            relation: &Relation,
            resume: bool,
        ) -> Result<IcebergSink, SinkError> {
            IcebergSink::open_with_catalog(
                self.catalog.clone(),
                &format!("{NAMESPACE}.{table}"),
                mode,
                relation,
                resume,
                &Uuid::new_v4().to_string(),
            )
            .await
        }

        pub async fn table(&self, table: &str) -> Table {
            self.catalog
                .load_table(&TableIdent::from_strs([NAMESPACE, table]).unwrap())
                .await
                .unwrap()
        }

        /// The rows of the current snapshot of `table`.
        pub async fn read(&self, table: &str) -> Vec<RecordBatch> {
            let table = self.table(table).await;
            if table.metadata().current_snapshot().is_none() {
                return Vec::new();
            }
            let scan = table.scan().select_all().build().unwrap();
            scan.to_arrow().await.unwrap().try_collect().await.unwrap()
        }

        /// Adds the optional column `name` of type `ty` to `table`.
        pub async fn add_column(&self, table: &str, name: &str, ty: PrimitiveType) {
            let table = TableIdent::from_strs([NAMESPACE, table]).unwrap();
            add_column_to(&self.catalog, &table, name, ty).await;
        }

        pub async fn drop_table(&self, table: &str) {
            let table = TableIdent::from_strs([NAMESPACE, table]).unwrap();
            self.catalog.drop_table(&table).await.unwrap();
        }

        pub async fn drop_column(&self, table: &str, name: &str) {
            let table = self.table(table).await;
            let transaction = Transaction::new(&table);
            transaction
                .update_schema()
                .delete_column(name)
                .apply(transaction)
                .unwrap()
                .commit(self.catalog.as_ref())
                .await
                .unwrap();
        }

        /// Opens table `table` through the Glue client's checks.
        pub async fn glue_sink(&self, table: &str, relation: &Relation) -> IcebergSink {
            let catalog = self.catalog.clone();
            let connect: Connect = Box::new(move || {
                let catalog = catalog.clone();
                Box::pin(async move { Ok(catalog) })
            });
            let name = format!("{NAMESPACE}.{table}");
            let (mode, glue) = (IcebergWriteMode::Append, Some(IcebergCatalogType::Glue));
            IcebergSink::open_with(connect, glue, &name, mode, relation, false, "create-1")
                .await
                .unwrap()
        }

        /// The directory of the data files of `table`.
        pub async fn data_dir(&self, table: &str) -> std::path::PathBuf {
            let table = self.table(table).await;
            std::path::Path::new(table.metadata().location()).join("data")
        }

        /// Data files of `table`: (on disk, in the current snapshot).
        pub async fn data_files(&self, table: &str) -> (usize, usize) {
            let on_disk = std::fs::read_dir(self.data_dir(table).await)
                .unwrap()
                .count();
            let loaded = self.table(table).await;
            let in_table = loaded.metadata().current_snapshot().map_or(0, |snapshot| {
                snapshot.summary().additional_properties["total-data-files"]
                    .parse()
                    .unwrap()
            });
            (on_disk, in_table)
        }

        /// Sets properties of `table`.
        pub async fn set_properties(&self, table: &str, properties: &[(&str, &str)]) {
            let table = self.table(table).await;
            let transaction = Transaction::new(&table);
            let mut update = transaction.update_table_properties();
            for (key, value) in properties {
                update = update.set(key.to_string(), value.to_string());
            }
            update
                .apply(transaction)
                .unwrap()
                .commit(self.catalog.as_ref())
                .await
                .unwrap();
        }

        /// Opens `table` as [Self::sink] does, with a catalog that fails as `faults` say.
        pub async fn flaky_sink(
            &self,
            table: &str,
            relation: &Relation,
            faults: Faults,
        ) -> (IcebergSink, Arc<FlakyCatalog>) {
            let catalog = FlakyCatalog::new(&self.catalog, faults);
            let sink = IcebergSink::open_with_catalog(
                catalog.clone(),
                &format!("{NAMESPACE}.{table}"),
                IcebergWriteMode::Append,
                relation,
                false,
                &Uuid::new_v4().to_string(),
            )
            .await
            .unwrap();
            (sink, catalog)
        }

        /// The number of snapshots of `table`.
        pub async fn snapshots(&self, table: &str) -> usize {
            self.table(table).await.metadata().snapshots().count()
        }
    }

    async fn add_column_to(
        catalog: &Arc<dyn Catalog>,
        table: &TableIdent,
        name: &str,
        ty: PrimitiveType,
    ) {
        let table = catalog.load_table(table).await.unwrap();
        let transaction = Transaction::new(&table);
        transaction
            .update_schema()
            .add_column(AddColumn::optional(name, Type::Primitive(ty)))
            .apply(transaction)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }

    /// Injected commit failure.
    #[derive(Debug, Clone, Copy)]
    pub enum Failure {
        /// The commit does not reach the table.
        Lost,
        /// The commit lands, and the response is lost.
        ResponseLost,
        /// The client gets an error, and the commit lands later: after the next table
        /// load, before the next commit.
        Late,
        /// The catalog never answers, and the commit does not land.
        Hang,
    }

    #[derive(Debug, Default)]
    pub struct Faults {
        pub commits: VecDeque<Failure>,
        /// A commit that [Failure::Late] holds back.
        pub late: Option<TableCommit>,
        /// Number of upcoming table loads that fail.
        pub loads: usize,
        /// Kind of the load failures. `None` gives `Unexpected`.
        pub load_kind: Option<ErrorKind>,
        /// The next create lands, and the response is lost.
        pub create_response_lost: bool,
        /// Adds column `added` before the Nth table load from now.
        pub add_column_at_load: Option<usize>,
        /// `table_exists` returns false, as the REST catalog does for a missing namespace.
        pub lookup_finds_nothing: bool,
        /// Number of upcoming table loads that each add a column first.
        pub schema_changes: usize,
    }

    /// A catalog that fails as its [Faults] say.
    #[derive(Debug)]
    pub struct FlakyCatalog {
        inner: Arc<dyn Catalog>,
        pub faults: Mutex<Faults>,
    }

    impl FlakyCatalog {
        pub fn new(inner: &Arc<dyn Catalog>, faults: Faults) -> Arc<Self> {
            Arc::new(Self {
                inner: inner.clone(),
                faults: Mutex::new(faults),
            })
        }

        /// What the REST catalog client returns for a 500, 502, or 504 response.
        fn unknown() -> iceberg::Error {
            iceberg::Error::new(ErrorKind::Unexpected, "the state is unknown (injected)")
        }
    }

    #[async_trait]
    impl Catalog for FlakyCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> iceberg::Result<Table> {
            let table = self.inner.create_table(namespace, creation).await?;
            if std::mem::take(&mut self.faults.lock().unwrap().create_response_lost) {
                return Err(Self::unknown());
            }
            Ok(table)
        }

        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
            let (add_column, change) = {
                let mut faults = self.faults.lock().unwrap();
                if faults.loads > 0 {
                    faults.loads -= 1;
                    return Err(match faults.load_kind {
                        Some(kind) => iceberg::Error::new(kind, "the load failed (injected)"),
                        None => Self::unknown(),
                    });
                }
                let add_column = match &mut faults.add_column_at_load {
                    Some(1) => faults.add_column_at_load.take().is_some(),
                    Some(loads) => {
                        *loads -= 1;
                        false
                    }
                    None => false,
                };
                let change = faults.schema_changes.checked_sub(1);
                faults.schema_changes = change.unwrap_or(0);
                (add_column, change)
            };
            if add_column {
                add_column_to(&self.inner, table, "added", PrimitiveType::Long).await;
            }
            if let Some(n) = change {
                add_column_to(
                    &self.inner,
                    table,
                    &format!("changed{n}"),
                    PrimitiveType::Long,
                )
                .await;
            }
            let loaded = self.inner.load_table(table).await?;
            let late = self.faults.lock().unwrap().late.take();
            if let Some(late) = late {
                self.inner.update_table(late).await?;
            }
            Ok(loaded)
        }

        async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.inner.purge_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            if self.faults.lock().unwrap().lookup_finds_nothing {
                return Ok(false);
            }
            self.inner.table_exists(table).await
        }

        async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> iceberg::Result<Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
            let failure = self.faults.lock().unwrap().commits.pop_front();
            match failure {
                None => self.inner.update_table(commit).await,
                Some(Failure::Lost) => Err(Self::unknown()),
                Some(Failure::ResponseLost) => {
                    self.inner.update_table(commit).await?;
                    Err(Self::unknown())
                }
                Some(Failure::Late) => {
                    self.faults.lock().unwrap().late = Some(commit);
                    Err(Self::unknown())
                }
                Some(Failure::Hang) => std::future::pending().await,
            }
        }
    }
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
