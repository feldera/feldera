use crate::{
    arrow::{decode_base64_ipc_stream, normalize_snowflake_batch},
    client::{response_data, SnowflakeApiResponse, SnowflakeClient},
};
use anyhow::{anyhow, bail, Context, Result as AnyResult};
use arrow::{buffer::Buffer, ipc::reader::StreamDecoder, record_batch::RecordBatch};
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use bytes::BytesMut;
use feldera_types::{program_schema::Relation, transport::snowflake::SnowflakeNumberMode};
use futures_util::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use rand::Rng;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_ENCODING},
    StatusCode, Url,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    io,
    pin::Pin,
    sync::atomic::Ordering,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader as AsyncStreamReader;
use uuid::Uuid;

const QUERY_IN_PROGRESS_CODE: &str = "333333";
const QUERY_IN_PROGRESS_ASYNC_CODE: &str = "333334";
const CHUNK_DOWNLOAD_MAX_ATTEMPTS: u32 = 6;
const CHUNK_RETRY_INITIAL_BACKOFF_MS: u64 = 250;
const CHUNK_RETRY_MAX_BACKOFF_MS: u64 = 8_000;
const ARROW_DECODE_BUFFER_SIZE: usize = 2 * 1024 * 1024;

pub(crate) struct SnowflakeArrowQueryMetadata {
    pub(crate) query_id: Option<String>,
    pub(crate) total_rows: Option<u64>,
}

pub(crate) struct SnowflakeArrowBatchStream<'a> {
    pub(crate) metadata: SnowflakeArrowQueryMetadata,
    pub(crate) batches: BoxStream<'a, AnyResult<RecordBatch>>,
}

fn snowflake_column_identifier(identifier: &str) -> AnyResult<&str> {
    let identifier = identifier.trim();

    if identifier.starts_with('"') {
        if identifier.len() < 2 || !identifier.ends_with('"') {
            bail!("invalid quoted Snowflake column identifier '{identifier}'");
        }

        let mut characters = identifier[1..identifier.len() - 1].chars();
        while let Some(character) = characters.next() {
            if character == '"' && characters.next() != Some('"') {
                bail!("invalid quoted Snowflake column identifier '{identifier}'");
            }
        }

        return Ok(identifier);
    }

    let mut characters = identifier.chars();
    if !matches!(characters.next(), Some(character) if character.is_ascii_alphabetic() || character == '_')
        || !characters
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '$'))
    {
        bail!("invalid Snowflake column identifier '{identifier}'");
    }

    Ok(identifier)
}

pub(crate) fn build_snapshot_query(
    table: &str,
    snapshot_filter: Option<&str>,
    column_mapping: &BTreeMap<String, String>,
    relation: &Relation,
) -> AnyResult<String> {
    let table = table.trim();
    if table.is_empty() {
        bail!("Snowflake table name must not be empty");
    }

    let skip_unused_columns = relation.get_property("skip_unused_columns") == Some("true");
    let mut source_columns = HashMap::new();
    for (target, source) in column_mapping {
        let target_lowercase = target.to_lowercase();
        let field = relation
            .fields
            .iter()
            .find(|field| field.name.case_sensitive && field.name.name() == *target)
            .or_else(|| {
                relation.fields.iter().find(|field| {
                    !field.name.case_sensitive && field.name.name() == target_lowercase
                })
            });
        let Some(field) = field else {
            bail!(
                "Snowflake column mapping refers to unknown Feldera column '{target}' in input relation '{}'",
                relation.name
            );
        };

        let source = snowflake_column_identifier(source)?;
        let target = field.name.name();
        if source_columns.insert(target.clone(), source).is_some() {
            bail!(
                "Snowflake column mapping contains multiple entries for Feldera column '{target}'"
            );
        }
    }

    let columns = relation
        .fields
        .iter()
        .filter(|field| {
            !skip_unused_columns
                || !field.unused
                || (!field.columntype.nullable && field.default.is_none())
        })
        .map(|field| {
            source_columns.get(&field.name.name()).map_or_else(
                || field.name.sql_name(),
                |source| format!("{source} AS {}", field.name.sql_name()),
            )
        })
        .collect::<Vec<_>>();

    if columns.is_empty() {
        bail!(
            "Snowflake snapshot query for input relation '{}' has no columns to read",
            relation.name
        );
    }

    let mut query = format!("SELECT {} FROM {table}", columns.join(", "));

    if let Some(filter) = snapshot_filter {
        let filter = filter.trim();
        if filter.is_empty() {
            bail!("Snowflake snapshot filter must not be empty when specified");
        }
        query.push_str(" WHERE ");
        query.push_str(filter);
    }

    Ok(query)
}

#[derive(Deserialize)]
pub(super) struct QueryData {
    #[serde(default, rename = "queryId")]
    query_id: Option<String>,
    #[serde(default, rename = "queryResultFormat")]
    query_result_format: Option<String>,
    #[serde(default, rename = "rowsetBase64")]
    rowset_base64: Option<String>,
    #[serde(default)]
    chunks: Vec<SnowflakeChunk>,
    #[serde(default, rename = "chunkHeaders")]
    chunk_headers: HashMap<String, String>,
    #[serde(default)]
    qrmk: Option<String>,
    #[serde(default)]
    total: Option<u64>,
    #[serde(default, rename = "getResultUrl")]
    get_result_url: Option<String>,
}

#[derive(Deserialize)]
struct SnowflakeChunk {
    url: String,
}

impl SnowflakeClient {
    pub(crate) async fn query_arrow_batch_stream(
        &self,
        sql: &str,
        max_concurrent_readers: usize,
        number_mode: SnowflakeNumberMode,
    ) -> AnyResult<SnowflakeArrowBatchStream<'_>> {
        let data = self.execute_statement(sql).await?;
        let format = data.query_result_format.as_deref().unwrap_or_default();
        if !format.eq_ignore_ascii_case("arrow") && !format.eq_ignore_ascii_case("arrow_force") {
            bail!("Snowflake returned queryResultFormat='{format}', expected Arrow");
        }

        let inline_batches = data
            .rowset_base64
            .as_deref()
            .map(|rowset| decode_base64_ipc_stream(rowset, number_mode))
            .transpose()?;
        let chunk_headers = chunk_headers(&data)?;
        let metadata = SnowflakeArrowQueryMetadata {
            query_id: data.query_id,
            total_rows: data.total,
        };

        let inline_stream = stream::iter(
            inline_batches
                .into_iter()
                .flatten()
                .map(Ok::<_, anyhow::Error>),
        );
        let downloaded_batches = stream::iter(
            data.chunks
                .into_iter()
                .map(move |chunk| self.stream_chunk(chunk, chunk_headers.clone(), number_mode)),
        )
        .flatten_unordered(max_concurrent_readers.max(1));

        Ok(SnowflakeArrowBatchStream {
            metadata,
            batches: inline_stream.chain(downloaded_batches).boxed(),
        })
    }

    pub(super) async fn execute_statement(&self, sql: &str) -> AnyResult<QueryData> {
        let sql = sql.trim();
        if sql.is_empty() {
            bail!("Snowflake query must not be empty");
        }

        let mut url = self.endpoint("/queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &Uuid::new_v4().to_string());

        let body = json!({
            "sqlText": sql,
            "asyncExec": false,
            "sequenceId": self.sequence_id.fetch_add(1, Ordering::Relaxed) + 1,
            "querySubmissionTime": epoch_millis(),
        });

        let mut response = self
            .http
            .post(url)
            .headers(self.session_headers()?)
            .json(&body)
            .send()
            .await
            .context("error sending Snowflake query request")?
            .error_for_status()
            .context("Snowflake query request failed")?
            .json::<SnowflakeApiResponse<QueryData>>()
            .await
            .context("error parsing Snowflake query response")?;

        while response.code.as_deref().is_some_and(|code| {
            code == QUERY_IN_PROGRESS_CODE || code == QUERY_IN_PROGRESS_ASYNC_CODE
        }) {
            let result_url = response
                .data
                .as_ref()
                .and_then(|data| data.get_result_url.as_deref())
                .ok_or_else(|| {
                    anyhow!("Snowflake query-in-progress response did not include getResultUrl")
                })?;
            let result_url = self
                .base_url
                .join(result_url.trim_start_matches('/'))
                .with_context(|| format!("invalid Snowflake query result URL '{result_url}'"))?;
            response = self
                .http
                .get(result_url)
                .headers(self.session_headers()?)
                .send()
                .await
                .context("error polling Snowflake query result")?
                .error_for_status()
                .context("Snowflake query result poll failed")?
                .json::<SnowflakeApiResponse<QueryData>>()
                .await
                .context("error parsing Snowflake query result response")?;
        }

        response_data(response, "Snowflake query")
    }

    fn stream_chunk<'a>(
        &'a self,
        chunk: SnowflakeChunk,
        headers: HeaderMap,
        number_mode: SnowflakeNumberMode,
    ) -> BoxStream<'a, AnyResult<RecordBatch>> {
        try_stream! {
            let chunk_url = redact_url(&chunk.url);
            let response = self
                .open_chunk_response(&chunk.url, &headers)
                .await
                .with_context(|| format!("error opening Snowflake result chunk {chunk_url}"))?;
            let gzip_header = response_is_gzip_encoded(&response, &chunk.url);
            let body_url = chunk_url.clone();
            let body = response.bytes_stream().map_err(move |error| {
                io::Error::other(format!(
                    "error reading Snowflake result chunk {body_url}: {error}"
                ))
            });
            let mut body = BufReader::new(AsyncStreamReader::new(body));
            let gzip_magic = body
                .fill_buf()
                .await
                .with_context(|| format!("error reading Snowflake result chunk {chunk_url}"))?
                .starts_with(&[0x1f, 0x8b]);

            let reader: Pin<Box<dyn AsyncRead + Send>> = if gzip_header || gzip_magic {
                Box::pin(GzipDecoder::new(body))
            } else {
                Box::pin(body)
            };
            let mut batches = decode_arrow_reader(reader, chunk_url, number_mode);
            while let Some(batch) = batches.next().await {
                yield batch?;
            }
        }
        .boxed()
    }

    async fn open_chunk_response(
        &self,
        url: &str,
        headers: &HeaderMap,
    ) -> AnyResult<reqwest::Response> {
        for attempt in 1..=CHUNK_DOWNLOAD_MAX_ATTEMPTS {
            let response = match self.http.get(url).headers(headers.clone()).send().await {
                Ok(response) => response,
                Err(_) if attempt < CHUNK_DOWNLOAD_MAX_ATTEMPTS => {
                    tokio::time::sleep(chunk_retry_backoff(attempt)).await;
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("error sending Snowflake chunk request, attempt {attempt}")
                    });
                }
            };

            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }

            if !is_retryable_chunk_status(status) || attempt == CHUNK_DOWNLOAD_MAX_ATTEMPTS {
                response.error_for_status().with_context(|| {
                    format!("Snowflake chunk request failed with status {status}")
                })?;
                unreachable!("error_for_status returned Ok for unsuccessful status {status}");
            }

            tokio::time::sleep(chunk_retry_backoff(attempt)).await;
        }

        unreachable!("chunk retry loop should return or error");
    }
}

/// Incrementally decode Arrow IPC batches from an asynchronous byte source.
fn decode_arrow_reader<R>(
    mut reader: R,
    source: String,
    number_mode: SnowflakeNumberMode,
) -> BoxStream<'static, AnyResult<RecordBatch>>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    try_stream! {
        let mut decoder = StreamDecoder::new();

        loop {
            let mut bytes = BytesMut::with_capacity(ARROW_DECODE_BUFFER_SIZE);
            let count = reader
                .read_buf(&mut bytes)
                .await
                .with_context(|| format!("error reading Arrow IPC data from {source}"))?;
            if count == 0 {
                break;
            }

            let mut buffer = Buffer::from(bytes.freeze());
            while !buffer.is_empty() {
                if let Some(batch) = decoder
                    .decode(&mut buffer)
                    .with_context(|| format!("error decoding Arrow IPC data from {source}"))?
                {
                    yield normalize_snowflake_batch(batch, number_mode).with_context(|| {
                        format!("error normalizing Arrow data from {source}")
                    })?;
                }
            }
        }

        decoder
            .finish()
            .with_context(|| format!("incomplete Arrow IPC stream from {source}"))?;
    }
    .boxed()
}

fn chunk_retry_backoff(attempt: u32) -> Duration {
    let base_delay_ms = CHUNK_RETRY_INITIAL_BACKOFF_MS
        .checked_shl(attempt.saturating_sub(1))
        .unwrap_or(u64::MAX)
        .min(CHUNK_RETRY_MAX_BACKOFF_MS);
    let jitter_ms = rand::thread_rng().gen_range(0..=(base_delay_ms / 4));

    Duration::from_millis((base_delay_ms + jitter_ms).min(CHUNK_RETRY_MAX_BACKOFF_MS))
}

fn chunk_headers(data: &QueryData) -> AnyResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in &data.chunk_headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid Snowflake chunk header name: {name}"))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid Snowflake chunk header value for {name}"))?;
        headers.insert(name, value);
    }

    if headers.is_empty() {
        if let Some(qrmk) = &data.qrmk {
            headers.insert(
                HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
                HeaderValue::from_static("AES256"),
            );
            headers.insert(
                HeaderName::from_static("x-amz-server-side-encryption-customer-key"),
                HeaderValue::from_str(qrmk).context("invalid Snowflake qrmk chunk header")?,
            );
        }
    }

    Ok(headers)
}

fn response_is_gzip_encoded(response: &reqwest::Response, url: &str) -> bool {
    response
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"))
        || url.contains("response-content-encoding=gzip")
}

fn is_retryable_chunk_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::INTERNAL_SERVER_ERROR
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::GATEWAY_TIMEOUT
}

fn redact_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => "<invalid-url>".to_string(),
    }
}

fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema},
        ipc::writer::StreamWriter,
    };
    use feldera_types::program_schema::{
        ColumnType, Field, PropertyValue, Relation, SourcePosition, SqlIdentifier,
    };
    use flate2::{write::GzEncoder, Compression};
    use std::{io::Write, sync::Arc};
    use tokio::{
        io::{duplex, AsyncWriteExt},
        sync::oneshot,
        time::timeout,
    };

    fn relation(fields: &[&str]) -> Relation {
        Relation {
            name: SqlIdentifier::from("T".to_string()),
            fields: fields
                .iter()
                .map(|name| {
                    Field::new(
                        SqlIdentifier::from((*name).to_string()),
                        ColumnType::varchar(true),
                    )
                })
                .collect(),
            materialized: false,
            properties: Default::default(),
            primary_key: None,
        }
    }

    fn enable_skip_unused_columns(relation: &mut Relation) {
        let position = SourcePosition {
            start_line_number: 0,
            start_column: 0,
            end_line_number: 0,
            end_column: 0,
        };
        relation.properties.insert(
            "skip_unused_columns".to_string(),
            PropertyValue {
                value: "true".to_string(),
                key_position: position,
                value_position: position,
            },
        );
    }

    #[test]
    fn builds_snapshot_query() {
        let query = build_snapshot_query(
            "DB.SCHEMA.T",
            Some("ID > 10"),
            &BTreeMap::new(),
            &relation(&["ID", "\"customer name\""]),
        )
        .unwrap();

        assert_eq!(
            query,
            "SELECT ID, \"customer name\" FROM DB.SCHEMA.T WHERE ID > 10"
        );
    }

    #[test]
    fn reads_declared_unused_columns_by_default() {
        let mut relation = relation(&["ID", "UNUSED"]);
        relation.fields[1].unused = true;

        assert_eq!(
            build_snapshot_query("T", None, &BTreeMap::new(), &relation).unwrap(),
            "SELECT ID, UNUSED FROM T"
        );
    }

    #[test]
    fn skips_unused_columns_when_requested_by_table() {
        let mut relation = relation(&["ID", "UNUSED"]);
        relation.fields[1].unused = true;
        enable_skip_unused_columns(&mut relation);

        assert_eq!(
            build_snapshot_query("T", None, &BTreeMap::new(), &relation).unwrap(),
            "SELECT ID FROM T"
        );
    }

    #[test]
    fn retains_nonnullable_unused_columns_without_defaults() {
        let mut relation = relation(&["ID", "REQUIRED"]);
        relation.fields[1].unused = true;
        relation.fields[1].columntype.nullable = false;
        enable_skip_unused_columns(&mut relation);

        assert_eq!(
            build_snapshot_query("T", None, &BTreeMap::new(), &relation).unwrap(),
            "SELECT ID, REQUIRED FROM T"
        );
    }

    #[test]
    fn rejects_empty_table() {
        let err =
            build_snapshot_query(" ", None, &BTreeMap::new(), &relation(&["ID"])).unwrap_err();
        assert!(err.to_string().contains("table name"));
    }

    #[test]
    fn maps_snowflake_columns_to_feldera_columns() {
        let mapping = BTreeMap::from([
            ("uuid".to_string(), "UUID".to_string()),
            ("status".to_string(), "\"applicationStatus\"".to_string()),
            ("CamelCase".to_string(), "CAMEL_CASE".to_string()),
        ]);

        let query = build_snapshot_query(
            "T",
            None,
            &mapping,
            &relation(&[
                "\"uuid\"",
                "STATUS",
                "CAMELCASE",
                "\"CamelCase\"",
                "UNCHANGED",
            ]),
        )
        .unwrap();

        assert_eq!(
            query,
            "SELECT UUID AS \"uuid\", \"applicationStatus\" AS STATUS, CAMELCASE, CAMEL_CASE AS \"CamelCase\", UNCHANGED FROM T"
        );
    }

    #[test]
    fn rejects_unknown_column_mapping_target() {
        let mapping = BTreeMap::from([("missing".to_string(), "UUID".to_string())]);

        let error =
            build_snapshot_query("T", None, &mapping, &relation(&["\"uuid\""])).unwrap_err();

        assert!(error
            .to_string()
            .contains("unknown Feldera column 'missing'"));
    }

    #[test]
    fn rejects_duplicate_mapping_for_case_insensitive_column() {
        let mapping = BTreeMap::from([
            ("status".to_string(), "STATUS_A".to_string()),
            ("STATUS".to_string(), "STATUS_B".to_string()),
        ]);

        let error = build_snapshot_query("T", None, &mapping, &relation(&["STATUS"])).unwrap_err();

        assert!(error.to_string().contains("multiple entries"));
    }

    #[test]
    fn validates_mapped_snowflake_column_identifier() {
        let valid_mapping = BTreeMap::from([(
            "target".to_string(),
            "\"source with \"\"quote\"\"\"".to_string(),
        )]);
        assert_eq!(
            build_snapshot_query("T", None, &valid_mapping, &relation(&["TARGET"]),).unwrap(),
            "SELECT \"source with \"\"quote\"\"\" AS TARGET FROM T"
        );

        for source in ["source, other", "source AS other", "unterminated\""] {
            let mapping = BTreeMap::from([("target".to_string(), source.to_string())]);
            let error =
                build_snapshot_query("T", None, &mapping, &relation(&["TARGET"])).unwrap_err();
            assert!(error.to_string().contains("Snowflake column identifier"));
        }
    }

    #[tokio::test]
    async fn streams_arrow_batch_before_gzip_eof() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let first =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let second =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3, 4]))])
                .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &schema).unwrap();
            writer.write(&first).unwrap();
            writer.write(&second).unwrap();
            writer.finish().unwrap();
        }

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&ipc).unwrap();
        let gzip = encoder.finish().unwrap();
        let trailer_start = gzip.len() - 8;
        let (mut writer, reader) = duplex(gzip.len());
        let (release_sender, release_receiver) = oneshot::channel();
        tokio::spawn(async move {
            writer.write_all(&gzip[..trailer_start]).await.unwrap();
            release_receiver.await.unwrap();
            writer.write_all(&gzip[trailer_start..]).await.unwrap();
        });

        let reader = GzipDecoder::new(BufReader::new(reader));
        let mut batches = decode_arrow_reader(
            reader,
            "test stream".to_string(),
            SnowflakeNumberMode::Decimal,
        );
        let first = timeout(Duration::from_secs(2), batches.next())
            .await
            .expect("first batch was not decoded before gzip EOF")
            .unwrap()
            .unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );

        release_sender.send(()).unwrap();
        let second = batches.next().await.unwrap().unwrap();
        assert_eq!(second.num_rows(), 2);
        assert_eq!(
            second
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 4]
        );
        assert!(batches.next().await.is_none());
    }

    #[test]
    fn redacts_presigned_chunk_url() {
        assert_eq!(
            redact_url("https://example.s3.amazonaws.com/chunk?X-Amz-Signature=secret#frag"),
            "https://example.s3.amazonaws.com/chunk"
        );
        assert_eq!(redact_url("not a url"), "<invalid-url>");
    }

    #[test]
    fn chunk_retry_uses_bounded_exponential_backoff() {
        for _ in 0..100 {
            assert!((250..=312).contains(&chunk_retry_backoff(1).as_millis()));
            assert!((4_000..=5_000).contains(&chunk_retry_backoff(5).as_millis()));
            assert_eq!(chunk_retry_backoff(u32::MAX), Duration::from_secs(8));
        }
    }

    #[test]
    fn retries_transient_chunk_statuses() {
        for status in [429, 500, 502, 503, 504] {
            assert!(is_retryable_chunk_status(StatusCode::from_u16(status).unwrap()));
        }
        for status in [400, 403, 404, 501, 505] {
            assert!(!is_retryable_chunk_status(
                StatusCode::from_u16(status).unwrap()
            ));
        }
    }
}
