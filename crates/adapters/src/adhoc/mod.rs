use crate::PipelineError;
use actix_web::http::header;
use actix_web::HttpResponse;
use arrow::record_batch::RecordBatch;
use arrow_json::LineDelimitedWriter;
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionContext;
use feldera_types::query::{AdHocResultFormat, AdhocQueryArgs};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub(crate) mod table;

pub async fn stream_adhoc_result(
    args: AdhocQueryArgs,
    session: SessionContext,
) -> Result<HttpResponse, PipelineError> {
    let df = session.sql(&args.sql).await?;
    let response = df.collect().await?;

    match args.format {
        AdHocResultFormat::Text => {
            let pretty_results = arrow::util::pretty::pretty_format_batches(&response)
                .map_err(DataFusionError::from)?
                .to_string();
            Ok(HttpResponse::Ok()
                .content_type(mime::TEXT_PLAIN)
                .body(pretty_results))
        }
        AdHocResultFormat::Json => {
            let mut buf = Vec::with_capacity(4096);
            let mut writer = LineDelimitedWriter::new(&mut buf);
            writer
                .write_batches(response.iter().collect::<Vec<&RecordBatch>>().as_slice())
                .map_err(DataFusionError::from)?;
            writer.finish().map_err(DataFusionError::from)?;

            Ok(HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .body(buf))
        }
        AdHocResultFormat::Parquet => {
            let mut buf = Vec::with_capacity(4096);
            let writer_properties = WriterProperties::builder().build();
            let schema = response[0].schema();
            let mut writer =
                ArrowWriter::try_new(&mut buf, schema.clone(), Some(writer_properties))
                    .map_err(PipelineError::from)?;
            for batch in response {
                writer.write(&batch)?;
            }
            let file_name = format!(
                "results_{}.parquet",
                chrono::Utc::now().format("%Y%m%d_%H%M%S")
            );
            writer.close().expect("closing writer");
            Ok(HttpResponse::Ok()
                .insert_header(header::ContentDisposition::attachment(file_name))
                .content_type(mime::APPLICATION_OCTET_STREAM)
                .body(buf))
        }
    }
}
