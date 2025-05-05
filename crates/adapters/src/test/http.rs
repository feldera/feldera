//! Helper functions for testing http-based communication.

use crate::{test::TestStruct, transport::http::Chunk};
use actix_web::web::Bytes;
use async_stream::stream;
use awc::{error::PayloadError, ClientRequest};
use csv::ReaderBuilder as CsvReaderBuilder;
use csv::WriterBuilder as CsvWriterBuilder;
use futures::{Stream, StreamExt};
use serde::Deserialize;
use tracing::trace;

pub struct TestHttpSender;
pub struct TestHttpReceiver;

impl TestHttpSender {
    /// Serialize `data` as `csv` and send it as part of HTTP request.
    pub async fn send_stream(req: ClientRequest, data: &[Vec<TestStruct>]) -> Bytes {
        let data = data.to_vec();

        let mut response = req
            .send_stream(stream! {
                for batch in data.iter() {
                    let mut writer = CsvWriterBuilder::new()
                        .has_headers(false)
                        .from_writer(Vec::with_capacity(batch.len() * 32));

                    for val in batch.iter().cloned() {
                        writer.serialize(val).unwrap();
                    }
                    writer.flush().unwrap();
                    let bytes = writer.into_inner().unwrap();
                    yield <Result<_, anyhow::Error>>::Ok(Bytes::from(bytes));
                }
            })
            .await
            .unwrap();

        response.body().await.unwrap()
    }

    pub async fn send_stream_deserialize_resp<R>(req: ClientRequest, data: &[Vec<TestStruct>]) -> R
    where
        R: for<'de> Deserialize<'de>,
    {
        let resp_bytes = Self::send_stream(req, data).await;
        serde_json::from_slice::<R>(&resp_bytes).unwrap()
    }
}

impl TestHttpReceiver {
    /// Read from `response` until the entire contents of `data` is received.
    pub async fn wait_for_output_unordered<S>(response: &mut S, data: &[Vec<TestStruct>])
    where
        S: Stream<Item = Result<Bytes, PayloadError>> + Unpin,
    {
        let num_records: usize = data.iter().map(Vec::len).sum();

        let mut expected = data
            .iter()
            .flat_map(|data| data.iter())
            .cloned()
            .collect::<Vec<_>>();
        expected.sort();

        let mut received = Vec::with_capacity(num_records);

        let mut data = Vec::new();

        while received.len() < num_records {
            let bytes = response.next().await.unwrap().unwrap();
            trace!("TestHttpReceiver: received {} bytes", bytes.len());

            data.extend_from_slice(&bytes);

            if data[data.len() - 1] == b'\n' {
                for chunk in serde_json::Deserializer::from_slice(&data).into_iter::<Chunk>() {
                    let chunk = chunk.unwrap();
                    trace!("TestHttpReceiver: chunk {}", chunk.sequence_number);

                    let mut builder = CsvReaderBuilder::new();
                    builder.has_headers(false);
                    let mut reader = builder.from_reader(if let Some(csv) = &chunk.text_data {
                        csv.as_bytes()
                    } else {
                        continue;
                    });
                    // let mut num_received = 0;
                    for (record, w) in reader
                        .deserialize::<(TestStruct, i32)>()
                        .map(Result::unwrap)
                    {
                        // num_received += 1;
                        assert_eq!(w, 1);
                        // println!("received record: {:?}", record);
                        received.push(record);
                    }
                }
                data.clear();
            }
        }
        received.sort();

        assert_eq!(expected, received);
    }
}
