//! Helper functions for testing http-based communication.

use crate::{test::TestStruct, transport::http::Chunk};
use bytes::Bytes;
use csv::ReaderBuilder as CsvReaderBuilder;
use csv::WriterBuilder as CsvWriterBuilder;
use futures::{Stream, StreamExt, TryStreamExt};
use reqwest::{RequestBuilder, Response};
use serde::Deserialize;
use tracing::debug;
use tracing::trace;

pub struct TestHttpSender;
pub struct TestHttpReceiver;

impl TestHttpSender {
    /// Serialize `data` as `csv` and send it as part of HTTP request.
    /// Returns the response as bytes.
    pub async fn send_stream(req: RequestBuilder, data: &[Vec<TestStruct>]) -> Bytes {
        // Serialize data as CSV
        let mut csv_data = Vec::new();
        for batch in data.iter() {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::new());

            for val in batch.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            csv_data.extend_from_slice(&writer.into_inner().unwrap());
        }

        // Send the data
        let response = req
            .header("Content-Type", "text/csv")
            .body(csv_data)
            .send()
            .await
            .unwrap();

        response.bytes().await.unwrap()
    }

    /// Serialize `data` as `csv` and send it as part of HTTP request;
    /// Deserialize JSON response.
    pub async fn send_stream_deserialize_resp<R>(req: RequestBuilder, data: &[Vec<TestStruct>]) -> R
    where
        R: for<'de> Deserialize<'de>,
    {
        let resp_bytes = Self::send_stream(req, data).await;
        serde_json::from_slice::<R>(&resp_bytes).unwrap()
    }
}

impl TestHttpReceiver {
    /// Read from `stream` until the entire contents of `data` is received.
    /// Uses a mutable reference to a byte stream to allow partial reading.
    pub async fn wait_for_output_unordered<S>(stream: &mut S, data: &[Vec<TestStruct>])
    where
        S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
    {
        let num_records: usize = data.iter().map(Vec::len).sum();

        let mut expected = data
            .iter()
            .flat_map(|data| data.iter())
            .cloned()
            .collect::<Vec<_>>();
        expected.sort();

        let mut received = Vec::with_capacity(num_records);
        let mut buffer = Vec::new();

        println!("TestHttpReceiver::wait_for_output_unordered");
        // Use the provided stream directly

        while received.len() < num_records {
            match stream.try_next().await {
                Ok(Some(chunk)) => {
                    debug!("TestHttpReceiver: received {} bytes", chunk.len());
                    buffer.extend_from_slice(&chunk);

                    // Process complete lines (ending with '\n')
                    while let Some(newline_pos) = buffer.iter().position(|&b| b == b'\n') {
                        let line = buffer.drain(..=newline_pos).collect::<Vec<u8>>();

                        // Parse JSON chunk from the line
                        for chunk in
                            serde_json::Deserializer::from_slice(&line).into_iter::<Chunk>()
                        {
                            let chunk = chunk.unwrap();
                            debug!("TestHttpReceiver: chunk {}", chunk.sequence_number);

                            if let Some(csv) = &chunk.text_data {
                                let mut builder = CsvReaderBuilder::new();
                                builder.has_headers(false);
                                let mut reader = builder.from_reader(csv.as_bytes());

                                for (record, w) in reader
                                    .deserialize::<(TestStruct, i32)>()
                                    .map(Result::unwrap)
                                {
                                    assert_eq!(w, 1);
                                    received.push(record);
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Stream ended, process any remaining data in buffer
                    if !buffer.is_empty() {
                        for chunk in
                            serde_json::Deserializer::from_slice(&buffer).into_iter::<Chunk>()
                        {
                            let chunk = chunk.unwrap();
                            debug!("TestHttpReceiver: final chunk {}", chunk.sequence_number);

                            if let Some(csv) = &chunk.text_data {
                                let mut builder = CsvReaderBuilder::new();
                                builder.has_headers(false);
                                let mut reader = builder.from_reader(csv.as_bytes());

                                for (record, w) in reader
                                    .deserialize::<(TestStruct, i32)>()
                                    .map(Result::unwrap)
                                {
                                    assert_eq!(w, 1);
                                    received.push(record);
                                }
                            }
                        }
                    }
                    break;
                }
                Err(e) => {
                    panic!("Error reading from stream: {}", e);
                }
            }
        }

        received.sort();
        assert_eq!(expected, received);
    }
}
