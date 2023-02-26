//! Helper functions for testing websocket-based communication.

use crate::test::TestStruct;
use actix_codec::Framed;
use actix_http::ws::{Codec as WsCodec, Frame as WsFrame, Item as WsItem, Message as WsMessage};
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use csv::ReaderBuilder as CsvReaderBuilder;
use csv::WriterBuilder as CsvWriterBuilder;
use futures::{SinkExt, StreamExt};
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct TestWsSender;

impl TestWsSender {
    /// Serialize `data` as `csv` and send it through web socket using one
    /// single-frame message per buffer.
    pub async fn send_to_websocket(
        mut ws: Pin<&mut Framed<impl AsyncRead + AsyncWrite, WsCodec>>,
        data: &[Vec<TestStruct>],
    ) {
        for batch in data.iter() {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::with_capacity(batch.len() * 32));

            for val in batch.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.into_inner().unwrap();

            let mut buf = BytesMut::with_capacity(8 + bytes.len());
            buf.put(&b"12345678"[..]);
            buf.put(bytes.as_slice());

            ws.send(WsMessage::Binary(buf.freeze())).await.unwrap();
            let ack = ws.next().await.unwrap().unwrap();
            if let WsFrame::Binary(ack) = ack {
                assert_eq!(ack.len(), 16);
                assert_eq!(ack[0..8], b"12345678"[..]);
                assert_eq!(BigEndian::read_u64(&ack[8..16]), (bytes.len() + 8) as u64);
            } else {
                panic!("Unexpected websocket response: {ack:?}");
            }
        }
    }

    /// Serialize `data` as `csv` and send it through web socket as one big
    /// composite message with one frame per buffer.
    pub async fn send_to_websocket_continuations(
        mut ws: Pin<&mut Framed<impl AsyncRead + AsyncWrite, WsCodec>>,
        data: &[Vec<TestStruct>],
    ) {
        if data.len() <= 1 {
            return Self::send_to_websocket(ws, data).await;
        }

        let mut total_len = 0;

        for (index, batch) in data.iter().enumerate() {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::with_capacity(batch.len() * 32));

            for val in batch.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.into_inner().unwrap();
            total_len += bytes.len();

            if index == 0 {
                let mut buf = BytesMut::with_capacity(8 + bytes.len());
                buf.put(&b"12345678"[..]);
                buf.put(bytes.as_slice());

                ws.send(WsMessage::Continuation(WsItem::FirstBinary(buf.freeze())))
                    .await
                    .unwrap();
            } else if index == data.len() - 1 {
                ws.send(WsMessage::Continuation(WsItem::Last(bytes.into())))
                    .await
                    .unwrap();

                let ack = ws.next().await.unwrap().unwrap();
                if let WsFrame::Binary(ack) = ack {
                    assert_eq!(ack.len(), 16);
                    assert_eq!(ack[0..8], b"12345678"[..]);
                    assert_eq!(BigEndian::read_u64(&ack[8..16]), (total_len + 8) as u64);
                } else {
                    panic!("Unexpected websocket response: {ack:?}");
                }
            } else {
                ws.send(WsMessage::Continuation(WsItem::Continue(bytes.into())))
                    .await
                    .unwrap();
            }
        }
    }
}

pub struct TestWsReceiver;

impl TestWsReceiver {
    /// Read from `ws` until the entire contents of `data` is received.
    // FIXME: Generalize this to process multi-frame messages.
    pub async fn wait_for_output_unordered(
        mut ws: Pin<&mut Framed<impl AsyncRead + AsyncWrite, WsCodec>>,
        data: &[Vec<TestStruct>],
    ) {
        let num_records: usize = data.iter().map(Vec::len).sum();

        let mut expected = data
            .iter()
            .flat_map(|data| data.iter())
            .cloned()
            .collect::<Vec<_>>();
        expected.sort();

        let mut received = Vec::with_capacity(num_records);

        while received.len() < num_records {
            let frame = ws.next().await.unwrap().unwrap();

            match frame {
                WsFrame::Binary(bytes) => {
                    let mut builder = CsvReaderBuilder::new();
                    builder.has_headers(false);
                    let mut reader = builder.from_reader(&*bytes);
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
                frame => {
                    panic!("TestWsReceiver::wait_for_output_unordered: Unexpected frame: {frame:?}")
                }
            }
        }
        received.sort();

        assert_eq!(expected, received);
    }
}
