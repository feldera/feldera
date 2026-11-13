//! Settings shared by the HTTP servers of the API server and the runner.

/// Whether the HTTP servers keep an HTTP/1 connection open after the client closes its side.
///
/// It is `false`: the server closes the connection as soon as it reads EOF from the client.
/// With half-closed connections allowed (the actix default), a streaming response notices
/// that the client left only when a write to it fails. The logs stream of a pipeline that
/// writes no new lines never writes, so the connection and its log follower stay alive
/// forever, and each abandoned `/logs` request leaks a socket and memory in both the API
/// server and the runner.
pub(crate) const H1_ALLOW_HALF_CLOSED: bool = false;

#[cfg(test)]
mod tests {
    use super::H1_ALLOW_HALF_CLOSED;
    use actix_web::{App, HttpResponse, HttpServer, get, web};
    use std::convert::Infallible;
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    /// Streams a body that never produces data, like the logs of a quiet pipeline. The
    /// sender goes to the test, which watches for the server to drop the body.
    #[get("/stream")]
    async fn stream(
        senders: web::Data<mpsc::UnboundedSender<mpsc::Sender<web::Bytes>>>,
    ) -> HttpResponse {
        let (sender, receiver) = mpsc::channel::<web::Bytes>(1);
        senders.send(sender).unwrap();
        HttpResponse::Ok()
            .content_type("text/plain")
            .streaming(ReceiverStream::new(receiver).map(Ok::<_, Infallible>))
    }

    /// A client that disconnects from a silent streaming response must cause the server to
    /// drop the response body, which is what releases a log follower.
    #[actix_web::test]
    async fn silent_stream_is_dropped_when_client_disconnects() {
        let (senders, mut sender_receiver) = mpsc::unbounded_channel::<mpsc::Sender<web::Bytes>>();
        let senders = web::Data::new(senders);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = HttpServer::new(move || App::new().app_data(senders.clone()).service(stream))
            .h1_allow_half_closed(H1_ALLOW_HALF_CLOSED)
            .workers(1)
            .listen(listener)
            .unwrap()
            .run();
        let server_handle = server.handle();
        actix_web::rt::spawn(server);

        // Send the request and wait for the response head, so the body is streaming.
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut head = Vec::new();
        let mut buffer = [0u8; 1024];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = client.read(&mut buffer).await.unwrap();
            assert!(
                n > 0,
                "server closed the connection before the response head"
            );
            head.extend_from_slice(&buffer[..n]);
        }
        let body_sender = sender_receiver.recv().await.unwrap();

        // The client goes away without the body ever producing a byte.
        drop(client);

        let dropped = timeout(Duration::from_secs(5), body_sender.closed()).await;
        server_handle.stop(false).await;
        assert!(
            dropped.is_ok(),
            "the server kept the streaming body after the client disconnected"
        );
    }
}
