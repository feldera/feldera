use super::MAX_SOCKETS_PER_ENDPOINT;
use crate::{InputConsumer, InputEndpoint, InputTransport, PipelineState};
use actix::{Actor, ActorContext, Addr, AsyncContext, Handler, Message, StreamHandler};
use actix_http::ws::Item as WsItem;
use actix_web::{web::Payload, HttpRequest, HttpResponse};
use actix_web_actors::ws::{
    self, CloseCode as WsCloseCode, CloseReason as WsCloseReason, Message as WsMessage,
    ProtocolError as WsProtocolError, WebsocketContext,
};
use anyhow::{anyhow, Result as AnyResult};
use byteorder::{BigEndian, WriteBytesExt};
use bytes::Bytes;
use log::{debug, info};
use num_traits::FromPrimitive;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, RwLock,
    },
};
use utoipa::ToSchema;

/// Global map of input HTTP endpoints.
static INPUT_HTTP_ENDPOINTS: Lazy<RwLock<BTreeMap<String, HttpInputEndpoint>>> =
    Lazy::new(|| RwLock::new(BTreeMap::new()));

/// `InputTransport` implementation that receives data from an HTTP endpoint
/// via a websocket.
pub struct HttpInputTransport;

impl InputTransport for HttpInputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("http")
    }

    fn new_endpoint(
        &self,
        name: &str,
        _config: &YamlValue,
        consumer: Box<dyn InputConsumer>,
    ) -> AnyResult<Box<dyn InputEndpoint>> {
        // let _config = HttpInputConfig::deserialize(config)?;
        let ep = HttpInputEndpoint::new(name, consumer)?;
        Ok(Box::new(ep))
    }
}

impl HttpInputTransport {
    pub(crate) fn get_endpoint_websocket(
        endpoint_name: &str,
        req: &HttpRequest,
        stream: Payload,
    ) -> AnyResult<HttpResponse> {
        let endpoint = INPUT_HTTP_ENDPOINTS
            .read()
            .unwrap()
            .get(endpoint_name)
            .map(Clone::clone)
            .ok_or_else(|| anyhow!("unknown HTTP input endpoint '{endpoint_name}'"))?;
        if endpoint.num_sockets() >= MAX_SOCKETS_PER_ENDPOINT {
            return Err(anyhow!(
                "maximum number of connections per HTTP endpoint exceeded"
            ));
        }
        let resp = ws::start(HttpInputWs::new(endpoint), req, stream)
            .map_err(|e| anyhow!(format!("error initializing websocket: {e}")))?;
        info!("HTTP input endpoint '{endpoint_name}': opened websocket");
        Ok(resp)
    }
}

#[derive(Clone, Deserialize, ToSchema)]
pub struct HttpInputConfig;

struct HttpInputEndpointInner {
    name: String,
    state: AtomicU32,
    consumer: Mutex<Box<dyn InputConsumer>>,

    /// Addresses of websocket actors associated with the endpoint.
    ///
    /// HTTP endpoint supports up to MAX_SOCKETS_PER_ENDPOINT simultaneously
    /// open websockets.  The client opens a new websocket by issuing a GET
    /// request to the `/input_endpoint/{endpoint_name}` endpoint.  The
    /// websocket is removed from this set when client connection closes.
    ///
    /// This field is used to notify all websocket actors about endpoint
    /// state changes.
    socket_addrs: RwLock<HashSet<Addr<HttpInputWs>>>,
}

impl HttpInputEndpointInner {
    fn new(name: &str, consumer: Box<dyn InputConsumer>) -> Self {
        Self {
            name: name.to_string(),
            state: AtomicU32::new(PipelineState::Paused as u32),
            consumer: Mutex::new(consumer),
            socket_addrs: RwLock::new(HashSet::new()),
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct EndpointStateNotification;

/// Input endpoint that establishes websocket connections with clients on demand
/// and receives data from these websockets.
///
/// This implementation only provides rudimentary support for reliable delivery
/// and is mostly intended for browser-based testing.
#[derive(Clone)]
struct HttpInputEndpoint {
    inner: Arc<HttpInputEndpointInner>,
}

impl HttpInputEndpoint {
    fn new(name: &str, consumer: Box<dyn InputConsumer>) -> AnyResult<Self> {
        let mut endpoint_map = INPUT_HTTP_ENDPOINTS.write().unwrap();

        if endpoint_map.contains_key(name) {
            return Err(anyhow!(format!("Duplicate HTTP endpoint name '{name}'")));
        }

        let endpoint = Self {
            inner: Arc::new(HttpInputEndpointInner::new(name, consumer)),
        };

        endpoint_map.insert(name.to_string(), endpoint.clone());
        Ok(endpoint)
    }

    fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    fn state(&self) -> PipelineState {
        PipelineState::from_u32(self.inner.state.load(Ordering::Acquire)).unwrap()
    }

    /// Number of connected websockets.
    fn num_sockets(&self) -> usize {
        self.inner.socket_addrs.read().unwrap().len()
    }

    /// Register new websocket actor.
    fn add_socket(&self, addr: Addr<HttpInputWs>) {
        self.inner.socket_addrs.write().unwrap().insert(addr);
    }

    /// Remove closed websocket.
    fn remove_socket(&self, addr: &Addr<HttpInputWs>) {
        self.inner.socket_addrs.write().unwrap().remove(addr);
    }

    /// Notify all websocket actors about endpoint state change.
    fn notify_sockets(&self) {
        for addr in self.inner.socket_addrs.read().unwrap().iter() {
            // Don't do blocking send, which will deadlock if the message
            // causes the actor to shut down.
            addr.do_send(EndpointStateNotification);
        }
    }

    fn push_bytes(&self, bytes: &[u8]) {
        self.inner.consumer.lock().unwrap().input(bytes);
    }
}

impl InputEndpoint for HttpInputEndpoint {
    fn pause(&self) -> AnyResult<()> {
        self.inner
            .state
            .store(PipelineState::Paused as u32, Ordering::Release);
        self.notify_sockets();

        Ok(())
    }

    fn start(&self) -> AnyResult<()> {
        self.inner
            .state
            .store(PipelineState::Running as u32, Ordering::Release);
        self.notify_sockets();

        Ok(())
    }

    fn disconnect(&self) {
        self.inner
            .state
            .store(PipelineState::Terminated as u32, Ordering::Release);
        self.notify_sockets();
    }
}

/// Actix actor that handles websocket communication.
struct HttpInputWs {
    endpoint: HttpInputEndpoint,

    /// Message tag of a multi-part message.
    ///
    /// Message tag is used to acknowledge a multipart message after its last
    /// fragment has been received.
    message_tag: [u8; 8],

    /// Total accumulated length of a multipart message used to acknowledge
    /// the message after its last fragment has been received.
    message_len: usize,
}

impl HttpInputWs {
    fn new(endpoint: HttpInputEndpoint) -> Self {
        Self {
            endpoint,
            message_tag: [0; 8],
            message_len: 0,
        }
    }

    /// Notify client about the current state of the endpoint.
    fn send_state(&self, ctx: &mut WebsocketContext<Self>, state: PipelineState) {
        debug!(
            "HTTP endpoint '{}': sending status update '{state:?}'",
            self.endpoint.name(),
        );
        ctx.text(match state {
            PipelineState::Running => "running",
            PipelineState::Paused => "paused",
            PipelineState::Terminated => "terminated",
        });
    }
}

impl Actor for HttpInputWs {
    type Context = WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Websocket connection established: register websocket actor with the endpoint.
        let addr = ctx.address();
        self.endpoint.add_socket(addr);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        // Connection closed: unregister the actor.
        let addr = ctx.address();
        self.endpoint.remove_socket(&addr);
    }
}

impl StreamHandler<Result<WsMessage, WsProtocolError>> for HttpInputWs {
    /// Handle websocket message from the client.
    fn handle(&mut self, msg: Result<WsMessage, WsProtocolError>, ctx: &mut Self::Context) {
        match msg {
            // The client may check our heartbeat by sending ping messages.
            // Reply with a pong message.
            Ok(WsMessage::Ping(msg)) => ctx.pong(&msg),
            // Normally we send status notifications to the client whenever endpoint
            // state changes; however the client may request a state notification, e.g.,
            // right after connection has been established.
            Ok(WsMessage::Text(cmd)) if cmd.trim() == "state" => {
                self.send_state(ctx, self.endpoint.state())
            }
            // Data buffer received in running state.  The first 8 bytes of the buffer
            // contain a client-assigned tag.  We acknowledge the message by sending a
            // 16-byte response, where the first 8 bytes contain a copy of the message tag;
            // and the rest of the message contains big-endian-encoded total message
            // length, including the 8-byte header.
            Ok(WsMessage::Binary(bin)) if self.endpoint.state() == PipelineState::Running => {
                debug!(
                    "HTTP input endpoint '{}': received {}-byte message",
                    self.endpoint.name(),
                    bin.len()
                );
                if bin.len() < 8 {
                    return;
                }
                // Acknowledgement message.
                let mut ack: [u8; 16] = [0; 16];
                self.endpoint.push_bytes(&bin[8..]);
                ack[0..8].copy_from_slice(&bin[0..8]);
                (&mut ack[8..16])
                    .write_u64::<BigEndian>(bin.len() as u64)
                    .unwrap();
                ctx.binary(Bytes::copy_from_slice(&ack));
            }
            // Data received in paused state.  Drop the message and send a negative
            // acknowledgement, consisting of message tag followed by 8 zero bytes.
            Ok(WsMessage::Binary(bin)) if self.endpoint.state() == PipelineState::Paused => {
                debug!(
                    "HTTP input endpoint '{}': rejecting {}-byte message in paused state",
                    self.endpoint.name(),
                    bin.len()
                );

                let mut ack: [u8; 16] = [0; 16];
                ack[0..8].copy_from_slice(&bin[0..8]);
                ctx.binary(Bytes::copy_from_slice(&ack));
            }
            // First frame of a multipart message.  Save message tag, so we can send an
            // acknowledgement after the last fragment has been received.
            Ok(WsMessage::Continuation(WsItem::FirstBinary(bin)))
                if self.endpoint.state() == PipelineState::Running =>
            {
                debug!(
                    "HTTP input endpoint '{}': received {}-byte first frame",
                    self.endpoint.name(),
                    bin.len()
                );

                if bin.len() < 8 {
                    return;
                }
                self.endpoint.push_bytes(&bin[8..]);
                self.message_tag.copy_from_slice(&bin[0..8]);
                self.message_len = bin.len();
            }
            // Middle frame of a multipart message.  `message_len > 0` indicates that
            // we accepted the first frame of the message and will continue receiving it
            // even if the endpoint has been paused since.
            Ok(WsMessage::Continuation(WsItem::Continue(bin))) if self.message_len > 0 => {
                debug!(
                    "HTTP input endpoint '{}': received {}-byte continuation frame",
                    self.endpoint.name(),
                    bin.len()
                );

                self.endpoint.push_bytes(&bin);
                // Add frame to message length.
                self.message_len += bin.len();
            }
            // Last frame of a multipart message.  Send acknowledgement using message
            // tag read from the first frame and total length of all frames.
            Ok(WsMessage::Continuation(WsItem::Last(bin))) if self.message_len > 0 => {
                debug!(
                    "HTTP input endpoint '{}': received {}-byte last frame",
                    self.endpoint.name(),
                    bin.len()
                );

                self.endpoint.push_bytes(&bin);
                self.message_len += bin.len();

                let mut ack: [u8; 16] = [0; 16];
                ack[0..8].copy_from_slice(&self.message_tag);
                (&mut ack[8..16])
                    .write_u64::<BigEndian>(self.message_len as u64)
                    .unwrap();
                self.message_len = 0;

                ctx.binary(Bytes::copy_from_slice(&ack));
            }
            // Reject multi-frame message in paused state.
            Ok(WsMessage::Continuation(WsItem::FirstBinary(bin)))
                if self.endpoint.state() == PipelineState::Paused =>
            {
                debug!(
                    "HTTP input endpoint '{}': rejecting {}-byte first frame in paused state",
                    self.endpoint.name(),
                    bin.len()
                );

                let mut ack: [u8; 16] = [0; 16];
                ack[0..8].copy_from_slice(&bin[0..8]);
                ctx.binary(Bytes::copy_from_slice(&ack));
            }
            // Client closed websocket.
            Ok(WsMessage::Close(reason)) => {
                info!(
                    "HTTP input endpoint '{}': websocket closed by client (reason: {reason:?})",
                    self.endpoint.name(),
                );
                ctx.stop()
            }
            _ => (),
        }
    }
}

impl Handler<EndpointStateNotification> for HttpInputWs {
    type Result = ();

    /// Handle endpoint state change notification.
    fn handle(&mut self, _msg: EndpointStateNotification, ctx: &mut Self::Context) {
        let state = self.endpoint.state();
        match state {
            PipelineState::Terminated => {
                info!(
                    "HTTP input endpoint '{}': closing websocket",
                    self.endpoint.name(),
                );

                // Notify sender.
                ctx.close(Some(WsCloseReason {
                    code: WsCloseCode::Away,
                    description: Some("pipeline terminating".to_string()),
                }));
                // Initiate shutdown; `Actor::stopped` will cleanup state when finished.
                ctx.stop();
            }
            state => {
                self.send_state(ctx, state);
            }
        }
    }
}
