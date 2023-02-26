use super::MAX_SOCKETS_PER_ENDPOINT;
use crate::{OutputEndpoint, OutputTransport};
use actix::{Actor, ActorContext, Addr, AsyncContext, Handler, Message, StreamHandler};
use actix_web::{web::Payload, HttpRequest, HttpResponse};
use actix_web_actors::ws::{
    self, Message as WsMessage, ProtocolError as WsProtocolError, WebsocketContext,
};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use futures::executor::block_on;
use log::{debug, info};
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    sync::{Arc, RwLock},
};
use utoipa::ToSchema;

/// Global map of output HTTP endpoints.
static OUTPUT_HTTP_ENDPOINTS: Lazy<RwLock<BTreeMap<String, HttpOutputEndpoint>>> =
    Lazy::new(|| RwLock::new(BTreeMap::new()));

/// `OutputTransport` implementation that sends data to websockets.
pub struct HttpOutputTransport;

impl OutputTransport for HttpOutputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("http")
    }

    fn new_endpoint(
        &self,
        name: &str,
        _config: &YamlValue,
        async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<Box<dyn OutputEndpoint>> {
        // let _config = HttpOutputConfig::deserialize(config)?;
        let ep = HttpOutputEndpoint::new(name, async_error_callback)?;
        Ok(Box::new(ep))
    }
}

impl HttpOutputTransport {
    pub(crate) fn get_endpoint_websocket(
        endpoint_name: &str,
        req: &HttpRequest,
        stream: Payload,
    ) -> AnyResult<HttpResponse> {
        let endpoint = OUTPUT_HTTP_ENDPOINTS
            .read()
            .unwrap()
            .get(endpoint_name)
            .map(Clone::clone)
            .ok_or_else(|| anyhow!("unknown HTTP output endpoint '{endpoint_name}'"))?;
        if endpoint.num_sockets() >= MAX_SOCKETS_PER_ENDPOINT {
            return Err(anyhow!(
                "maximum number of connections per HTTP endpoint exceeded"
            ));
        }
        let resp = ws::start(HttpOutputWs::new(endpoint), req, stream)
            .map_err(|e| anyhow!(format!("error initializing websocket: {e}")))?;
        info!("HTTP output endpoint '{endpoint_name}': opened websocket");
        Ok(resp)
    }
}

#[derive(Clone, Deserialize, ToSchema)]
pub struct HttpOutputConfig;

struct HttpOutputEndpointInner {
    name: String,

    /// Addresses of websocket actors associated with the endpoint.
    ///
    /// HTTP endpoint supports up to MAX_SOCKETS_PER_ENDPOINT simultaneously
    /// open websockets.  The client opens a new websocket by issuing a GET
    /// request to the `/output_endpoint/{endpoint_name}` endpoint.  The
    /// websocket is removed from this set when client connection closes.
    ///
    /// This field is used to notify all websocket actors about new data
    /// buffers to send out.
    socket_addrs: RwLock<HashSet<Addr<HttpOutputWs>>>,
    _async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
}

impl HttpOutputEndpointInner {
    fn new(name: &str, async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>) -> Self {
        Self {
            name: name.to_string(),
            socket_addrs: RwLock::new(HashSet::new()),
            _async_error_callback: async_error_callback,
        }
    }
}

/// Output endpoint that establishes websocket connections with clients on
/// demand and sends output batches to these websockets.
///
/// This implementation provides no support for reliable delivery
/// and is mostly intended for browser-based testing.
#[derive(Clone)]
struct HttpOutputEndpoint {
    inner: Arc<HttpOutputEndpointInner>,
}

impl HttpOutputEndpoint {
    fn new(
        name: &str,
        async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<Self> {
        let mut endpoint_map = OUTPUT_HTTP_ENDPOINTS.write().unwrap();

        if endpoint_map.contains_key(name) {
            return Err(anyhow!(format!(
                "duplicate HTTP output endpoint name '{name}'"
            )));
        }

        let endpoint = Self {
            inner: Arc::new(HttpOutputEndpointInner::new(name, async_error_callback)),
        };

        endpoint_map.insert(name.to_string(), endpoint.clone());
        Ok(endpoint)
    }

    fn name(&self) -> &str {
        self.inner.name.as_str()
    }

    /// Number of connected websockets.
    fn num_sockets(&self) -> usize {
        self.inner.socket_addrs.read().unwrap().len()
    }

    /// Register new websocket actor.
    fn add_socket(&self, addr: Addr<HttpOutputWs>) {
        self.inner.socket_addrs.write().unwrap().insert(addr);
    }

    /// Remove closed websocket.
    fn remove_socket(&self, addr: &Addr<HttpOutputWs>) {
        self.inner.socket_addrs.write().unwrap().remove(addr);
    }
}

impl OutputEndpoint for HttpOutputEndpoint {
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        for addr in self.inner.socket_addrs.read().unwrap().iter() {
            block_on(addr.send(Event::Buffer(Vec::from(buffer))))?;
        }
        Ok(())
    }
}

#[derive(Message)]
#[rtype(result = "()")]
enum Event {
    Buffer(Vec<u8>),
}

/// Actix actor that handles websocket communication.
struct HttpOutputWs {
    endpoint: HttpOutputEndpoint,
}

impl HttpOutputWs {
    fn new(endpoint: HttpOutputEndpoint) -> Self {
        Self { endpoint }
    }
}

impl Actor for HttpOutputWs {
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

impl StreamHandler<Result<WsMessage, WsProtocolError>> for HttpOutputWs {
    /// Handle websocket message from the client.
    fn handle(&mut self, msg: Result<WsMessage, WsProtocolError>, ctx: &mut Self::Context) {
        match msg {
            // The client may check our heartbeat by sending ping messages.
            // Reply with a pong message.
            Ok(WsMessage::Ping(msg)) => ctx.pong(&msg),
            Ok(WsMessage::Close(reason)) => {
                info!(
                    "HTTP output endpoint '{}': websocket closed by client (reason: {reason:?})",
                    self.endpoint.name(),
                );
                ctx.stop()
            }
            _ => (),
        }
    }
}

impl Handler<Event> for HttpOutputWs {
    type Result = ();

    /// Handle new outgoing data buffer.
    fn handle(&mut self, msg: Event, ctx: &mut Self::Context) {
        match msg {
            Event::Buffer(buf) => {
                debug!(
                    "HTTP output endpoint '{}': sending {} bytes",
                    self.endpoint.name(),
                    buf.len(),
                );

                ctx.binary(buf);
            }
        }
    }
}
