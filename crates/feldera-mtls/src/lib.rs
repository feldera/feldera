//! Client certificate identities of Feldera components, and the server side of checking
//! them: the compiler, runner, pipeline and coordinator servers with
//! `--require-client-certificates`.
//!
//! The control plane presents a certificate with the URI SAN [`CONTROL_PLANE_CERTIFICATE_URI`],
//! a pipeline one with [`pipeline_certificate_uri`]. The private CA signs both. Pipelines may
//! also admit external clients, such as sidecars, by a URI SAN outside `urn:feldera:`.

use actix_tls::accept::rustls_0_23::TlsStream;
use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{Extensions, ServiceRequest, ServiceResponse};
use actix_web::http::{Method, StatusCode};
use actix_web::middleware::Next;
use actix_web::rt::net::TcpStream;
use actix_web::{HttpResponse, ResponseError};
use feldera_types::error::{DetailedError, ErrorResponse};
use rustls::RootCertStore;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;
use serde::Serialize;
use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

/// URI SAN of the control-plane client certificate (API server, runner, compiler replicas).
pub const CONTROL_PLANE_CERTIFICATE_URI: &str = "urn:feldera:control-plane";

/// Prefix of the URI SAN of a pipeline certificate. The pipeline id follows it.
const PIPELINE_CERTIFICATE_URI_PREFIX: &str = "urn:feldera:pipeline:";

/// Namespace of the URI SANs of Feldera components. URN namespaces are case-insensitive.
const FELDERA_URI_PREFIX: &str = "urn:feldera:";

fn is_feldera_uri(uri: &str) -> bool {
    uri.get(..FELDERA_URI_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(FELDERA_URI_PREFIX))
}

/// URI SAN of the certificate of pipeline `pipeline_id`.
pub fn pipeline_certificate_uri(pipeline_id: Uuid) -> String {
    format!("{PIPELINE_CERTIFICATE_URI_PREFIX}{pipeline_id}")
}

/// Who called a server, according to the client certificate of the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// No client certificate, or a certificate without exactly one identity.
    Anonymous,
    ControlPlane,
    Pipeline(Uuid),
    /// A client outside Feldera, by its URI SAN.
    External(String),
}

impl Caller {
    /// Maps a certificate to its caller. Only a certificate that the TLS handshake verified
    /// proves the identity.
    pub fn from_certificate(cert: &CertificateDer<'_>) -> Self {
        let Ok(cert) = webpki::EndEntityCert::try_from(cert) else {
            return Self::Anonymous;
        };
        let mut callers = cert.valid_uri_names().filter_map(|uri| {
            if uri == CONTROL_PLANE_CERTIFICATE_URI {
                Some(Self::ControlPlane)
            } else if let Some(id) = uri.strip_prefix(PIPELINE_CERTIFICATE_URI_PREFIX) {
                Uuid::parse_str(id).ok().map(Self::Pipeline)
            } else if is_feldera_uri(uri) {
                // Not an identity that Feldera issues, in any spelling.
                None
            } else {
                Some(Self::External(uri.to_string()))
            }
        });
        match (callers.next(), callers.next()) {
            (Some(caller), None) => caller,
            // A certificate with two identities is ambiguous, so it grants neither.
            _ => Self::Anonymous,
        }
    }

    /// Stores the caller of a new connection, for [`Caller::of`]. Pass it to
    /// `HttpServer::on_connect`.
    pub fn record(connection: &dyn Any, extensions: &mut Extensions) {
        let caller = connection
            .downcast_ref::<TlsStream<TcpStream>>()
            .and_then(|stream| stream.get_ref().1.peer_certificates())
            .and_then(|certs| certs.first())
            .map_or(Self::Anonymous, Self::from_certificate);
        extensions.insert(caller);
    }

    /// The caller of the connection that carried `req`.
    pub fn of(req: &ServiceRequest) -> Self {
        req.conn_data::<Self>().cloned().unwrap_or(Self::Anonymous)
    }
}

/// Runs `next` if `is_allowed` admits the request for the caller of its connection, and
/// refuses it with 401 (no identity) or 403 (another identity) otherwise.
pub async fn authorize<B: MessageBody>(
    server: &'static str,
    is_allowed: impl FnOnce(&Caller, &Method, &str) -> bool,
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, actix_web::Error> {
    let caller = Caller::of(&req);
    if is_allowed(&caller, req.method(), req.path()) {
        return next.call(req).await;
    }
    let method = req.method().to_string();
    let path = req.path().to_string();
    warn!("The {server} server refused {method} {path} from {caller:?}");
    Err(match caller {
        Caller::Anonymous => ClientAuthError::ClientCertificateRequired {
            server,
            method,
            path,
        },
        _ => ClientAuthError::CallerNotAllowed {
            server,
            caller: format!("{caller:?}"),
            method,
            path,
        },
    }
    .into())
}

/// A request that a server refused because of the client certificate of its connection.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ClientAuthError {
    ClientCertificateRequired {
        server: &'static str,
        method: String,
        path: String,
    },
    CallerNotAllowed {
        server: &'static str,
        caller: String,
        method: String,
        path: String,
    },
}

impl DetailedError for ClientAuthError {
    fn error_code(&self) -> Cow<'static, str> {
        match self {
            Self::ClientCertificateRequired { .. } => Cow::from("ClientCertificateRequired"),
            Self::CallerNotAllowed { .. } => Cow::from("CallerNotAllowed"),
        }
    }
}

impl fmt::Display for ClientAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientCertificateRequired {
                server,
                method,
                path,
            } => write!(
                f,
                "The {server} server requires a client certificate for {method} {path}: present a certificate that the private CA signed (control plane: --https-client-cert-path)"
            ),
            Self::CallerNotAllowed {
                server,
                caller,
                method,
                path,
            } => write!(
                f,
                "{caller} may not send {method} {path} to the {server} server"
            ),
        }
    }
}

impl std::error::Error for ClientAuthError {}

impl ResponseError for ClientAuthError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::ClientCertificateRequired { .. } => StatusCode::UNAUTHORIZED,
            Self::CallerNotAllowed { .. } => StatusCode::FORBIDDEN,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from_error_nolog(self))
    }
}

/// The rustls provider that the process installed, or the aws-lc-rs one that Feldera binaries
/// install. With several rustls crypto features compiled in, rustls has no default unless the
/// process installed one.
pub fn crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
}

/// Builds a client certificate verifier that trusts every certificate in `anchors`, so a
/// client may send its leaf certificate without the chain. Clients without a certificate may
/// connect, so that they get an actionable 401 or reach routes open to everyone.
pub fn client_certificate_verifier(
    anchors: impl IntoIterator<Item = CertificateDer<'static>>,
) -> Result<Arc<dyn ClientCertVerifier>, String> {
    let mut roots = RootCertStore::empty();
    for anchor in anchors {
        roots
            .add(anchor)
            .map_err(|e| format!("invalid CA certificate: {e}"))?;
    }
    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), crypto_provider())
        .allow_unauthenticated()
        .build()
        .map_err(|e| format!("client certificate verifier: {e}"))
}

/// The client verifier and admitted callers of a pipeline or coordinator server. Its
/// certificate chain names the pipeline in the leaf and ends with the private CA chain, which
/// the runner appends, so the server needs no separate CA file.
pub struct PipelineClientAuth {
    pub own: Caller,
    /// URI SANs of the external clients that the server admits like the control plane.
    pub extra_client_uris: Vec<String>,
    pub verifier: Arc<dyn ClientCertVerifier>,
}

impl PipelineClientAuth {
    pub fn from_chain(
        chain: &[CertificateDer<'static>],
        extra_client_uris: &[String],
    ) -> Result<Self, String> {
        // Feldera mints `urn:feldera:` URIs for its own components, so admitting one here
        // could admit another pipeline.
        if let Some(uri) = extra_client_uris
            .iter()
            .find(|uri| uri.is_empty() || uri.contains(char::is_whitespace) || is_feldera_uri(uri))
        {
            return Err(format!(
                "extra client URI '{uri}' must be non-empty, have no whitespace, and lie outside {FELDERA_URI_PREFIX}, which Feldera components use"
            ));
        }
        let (leaf, ca_chain) = chain
            .split_first()
            .ok_or("the certificate file holds no certificate")?;
        let own @ Caller::Pipeline(_) = Caller::from_certificate(leaf) else {
            return Err(format!(
                "the certificate names no pipeline: client certificates need the URI SAN {PIPELINE_CERTIFICATE_URI_PREFIX}<id>, which the runner adds"
            ));
        };
        if ca_chain.is_empty() {
            return Err(
                "the certificate file holds no CA chain after the leaf to verify clients with"
                    .to_string(),
            );
        }
        Ok(Self {
            own,
            extra_client_uris: extra_client_uris.to_vec(),
            verifier: client_certificate_verifier(ca_chain.iter().cloned())?,
        })
    }

    /// Whether a pipeline or coordinator server admits `caller`: the control plane, the
    /// pipeline itself (the coordinator calls the hosts of its pipeline), or a listed external
    /// client.
    pub fn is_allowed(&self, caller: &Caller) -> bool {
        match caller {
            Caller::ControlPlane => true,
            Caller::Pipeline(_) => *caller == self.own,
            Caller::External(uri) => self.extra_client_uris.contains(uri),
            Caller::Anonymous => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTROL_PLANE_CERTIFICATE_URI, Caller, PipelineClientAuth, pipeline_certificate_uri,
    };
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        SanType,
    };
    use rustls::pki_types::CertificateDer;
    use uuid::Uuid;

    const PIPELINE_A: Uuid = Uuid::from_u128(0xa);
    const PIPELINE_B: Uuid = Uuid::from_u128(0xb);

    fn ca() -> (Issuer<'static, KeyPair>, CertificateDer<'static>) {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (Issuer::new(params, key), cert.der().clone())
    }

    fn leaf(issuer: &Issuer<'static, KeyPair>, uris: &[String]) -> CertificateDer<'static> {
        let mut params = CertificateParams::default();
        params.subject_alt_names = uris
            .iter()
            .map(|uri| SanType::URI(uri.as_str().try_into().unwrap()))
            .collect();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let key = KeyPair::generate().unwrap();
        params.signed_by(&key, issuer).unwrap().der().clone()
    }

    fn caller_of(uris: &[String]) -> Caller {
        Caller::from_certificate(&leaf(&ca().0, uris))
    }

    const SIDECAR: &str = "urn:example:sidecar";

    /// Certificate URIs map to callers, and a certificate with two identities grants neither.
    #[test]
    fn caller_from_certificate() {
        let control_plane = CONTROL_PLANE_CERTIFICATE_URI.to_string();
        let a = pipeline_certificate_uri(PIPELINE_A);
        let b = pipeline_certificate_uri(PIPELINE_B);
        let sidecar = SIDECAR.to_string();
        assert_eq!(
            caller_of(std::slice::from_ref(&control_plane)),
            Caller::ControlPlane
        );
        assert_eq!(
            caller_of(std::slice::from_ref(&a)),
            Caller::Pipeline(PIPELINE_A)
        );
        assert_eq!(caller_of(&[]), Caller::Anonymous);
        assert_eq!(
            caller_of(&["urn:feldera:pipeline:not-a-uuid".to_string()]),
            Caller::Anonymous
        );
        assert_eq!(
            caller_of(std::slice::from_ref(&sidecar)),
            Caller::External(SIDECAR.to_string())
        );
        assert_eq!(
            caller_of(&[format!("URN:Feldera:pipeline:{PIPELINE_A}")]),
            Caller::Anonymous
        );
        assert_eq!(
            caller_of(&["urn:feldera:sidecar".to_string()]),
            Caller::Anonymous
        );
        assert_eq!(
            caller_of(&[control_plane.clone(), a.clone()]),
            Caller::Anonymous
        );
        assert_eq!(caller_of(&[control_plane, sidecar]), Caller::Anonymous);
        assert_eq!(caller_of(&[a, b]), Caller::Anonymous);
    }

    /// A pipeline server admits the control plane, its own pipeline and the listed external
    /// clients, and needs a CA chain and a pipeline identity in its own certificate file.
    #[test]
    fn pipeline_client_auth() {
        let (issuer, ca_cert) = ca();
        let own_leaf = leaf(&issuer, &[pipeline_certificate_uri(PIPELINE_A)]);
        let chain = [own_leaf.clone(), ca_cert.clone()];
        let auth = PipelineClientAuth::from_chain(&chain, &[SIDECAR.to_string()]).unwrap();
        assert!(auth.is_allowed(&Caller::ControlPlane));
        assert!(auth.is_allowed(&Caller::Pipeline(PIPELINE_A)));
        assert!(auth.is_allowed(&Caller::External(SIDECAR.to_string())));
        assert!(!auth.is_allowed(&Caller::External("urn:example:other".to_string())));
        assert!(!auth.is_allowed(&Caller::Pipeline(PIPELINE_B)));
        assert!(!auth.is_allowed(&Caller::Anonymous));

        let no_external = PipelineClientAuth::from_chain(&chain, &[]).unwrap();
        assert!(!no_external.is_allowed(&Caller::External(SIDECAR.to_string())));

        for invalid in [
            pipeline_certificate_uri(PIPELINE_B),
            format!("URN:FELDERA:pipeline:{PIPELINE_B}"),
            String::new(),
            "urn:example:a urn:example:b".to_string(),
        ] {
            assert!(
                PipelineClientAuth::from_chain(&chain, std::slice::from_ref(&invalid)).is_err(),
                "{invalid}"
            );
        }

        assert!(PipelineClientAuth::from_chain(&[own_leaf], &[]).is_err());
        let server_leaf = leaf(&issuer, &[]);
        assert!(PipelineClientAuth::from_chain(&[server_leaf, ca_cert], &[]).is_err());
    }
}
