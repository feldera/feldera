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
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::verify_server_cert_signed_by_trust_anchor;
use rustls::crypto::{CryptoProvider, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};
use serde::Serialize;
use std::any::Any;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

/// Environment variable that makes the compiler, runner, pipeline and coordinator servers
/// require client certificates.
pub const REQUIRE_CLIENT_CERTIFICATES_ENV: &str = "FELDERA_REQUIRE_CLIENT_CERTIFICATES";

/// Environment variable with the URI SANs of the extra clients that pipelines admit.
pub const EXTRA_CLIENT_CERTIFICATE_URIS_ENV: &str = "FELDERA_EXTRA_CLIENT_CERTIFICATE_URIS";

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

/// Builds a client certificate verifier that trusts only the first certificate of `ca_chain`,
/// the private CA that issues client certificates, so a client may send its leaf certificate
/// without the chain. The rest of the chain is not trusted, because other CAs under the same
/// root could issue Feldera identities. Clients without a certificate may connect, so that
/// they get an actionable 401 or reach routes open to everyone.
pub fn client_certificate_verifier(
    ca_chain: &[CertificateDer<'static>],
) -> Result<Arc<dyn ClientCertVerifier>, String> {
    let issuing_ca = ca_chain
        .first()
        .ok_or("the CA chain holds no certificate")?;
    let mut roots = RootCertStore::empty();
    roots
        .add(issuing_ca.clone())
        .map_err(|e| format!("invalid CA certificate: {e}"))?;
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
            verifier: client_certificate_verifier(ca_chain)?,
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

/// TLS configurations for the connections between the hosts of a multihost pipeline.
#[derive(Clone, Debug)]
pub struct PipelinePeerTls {
    pub client: Arc<ClientConfig>,
    pub server: Arc<ServerConfig>,
}

impl PipelinePeerTls {
    /// Builds the configurations from the pipeline certificate chain and its key. Both ends
    /// present the pipeline certificate and admit only a peer whose certificate the private
    /// CA signed for the same pipeline. Peers connect by IP address, so the server name is
    /// not checked: the pipeline URI SAN is the identity.
    pub fn from_chain(
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, String> {
        let verifier = Arc::new(PipelinePeerVerifier::from_chain(&chain)?);
        let provider = verifier.provider.clone();
        let no_tls13 = |e| format!("the crypto provider does not support TLS 1.3: {e}");
        let bad_key = |e| {
            format!(
                "the private key does not match the pipeline certificate, or its type is not supported: {e}"
            )
        };
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(no_tls13)?
            .with_client_cert_verifier(verifier.clone())
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(bad_key)?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(no_tls13)?
            // rustls puts every custom server certificate verifier behind `dangerous()`,
            // because a custom verifier can skip checks. This one is needed because the
            // built-in verifier identifies a server only by its DNS name or IP address, and
            // hosts connect by an IP address that no certificate names. It skips nothing
            // but that name check, and checks the pipeline URI SAN instead (see
            // `verify_server_cert` below).
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(chain, key)
            .map_err(bad_key)?;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}

/// Verifies the certificate of another host of the same pipeline, on both ends of a
/// connection.
#[derive(Debug)]
struct PipelinePeerVerifier {
    own: Caller,
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
    client_verifier: Arc<dyn ClientCertVerifier>,
}

impl PipelinePeerVerifier {
    /// Admits peers with the pipeline identity of the leaf of `chain`, signed by the private
    /// CA, the first certificate after the leaf.
    fn from_chain(chain: &[CertificateDer<'static>]) -> Result<Self, String> {
        let (leaf, ca_chain) = chain
            .split_first()
            .ok_or("the certificate file holds no certificate")?;
        let own @ Caller::Pipeline(_) = Caller::from_certificate(leaf) else {
            return Err(format!(
                "the certificate names no pipeline: peer certificates need the URI SAN {PIPELINE_CERTIFICATE_URI_PREFIX}<id>, which the runner adds"
            ));
        };
        let issuing_ca = ca_chain
            .first()
            .ok_or("the certificate file holds no CA chain after the leaf to verify peers with: the runner appends the chain of the private CA (Helm value `caSecretRef`)")?;
        let mut roots = RootCertStore::empty();
        roots
            .add(issuing_ca.clone())
            .map_err(|e| format!("invalid CA certificate: {e}"))?;
        let roots = Arc::new(roots);
        let provider = crypto_provider();
        Ok(Self {
            own,
            client_verifier: WebPkiClientVerifier::builder_with_provider(
                roots.clone(),
                provider.clone(),
            )
            .build()
            .map_err(|e| format!("peer certificate verifier: {e}"))?,
            roots,
            provider,
        })
    }

    fn check_identity(&self, end_entity: &CertificateDer<'_>) -> Result<(), rustls::Error> {
        let caller = Caller::from_certificate(end_entity);
        if caller == self.own {
            Ok(())
        } else {
            Err(rustls::Error::General(format!(
                "peer certificate identifies {caller:?}, not this pipeline ({:?})",
                self.own
            )))
        }
    }
}

/// Peer configurations allow only TLS 1.3, so rustls never verifies a TLS 1.2 signature
/// with them. The verifier refuses one anyway.
fn tls12_refused() -> rustls::Error {
    rustls::Error::General("exchange between hosts allows only TLS 1.3".to_string())
}

impl ServerCertVerifier for PipelinePeerVerifier {
    /// Does what rustls's `WebPkiServerVerifier` does, which checks the chain to the
    /// private CA, the validity period and the server auth extended key usage, except
    /// that it checks the pipeline identity instead of `server_name`. Like
    /// `WebPkiServerVerifier` without CRLs, which Feldera does not use, it does not
    /// check revocation, and it ignores OCSP responses.
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        verify_server_cert_signed_by_trust_anchor(
            &ParsedCertificate::try_from(end_entity)?,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        self.check_identity(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(tls12_refused())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl ClientCertVerifier for PipelinePeerVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.client_verifier.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .client_verifier
            .verify_client_cert(end_entity, intermediates, now)?;
        self.check_identity(end_entity)?;
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(tls12_refused())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.client_verifier
            .verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.client_verifier.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::crypto_provider;
    use super::{
        CONTROL_PLANE_CERTIFICATE_URI, Caller, PipelineClientAuth, PipelinePeerTls,
        PipelinePeerVerifier, client_certificate_verifier, pipeline_certificate_uri,
    };
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, SanType,
    };
    use rustls::pki_types::{
        CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
    };
    use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};
    use std::sync::Arc;
    use uuid::Uuid;

    const PIPELINE_A: Uuid = Uuid::from_u128(0xa);
    const PIPELINE_B: Uuid = Uuid::from_u128(0xb);

    fn ca() -> (Issuer<'static, KeyPair>, CertificateDer<'static>) {
        named_ca("ca", None)
    }

    /// A CA certificate signed by `parent`, or self-signed without one.
    fn named_ca(
        name: &str,
        parent: Option<&Issuer<'static, KeyPair>>,
    ) -> (Issuer<'static, KeyPair>, CertificateDer<'static>) {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let cert = match parent {
            Some(parent) => params.signed_by(&key, parent),
            None => params.self_signed(&key),
        }
        .unwrap();
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

    /// Servers trust client certificates from the private CA, the first certificate of the CA
    /// chain, and refuse ones from its root or from another CA under that root.
    #[test]
    fn verifier_trusts_only_private_ca() {
        let (root, root_cert) = named_ca("root", None);
        let (private_ca, private_ca_cert) = named_ca("private", Some(&root));
        let (other_ca, other_ca_cert) = named_ca("other", Some(&root));
        let own_leaf = leaf(&private_ca, &[pipeline_certificate_uri(PIPELINE_A)]);
        let pipeline_chain = [own_leaf, private_ca_cert.clone(), root_cert.clone()];
        let ca_chain = [private_ca_cert, root_cert];
        let control_plane = [CONTROL_PLANE_CERTIFICATE_URI.to_string()];
        let now = UnixTime::now();
        for verifier in [
            client_certificate_verifier(&ca_chain).unwrap(),
            PipelineClientAuth::from_chain(&pipeline_chain, &[])
                .unwrap()
                .verifier,
        ] {
            let from_private_ca = leaf(&private_ca, &control_plane);
            assert!(
                verifier
                    .verify_client_cert(&from_private_ca, &[], now)
                    .is_ok()
            );
            let from_root = leaf(&root, &control_plane);
            assert!(verifier.verify_client_cert(&from_root, &[], now).is_err());
            let from_other_ca = leaf(&other_ca, &control_plane);
            let other_chain = std::slice::from_ref(&other_ca_cert);
            assert!(
                verifier
                    .verify_client_cert(&from_other_ca, other_chain, now)
                    .is_err()
            );
        }
    }

    /// A pipeline certificate chain with server and client auth, as the runner issues it: the
    /// leaf, then `ca_chain`.
    fn peer_chain(
        issuer: &Issuer<'static, KeyPair>,
        ca_chain: &[CertificateDer<'static>],
        uri: &str,
    ) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![SanType::URI(uri.try_into().unwrap())];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key = KeyPair::generate().unwrap();
        let leaf = params.signed_by(&key, issuer).unwrap().der().clone();
        let chain = std::iter::once(leaf)
            .chain(ca_chain.iter().cloned())
            .collect();
        (chain, PrivatePkcs8KeyDer::from(key.serialize_der()).into())
    }

    fn peer_tls(
        issuer: &Issuer<'static, KeyPair>,
        ca_chain: &[CertificateDer<'static>],
        uri: &str,
    ) -> PipelinePeerTls {
        let (chain, key) = peer_chain(issuer, ca_chain, uri);
        PipelinePeerTls::from_chain(chain, key).unwrap()
    }

    /// Runs a TLS handshake between `client` and `server` in memory.
    fn handshake(
        client: Arc<ClientConfig>,
        server: Arc<ServerConfig>,
    ) -> Result<(), rustls::Error> {
        let mut client = ClientConnection::new(client, ServerName::try_from("127.0.0.1").unwrap())?;
        let mut server = ServerConnection::new(server)?;
        for _ in 0..10 {
            let mut buf = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut buf).unwrap();
            }
            server.read_tls(&mut buf.as_slice()).unwrap();
            server.process_new_packets()?;

            let mut buf = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut buf).unwrap();
            }
            client.read_tls(&mut buf.as_slice()).unwrap();
            client.process_new_packets()?;

            if !client.is_handshaking() && !server.is_handshaking() {
                return Ok(());
            }
        }
        panic!("handshake did not finish");
    }

    /// Hosts of one pipeline connect to each other. A host refuses a peer that is another
    /// pipeline, the control plane, has no certificate, or has a certificate from a CA other
    /// than the private CA, as client and as server.
    #[test]
    fn peer_tls_admits_only_own_pipeline() {
        let (root, root_cert) = named_ca("root", None);
        let (private_ca, private_ca_cert) = named_ca("private", Some(&root));
        let (other_ca, other_ca_cert) = named_ca("other", Some(&root));
        let ca_chain = [private_ca_cert, root_cert.clone()];
        let a = pipeline_certificate_uri(PIPELINE_A);
        let b = pipeline_certificate_uri(PIPELINE_B);

        let host0 = peer_tls(&private_ca, &ca_chain, &a);
        let host1 = peer_tls(&private_ca, &ca_chain, &a);
        handshake(host0.client.clone(), host1.server.clone()).unwrap();
        handshake(host1.client.clone(), host0.server.clone()).unwrap();

        // A client that admits host 0 as its server, so that only host 0 can refuse. It
        // needs `dangerous()` for the same reason as `PipelinePeerTls::from_chain`.
        let verifier = Arc::new(
            PipelinePeerVerifier::from_chain(&peer_chain(&private_ca, &ca_chain, &a).0).unwrap(),
        );
        let client_builder = || {
            ClientConfig::builder_with_provider(crypto_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(verifier.clone())
        };
        let anonymous = client_builder().with_no_client_auth();
        assert!(
            handshake(Arc::new(anonymous), host0.server.clone()).is_err(),
            "server admitted a client without a certificate"
        );

        let refused = [
            ("another pipeline", peer_chain(&private_ca, &ca_chain, &b)),
            (
                "the control plane",
                peer_chain(&private_ca, &ca_chain, CONTROL_PLANE_CERTIFICATE_URI),
            ),
            (
                "the same pipeline from a sibling CA",
                peer_chain(&other_ca, &[other_ca_cert, root_cert.clone()], &a),
            ),
            (
                "the same pipeline from the root CA",
                peer_chain(&root, std::slice::from_ref(&root_cert), &a),
            ),
        ];
        for (name, (chain, key)) in refused {
            let client = client_builder()
                .with_client_auth_cert(chain.clone(), key.clone_key())
                .unwrap();
            assert!(
                handshake(Arc::new(client), host0.server.clone()).is_err(),
                "server admitted {name}"
            );

            // A server that requires no client certificate, so that only host 0 can refuse.
            let server = ServerConfig::builder_with_provider(crypto_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(chain, key)
                .unwrap();
            assert!(
                handshake(host0.client.clone(), Arc::new(server)).is_err(),
                "client admitted {name}"
            );
        }
    }

    /// Peer configurations need a pipeline certificate followed by the CA chain, and the key
    /// of that certificate.
    #[test]
    fn peer_tls_needs_pipeline_chain() {
        let (issuer, ca_cert) = ca();
        let ca_chain = [ca_cert];
        let a = pipeline_certificate_uri(PIPELINE_A);
        let (chain, key) = peer_chain(&issuer, &[], &a);
        assert!(PipelinePeerTls::from_chain(chain, key).is_err());
        let (chain, key) = peer_chain(&issuer, &ca_chain, CONTROL_PLANE_CERTIFICATE_URI);
        assert!(PipelinePeerTls::from_chain(chain, key).is_err());

        let (chain, _) = peer_chain(&issuer, &ca_chain, &a);
        let (_, other_key) = peer_chain(&issuer, &ca_chain, &a);
        let error = PipelinePeerTls::from_chain(chain, other_key).unwrap_err();
        assert!(
            error.contains("does not match the pipeline certificate"),
            "{error}"
        );
    }

    /// Hosts refuse TLS 1.2, as client and as server.
    #[test]
    fn peer_tls_refuses_tls12() {
        let (issuer, ca_cert) = ca();
        let ca_chain = [ca_cert];
        let a = pipeline_certificate_uri(PIPELINE_A);
        let host = peer_tls(&issuer, &ca_chain, &a);
        let verifier = Arc::new(
            PipelinePeerVerifier::from_chain(&peer_chain(&issuer, &ca_chain, &a).0).unwrap(),
        );

        let (chain, key) = peer_chain(&issuer, &ca_chain, &a);
        let client = ClientConfig::builder_with_provider(crypto_provider())
            .with_protocol_versions(&[&rustls::version::TLS12])
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(verifier.clone())
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .unwrap();
        let server = ServerConfig::builder_with_provider(crypto_provider())
            .with_protocol_versions(&[&rustls::version::TLS12])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .unwrap();

        // The handshake fails on the protocol version, before any certificate check.
        for (name, result) in [
            ("server", handshake(Arc::new(client), host.server.clone())),
            ("client", handshake(host.client.clone(), Arc::new(server))),
        ] {
            assert!(
                matches!(result, Err(rustls::Error::PeerIncompatible(_))),
                "{name} allowed TLS 1.2: {result:?}"
            );
        }
    }
}
