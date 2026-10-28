//! Client certificate authentication and route authorization of the compiler server
//! (`--require-client-certificates`).

use crate::compiler::error::CompilerError;
use crate::config::CommonConfig;
use crate::db::types::pipeline::PipelineId;
use actix_tls::accept::rustls_0_23::TlsStream;
use actix_web::body::MessageBody;
use actix_web::dev::{Extensions, ServiceRequest, ServiceResponse};
use actix_web::http::Method;
use actix_web::middleware::Next;
use actix_web::rt::net::TcpStream;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;
use std::any::Any;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

/// URI SAN of the control-plane client certificate (API server, runner, compiler replicas).
pub const CONTROL_PLANE_CERTIFICATE_URI: &str = "urn:feldera:control-plane";

/// Prefix of the URI SAN of a pipeline certificate. The pipeline id follows it.
const PIPELINE_CERTIFICATE_URI_PREFIX: &str = "urn:feldera:pipeline:";

/// URI SAN of the certificate of pipeline `pipeline_id`.
pub fn pipeline_certificate_uri(pipeline_id: PipelineId) -> String {
    format!("{PIPELINE_CERTIFICATE_URI_PREFIX}{pipeline_id}")
}

/// Who called the compiler server, according to the client certificate of the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompilerCaller {
    /// No client certificate, or a certificate without exactly one Feldera identity.
    Anonymous,
    ControlPlane,
    Pipeline(PipelineId),
}

impl CompilerCaller {
    /// Maps a client certificate, which the TLS handshake verified, to its caller.
    fn from_certificate(cert: &CertificateDer<'_>) -> Self {
        let Ok(cert) = webpki::EndEntityCert::try_from(cert) else {
            return Self::Anonymous;
        };
        let mut callers = cert.valid_uri_names().filter_map(|uri| {
            if uri == CONTROL_PLANE_CERTIFICATE_URI {
                Some(Self::ControlPlane)
            } else {
                uri.strip_prefix(PIPELINE_CERTIFICATE_URI_PREFIX)
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .map(|id| Self::Pipeline(PipelineId(id)))
            }
        });
        match (callers.next(), callers.next()) {
            (Some(caller), None) => caller,
            // A certificate with two identities is ambiguous, so it grants neither.
            _ => Self::Anonymous,
        }
    }

    /// Stores the caller of a new connection for [`authorize_compiler_caller`].
    pub(crate) fn record(connection: &dyn Any, extensions: &mut Extensions) {
        let caller = connection
            .downcast_ref::<TlsStream<TcpStream>>()
            .and_then(|stream| stream.get_ref().1.peer_certificates())
            .and_then(|certs| certs.first())
            .map_or(Self::Anonymous, Self::from_certificate);
        extensions.insert(caller);
    }
}

/// Decides whether `caller` may send a `method` request for `path` to the compiler server.
/// Routes that are not listed are for the control plane only, so new routes start closed.
fn is_request_allowed(caller: CompilerCaller, method: &Method, path: &str) -> bool {
    if caller == CompilerCaller::ControlPlane {
        return true;
    }
    if method == Method::GET && path == "/healthz" {
        return true;
    }
    let CompilerCaller::Pipeline(own_id) = caller else {
        return false;
    };
    // GET /binary/{pipeline_id}/... and GET /program_info/{pipeline_id}/...
    let mut segments = path.strip_prefix('/').unwrap_or(path).split('/');
    method == Method::GET
        && matches!(segments.next(), Some("binary" | "program_info"))
        && segments.next().and_then(|id| Uuid::parse_str(id).ok()) == Some(own_id.0)
}

/// Refuses the requests that [`is_request_allowed`] does not allow for the caller of the
/// connection.
pub(crate) async fn authorize_compiler_caller(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let caller = req
        .conn_data::<CompilerCaller>()
        .copied()
        .unwrap_or(CompilerCaller::Anonymous);
    if is_request_allowed(caller, req.method(), req.path()) {
        return next.call(req).await;
    }
    let method = req.method().to_string();
    let path = req.path().to_string();
    warn!("Compiler server refused {method} {path} from {caller:?}");
    Err(match caller {
        CompilerCaller::Anonymous => CompilerError::ClientCertificateRequired { method, path },
        _ => CompilerError::CallerNotAllowed {
            caller: format!("{caller:?}"),
            method,
            path,
        },
    }
    .into())
}

/// Builds the client certificate verifier of the compiler server. Every certificate in the
/// private CA bundle is a trust anchor, so a client may send its leaf certificate without
/// the chain. Clients without a certificate may connect, for `/healthz`.
pub(crate) fn client_certificate_verifier(
    common_config: &CommonConfig,
) -> Arc<dyn ClientCertVerifier> {
    assert!(
        common_config.enable_https,
        "--require-client-certificates requires --enable-https"
    );
    let ca_path = common_config.private_ca_cert_path.as_ref().expect(
        "--require-client-certificates requires --private-ca-cert-path, the CA that signs client certificates",
    );
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(ca_path)
        .unwrap_or_else(|e| panic!("private CA certificate '{ca_path}' should be read: {e}"))
    {
        let cert =
            cert.unwrap_or_else(|e| panic!("private CA certificate '{ca_path}' should parse: {e}"));
        roots
            .add(cert)
            .unwrap_or_else(|e| panic!("private CA certificate '{ca_path}' should be valid: {e}"));
    }
    WebPkiClientVerifier::builder(Arc::new(roots))
        .allow_unauthenticated()
        .build()
        .unwrap_or_else(|e| panic!("client certificate verifier should be built: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{
        CONTROL_PLANE_CERTIFICATE_URI, CompilerCaller, authorize_compiler_caller,
        client_certificate_verifier, is_request_allowed, pipeline_certificate_uri,
    };
    use crate::compiler::main::{check_compilation_artifacts, get_binary, upload_binary};
    use crate::compiler::util::pipeline_binary_filename;
    use crate::config::{CommonConfig, CompilerConfig};
    use crate::db::types::pipeline::PipelineId;
    use crate::db::types::program::CompilationProfile;
    use crate::db::types::version::Version;
    use actix_web::http::{Method, StatusCode};
    use actix_web::middleware::from_fn;
    use actix_web::{App, HttpResponse, HttpServer, web};
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        SanType,
    };
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    use std::net::TcpListener;
    use std::path::Path;
    use uuid::Uuid;

    const PIPELINE_A: PipelineId = PipelineId(Uuid::from_u128(0xa));
    const PIPELINE_B: PipelineId = PipelineId(Uuid::from_u128(0xb));
    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    struct TestCa {
        issuer: Issuer<'static, KeyPair>,
        pem: String,
    }

    fn test_ca(name: &str) -> TestCa {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let key = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        TestCa {
            pem: cert.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    /// Issues a certificate with these SANs and returns (certificate chain PEM, key PEM).
    fn issue(ca: &TestCa, sans: Vec<SanType>, eku: ExtendedKeyUsagePurpose) -> (String, String) {
        let mut params = CertificateParams::default();
        params.subject_alt_names = sans;
        params.extended_key_usages = vec![eku];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &ca.issuer).unwrap();
        (format!("{}{}", cert.pem(), ca.pem), key.serialize_pem())
    }

    fn uri(uri: &str) -> SanType {
        SanType::URI(uri.try_into().unwrap())
    }

    fn caller_of(sans: Vec<SanType>) -> CompilerCaller {
        let (chain, _) = issue(&test_ca("ca"), sans, ExtendedKeyUsagePurpose::ClientAuth);
        let leaf = CertificateDer::from_pem_slice(chain.as_bytes()).unwrap();
        CompilerCaller::from_certificate(&leaf)
    }

    /// Certificate URIs map to callers, and a certificate with two identities grants neither.
    #[test]
    fn caller_from_certificate() {
        assert_eq!(
            caller_of(vec![uri(CONTROL_PLANE_CERTIFICATE_URI)]),
            CompilerCaller::ControlPlane
        );
        assert_eq!(
            caller_of(vec![
                SanType::DnsName("localhost".try_into().unwrap()),
                uri(&pipeline_certificate_uri(PIPELINE_A)),
            ]),
            CompilerCaller::Pipeline(PIPELINE_A)
        );
        assert_eq!(
            caller_of(vec![SanType::DnsName("localhost".try_into().unwrap())]),
            CompilerCaller::Anonymous
        );
        assert_eq!(
            caller_of(vec![uri("urn:feldera:pipeline:not-a-uuid")]),
            CompilerCaller::Anonymous
        );
        assert_eq!(
            caller_of(vec![
                uri(CONTROL_PLANE_CERTIFICATE_URI),
                uri(&pipeline_certificate_uri(PIPELINE_A)),
            ]),
            CompilerCaller::Anonymous
        );
        assert_eq!(
            caller_of(vec![
                uri(&pipeline_certificate_uri(PIPELINE_A)),
                uri(&pipeline_certificate_uri(PIPELINE_B)),
            ]),
            CompilerCaller::Anonymous
        );
    }

    /// Each caller reaches only the routes of the compiler server route table.
    #[test]
    fn route_table() {
        use CompilerCaller::{Anonymous, ControlPlane, Pipeline};
        let a = PIPELINE_A.to_string();
        let b = PIPELINE_B.to_string();
        let binary_a = format!("/binary/{a}/1/{SHA}/{SHA}");
        let binary_b = format!("/binary/{b}/1/{SHA}/{SHA}");
        let info_a = format!("/program_info/{a}/1/{SHA}/{SHA}");
        let info_b = format!("/program_info/{b}/1/{SHA}/{SHA}");
        let artifacts_a = format!("/artifacts/{a}/1/{SHA}/{SHA}/{SHA}");
        let pipeline_a = Pipeline(PIPELINE_A);

        #[rustfmt::skip]
        let table: Vec<(Method, &str, [bool; 3])> = vec![
            //                                 control plane, pipeline A, anonymous
            (Method::GET,  &binary_a,           [true, true,  false]),
            (Method::GET,  &binary_b,           [true, false, false]),
            (Method::GET,  &info_a,             [true, true,  false]),
            (Method::GET,  &info_b,             [true, false, false]),
            (Method::GET,  &artifacts_a,        [true, false, false]),
            (Method::POST, &binary_a,           [true, false, false]),
            (Method::POST, &info_a,             [true, false, false]),
            (Method::POST, "/validate_program", [true, false, false]),
            (Method::GET,  "/healthz",          [true, true,  true]),
            (Method::GET,  "/unknown",          [true, false, false]),
            (Method::GET,  "/binary/A/1/x/y",   [true, false, false]),
        ];
        for (method, path, expected) in table {
            for (caller, allowed) in [ControlPlane, pipeline_a, Anonymous]
                .into_iter()
                .zip(expected)
            {
                assert_eq!(
                    is_request_allowed(caller, &method, path),
                    allowed,
                    "{caller:?} {method} {path}"
                );
            }
        }
    }

    fn write(dir: &Path, name: &str, contents: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    }

    /// Configuration of a compiler server client that presents `client` (certificate chain
    /// PEM, key PEM).
    fn client_config(
        dir: &Path,
        server_ca: &TestCa,
        client: Option<&(String, String)>,
    ) -> CommonConfig {
        let mut config = CommonConfig::test_config();
        config.enable_https = true;
        config.https_tls_cert_path = Some(write(dir, "client-trust.crt", &server_ca.pem));
        config.https_tls_key_path = Some(write(dir, "unused.key", ""));
        if let Some((cert, key)) = client {
            let name = Uuid::now_v7();
            config.https_client_cert_path = Some(write(dir, &format!("{name}.crt"), cert));
            config.https_client_key_path = Some(write(dir, &format!("{name}.key"), key));
        }
        config
    }

    async fn client(
        dir: &Path,
        server_ca: &TestCa,
        client: Option<&(String, String)>,
    ) -> reqwest::Client {
        client_config(dir, server_ca, client).reqwest_client().await
    }

    /// Over TLS, the compiler server identifies each caller by its client certificate and
    /// answers only the requests of the route table.
    #[actix_web::test]
    async fn client_certificates_over_tls() {
        crate::ensure_default_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let server_ca = test_ca("server ca");
        let private_ca = test_ca("private ca");
        let foreign_ca = test_ca("foreign ca");

        // Server: certificate from the server CA, client certificates from the private CA.
        let (server_cert, server_key) = issue(
            &server_ca,
            vec![SanType::DnsName("localhost".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let mut server_common_config = CommonConfig::test_config();
        server_common_config.enable_https = true;
        server_common_config.https_tls_cert_path =
            Some(write(dir.path(), "server.crt", &server_cert));
        server_common_config.https_tls_key_path =
            Some(write(dir.path(), "server.key", &server_key));
        server_common_config.private_ca_cert_path =
            Some(write(dir.path(), "private-ca.crt", &private_ca.pem));
        let server_tls_config = server_common_config
            .https_server_config_with_client_verifier(client_certificate_verifier(
                &server_common_config,
            ))
            .unwrap();
        #[cfg(feature = "fips")]
        assert!(server_tls_config.fips());

        // A binary of pipeline A that its download returns.
        let compiler_dir = tempfile::tempdir().unwrap();
        let compiler_config = CompilerConfig {
            compiler_working_directory: compiler_dir.path().to_string_lossy().into_owned(),
            compilation_profile: CompilationProfile::Optimized,
            sql_compiler_path: String::new(),
            sql_compiler_cache_url: String::new(),
            compilation_cargo_lock_path: String::new(),
            dbsp_override_path: String::new(),
            binary_upload_endpoint: None,
            binary_upload_timeout_secs: 600,
            binary_upload_max_retries: 3,
            binary_upload_retry_delay_ms: 1000,
            gen2_udf_sdk_path: None,
            gen2_udf_sdk_version: None,
            precompile: false,
            require_client_certificates: true,
        };
        let binaries = compiler_dir
            .path()
            .join("rust-compilation/pipeline-binaries");
        std::fs::create_dir_all(&binaries).unwrap();
        std::fs::write(
            binaries.join(pipeline_binary_filename(&PIPELINE_A, Version(1), SHA, SHA)),
            b"binary",
        )
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let compiler_config = web::Data::new(compiler_config);
        let server = HttpServer::new(move || {
            App::new()
                .wrap(from_fn(authorize_compiler_caller))
                .app_data(compiler_config.clone())
                .service(check_compilation_artifacts)
                .service(get_binary)
                .service(upload_binary)
                .route("/healthz", web::get().to(HttpResponse::Ok))
        })
        .on_connect(CompilerCaller::record)
        .workers(1)
        .listen_rustls_0_23(listener, server_tls_config)
        .unwrap()
        .run();
        let server_handle = server.handle();
        actix_web::rt::spawn(server);

        let url = |path: &str| format!("https://localhost:{port}{path}");
        let binary_a = url(&format!("/binary/{PIPELINE_A}/1/{SHA}/{SHA}"));
        let binary_b = url(&format!("/binary/{PIPELINE_B}/1/{SHA}/{SHA}"));
        let artifacts_a = url(&format!("/artifacts/{PIPELINE_A}/1/{SHA}/{SHA}/none"));

        let anonymous = client(dir.path(), &server_ca, None).await;
        let control_plane_cert = issue(
            &private_ca,
            vec![uri(CONTROL_PLANE_CERTIFICATE_URI)],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let control_plane = client(dir.path(), &server_ca, Some(&control_plane_cert)).await;
        // In a FIPS build, client certificates keep both ends of the handshake FIPS compliant.
        #[cfg(feature = "fips")]
        assert!(
            client_config(dir.path(), &server_ca, Some(&control_plane_cert))
                .https_client_tls_config()
                .fips()
        );
        let pipeline_a_cert = issue(
            &private_ca,
            vec![uri(&pipeline_certificate_uri(PIPELINE_A))],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let pipeline_a = client(dir.path(), &server_ca, Some(&pipeline_a_cert)).await;
        let foreign_cert = issue(
            &foreign_ca,
            vec![uri(CONTROL_PLANE_CERTIFICATE_URI)],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let foreign = client(dir.path(), &server_ca, Some(&foreign_cert)).await;

        let get = |client: &reqwest::Client, url: &str| client.get(url).send();
        let status =
            |response: reqwest::Response| StatusCode::from_u16(response.status().as_u16()).unwrap();

        assert_eq!(
            status(get(&anonymous, &url("/healthz")).await.unwrap()),
            StatusCode::OK
        );
        assert_eq!(
            status(get(&anonymous, &binary_a).await.unwrap()),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(get(&control_plane, &binary_a).await.unwrap()),
            StatusCode::OK
        );
        assert_eq!(
            status(get(&control_plane, &artifacts_a).await.unwrap()),
            StatusCode::OK
        );
        assert_eq!(
            status(get(&pipeline_a, &binary_a).await.unwrap()),
            StatusCode::OK
        );
        assert_eq!(
            status(get(&pipeline_a, &binary_b).await.unwrap()),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(get(&pipeline_a, &artifacts_a).await.unwrap()),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(pipeline_a.post(&binary_a).body("x").send().await.unwrap()),
            StatusCode::FORBIDDEN
        );
        // A certificate from another CA fails the handshake.
        assert!(get(&foreign, &url("/healthz")).await.is_err());

        server_handle.stop(false).await;
    }
}
