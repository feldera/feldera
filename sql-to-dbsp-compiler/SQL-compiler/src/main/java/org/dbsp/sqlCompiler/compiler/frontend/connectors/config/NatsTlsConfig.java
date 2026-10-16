package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** TLS options for connecting to a NATS server; mirrors the Rust {@code Tls} struct. */
@SuppressWarnings("unused")
public class NatsTlsConfig {
    /** Require an encrypted connection; refuse servers that do not offer TLS. */
    @JsonProperty("require_tls")
    public boolean requireTls = false;

    /** Path to a PEM file with additional root certificates to trust. */
    @Nullable
    @JsonProperty("root_certificates_file")
    public String rootCertificatesFile = null;
}
