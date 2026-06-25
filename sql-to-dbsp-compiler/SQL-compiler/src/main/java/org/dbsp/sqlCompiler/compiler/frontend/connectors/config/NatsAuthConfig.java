package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

/**
 * Authentication configuration for a NATS connection.
 *
 * <p>{@code credentials} maps to the Rust {@code Credentials} enum, which has
 * data-carrying variants ({@code FromString(String)} and {@code FromFile(PathBuf)}) that
 * serialize as {@code {"FromString": "..."}} or {@code {"FromFile": "..."}}.  These are
 * kept as {@link JsonNode} to avoid complex Jackson polymorphic deserialization.
 */
@SuppressWarnings("unused")
public class NatsAuthConfig {
    @Nullable
    @JsonProperty("credentials")
    public JsonNode credentials = null;

    @Nullable
    @JsonProperty("jwt")
    public String jwt = null;

    @Nullable
    @JsonProperty("nkey")
    public String nkey = null;

    @Nullable
    @JsonProperty("token")
    public String token = null;

    @Nullable
    @JsonProperty("user_and_password")
    public NatsUserAndPasswordConfig userAndPassword = null;
}
