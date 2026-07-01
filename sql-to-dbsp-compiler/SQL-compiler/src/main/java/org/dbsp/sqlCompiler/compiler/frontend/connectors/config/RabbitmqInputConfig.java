package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

/** Configuration for the RabbitMQ AMQP 1.0 input connector. */
@SuppressWarnings("unused")
public class RabbitmqInputConfig implements IValidateConfig {
    @JsonProperty("host")
    public String host = "";

    @JsonProperty("port")
    public int port = 5672;

    @JsonProperty("vhost")
    public String vhost = "/";

    @JsonProperty("username")
    public String username = "";

    @JsonProperty("password")
    public String password = "";

    @JsonProperty("queue")
    public String queue = "";

    @JsonProperty("offset")
    public JsonNode offset = null;

    @JsonProperty("tls")
    public boolean tls = false;

    @JsonProperty("consumer_name")
    public String consumerName = null;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (host.isBlank()) {
            reporter.warnPath("host", "Invalid configuration", "required field \"host\" is missing or empty");
            ok = false;
        }
        if (queue.isBlank()) {
            reporter.warnPath("queue", "Invalid configuration", "required field \"queue\" is missing or empty");
            ok = false;
        }
        if (username.isBlank()) {
            reporter.warnPath("username", "Invalid configuration",
                    "required field \"username\" is missing or empty");
            ok = false;
        }
        return ok;
    }
}
