package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
import org.dbsp.sqlCompiler.compiler.frontend.connectors.IValidateConfig;

/** Configuration for the RabbitMQ output connector. */
@SuppressWarnings("unused")
public class RabbitmqOutputConfig implements IValidateConfig {
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

    @JsonProperty("exchange")
    public String exchange = "";

    @JsonProperty("routing_key")
    public String routingKey = "";

    @JsonProperty("delivery_mode")
    public String deliveryMode = "persistent";

    @JsonProperty("tls")
    public boolean tls = false;

    @Override
    public boolean validate(ConfigReporter reporter) {
        boolean ok = true;
        if (host.isBlank()) {
            reporter.warnPath("host", "Invalid configuration", "required field \"host\" is missing or empty");
            ok = false;
        }
        if (exchange.isBlank()) {
            reporter.warnPath("exchange", "Invalid configuration", "required field \"exchange\" is missing or empty");
            ok = false;
        }
        if (routingKey.isBlank()) {
            reporter.warnPath("routing_key", "Invalid configuration",
                    "required field \"routing_key\" is missing or empty");
            ok = false;
        }
        return ok;
    }
}
