package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

public class JoinStrategy extends Annotation {
    public final Strategy strategy;

    public enum Strategy {
        /** Broadcast right join input */
        BroadcastLeft,
        /** Broadcast left join input */
        BroadcastRight,
        /** Hash merge join */
        Shard,
        /** Dynamic join balancing by hashing entire record */
        Balance,
    }

    public JoinStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject()
                .appendClass(this)
                .label("strategy")
                .append(this.strategy.name())
                .endObject();
    }

    @Override
    public String toString() {
        return this.strategy.name();
    }

    public static JoinStrategy fromJson(JsonNode node) {
        Strategy strategy = Strategy.valueOf(Utilities.getStringProperty(node, "strategy"));
        return new JoinStrategy(strategy);
    }
}
