package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

public class JoinStrategy extends Annotation {
    public final SourcePositionRange position;
    public final Strategy strategy;
    /** Which input to apply the strategy to */
    public final int input;
    /** Original input name; used for error reporting only */
    public final String inputName;

    public enum Strategy {
        /** Broadcast */
        Broadcast,
        /** Shard */
        Shard,
        /** Dynamic join balancing by hashing entire record */
        Balance,
    }

    public JoinStrategy(SourcePositionRange range, Strategy strategy, int input, String inputName) {
        this.position = range;
        this.strategy = strategy;
        this.input = input;
        this.inputName = inputName;
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject()
                .appendClass(this)
                .label("strategy")
                .append(this.strategy.name())
                .label("input")
                .append(this.input)
                .label("inputName")
                .append(this.inputName)
                .endObject();
    }

    public SourcePositionRange getPosition() {
        return this.position;
    }

    public boolean compatible(JoinStrategy other) {
        return this.strategy == other.strategy && this.input == other.input;
    }

    @Override
    public String toString() {
        return this.strategy.name() + "(" + this.inputName + ")";
    }

    public String toRust() {
        // Does not contain input; the strategy is actually applied to the corresponding input stream
        String policy = this.strategy.name();
        return "BalancerHint::Policy(Some(PartitioningPolicy::" + policy + "))";
    }

    public static JoinStrategy fromJson(JsonNode node) {
        Strategy strategy = Strategy.valueOf(Utilities.getStringProperty(node, "strategy"));
        int input = Utilities.getIntProperty(node, "input");
        String inputName = Utilities.getStringProperty(node, "inputName");
        return new JoinStrategy(SourcePositionRange.INVALID, strategy, input, inputName);
    }
}
