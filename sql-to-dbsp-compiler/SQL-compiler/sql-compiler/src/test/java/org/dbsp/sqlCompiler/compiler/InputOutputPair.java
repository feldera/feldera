package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.Linq;

/**
 * A pair of inputs and outputs used in a test.
 */
public class InputOutputPair {
    /**
     * An input value for every input table.
     */
    public final DBSPZSetLiteral.Contents[] inputs;
    /**
     * An expected output value for every output view.
     */
    public final DBSPZSetLiteral.Contents[] outputs;

    public InputOutputPair(DBSPZSetLiteral.Contents[] inputs, DBSPZSetLiteral.Contents[] outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public InputOutputPair(DBSPZSetLiteral.Contents input, DBSPZSetLiteral.Contents output) {
        this.inputs = new DBSPZSetLiteral.Contents[1];
        this.inputs[0] = input;
        this.outputs = new DBSPZSetLiteral.Contents[1];
        this.outputs[0] = output;
    }

    static DBSPZSetLiteral[] toZSets(DBSPZSetLiteral.Contents[] data) {
        return Linq.map(data, s -> new DBSPZSetLiteral(new DBSPTypeWeight(), s), DBSPZSetLiteral.class);
    }

    public DBSPZSetLiteral[] getInputs() {
        return toZSets(this.inputs);
    }

    public DBSPZSetLiteral[] getOutputs() {
        return toZSets(this.outputs);
    }
}
