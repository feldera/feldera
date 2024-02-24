package org.dbsp.sqlCompiler.compiler.sql.simple;

import java.util.ArrayList;
import java.util.List;

/** A change stream is a sequence of InputOutputChange objects
 * that is used to test a streaming circuit.  Each InputOutputChange
 * corresponds to a circuit execution step.  This is not a real stream,
 * since it always finite. */
public class InputOutputChangeStream {
    public final List<InputOutputChange> changes;

    public InputOutputChangeStream() {
        this.changes = new ArrayList<>();
    }

    public InputOutputChangeStream addChange(InputOutputChange change) {
        this.changes.add(change);
        return this;
    }

    public InputOutputChangeStream addPair(Change input, Change output) {
        this.changes.add(new InputOutputChange(input, output));
        return this;
    }
}
