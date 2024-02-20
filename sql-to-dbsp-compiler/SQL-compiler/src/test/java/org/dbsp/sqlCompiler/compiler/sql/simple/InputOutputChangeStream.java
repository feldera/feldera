package org.dbsp.sqlCompiler.compiler.sql.simple;

import java.util.ArrayList;
import java.util.List;

public class InputOutputChangeStream {
    public final List<InputOutputChange> changes;

    public InputOutputChangeStream() {
        this.changes = new ArrayList<>();
    }

    public InputOutputChangeStream addChange(InputOutputChange change) {
        this.changes.add(change);
        return this;
    }

    public InputOutputChangeStream addPair(IChange input, IChange output) {
        this.changes.add(new InputOutputChange(input, output));
        return this;
    }
}
