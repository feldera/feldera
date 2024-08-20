package org.dbsp.sqlCompiler.circuit.annotation;

import org.dbsp.util.ICastable;

/** Base class for annotations that can be attached to various IR nodes. */
public abstract class Annotation implements ICastable {
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
