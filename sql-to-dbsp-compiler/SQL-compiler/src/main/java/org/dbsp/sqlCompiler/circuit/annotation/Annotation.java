package org.dbsp.sqlCompiler.circuit.annotation;

import org.dbsp.util.ICastable;

/** Base class for annotations that can be attached to various IR nodes. */
public abstract class Annotation implements ICastable {
    /** True for annotations that we don't want displayed in circuit dumps */
    public boolean invisible() { return false; }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
