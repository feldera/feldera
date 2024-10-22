package org.dbsp.sqlCompiler.circuit.annotation;

/** Annotation used on operators that are used for computing the waterline */
public class Waterline extends Annotation {
    /** Operator whose waterline is represented by this one.
     * -1 if there is no exact such operator. */
    public final long id;

    public Waterline(long id) {
        this.id = id;
    }

    public Waterline() {
        this(-1);
    }

    @Override
    public String toString() {
        return "Waterline" + (this.id > 0 ? " " + this.id : "");
    }
}
