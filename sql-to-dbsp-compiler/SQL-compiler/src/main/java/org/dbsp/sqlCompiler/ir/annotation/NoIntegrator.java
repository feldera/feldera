package org.dbsp.sqlCompiler.ir.annotation;

/** Annotation used on a join to indicate that it doesn't need
 * an integrator on one or both sides. */
public class NoIntegrator extends Annotation {
    final boolean notNeededOnLeft;
    final boolean notNeededOnRight;

    public NoIntegrator(boolean notNeededOnLeft, boolean notNeededOnRight) {
        this.notNeededOnLeft = notNeededOnLeft;
        this.notNeededOnRight = notNeededOnRight;
    }

    @Override
    public String toString() {
        return "NoIntegrator[" + this.notNeededOnLeft + "," + this.notNeededOnRight + "]";
    }
}
