package org.dbsp.sqlCompiler.circuit.operator;

/** Interface implemented by operators that integrate an input: the integrator holds the
 * collection the operator reads there.  An operator may keep other integrators besides,
 * as the rolling aggregate does for the tree it builds over its input. */
public interface IHasInputIntegrator extends IContainsIntegrator {
    /** True if the operator keeps an integrator of the collection it reads on this input.
     * @param inputIndex  Input of the operator. */
    default boolean hasInputIntegrator(int inputIndex) {
        return true;
    }
}
