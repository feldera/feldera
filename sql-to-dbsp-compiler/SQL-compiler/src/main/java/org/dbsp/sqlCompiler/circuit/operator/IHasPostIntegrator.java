package org.dbsp.sqlCompiler.circuit.operator;

/** Interface implemented by operators that keep an integrator on their output path.
 * Other operators may stand between the integrator and the output. */
public interface IHasPostIntegrator extends IContainsIntegrator {
    /** True if an operator that integrates the output of this operator can 
     * share the integrator. */
    default boolean integratorHoldsOutput() {
        return true;
    }
}
