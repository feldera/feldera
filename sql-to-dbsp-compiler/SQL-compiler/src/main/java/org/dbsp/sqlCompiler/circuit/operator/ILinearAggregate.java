package org.dbsp.sqlCompiler.circuit.operator;

/** Interface implemented by linear aggregate operators.
 * The aggregate function is linear, but the operator is not a linear operator. */
public interface ILinearAggregate extends IIncremental, IHasPostIntegrator {}
