package org.dbsp.sqlCompiler.circuit.annotation;

/** Do not use this operator in a ChainOperator.
 * Used for example by the operator following a {@link
 * org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator} - only
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator} works there. */
public class NoChain extends Annotation {}
