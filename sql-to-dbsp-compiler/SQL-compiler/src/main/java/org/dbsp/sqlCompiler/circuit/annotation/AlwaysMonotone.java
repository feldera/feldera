package org.dbsp.sqlCompiler.circuit.annotation;

/** This annotation indicates that a DBSPOperator has an output that is always monotone.
 * This is intended to mark the part of the circuit that derives from the NOW table. */
public class AlwaysMonotone extends Annotation { }
