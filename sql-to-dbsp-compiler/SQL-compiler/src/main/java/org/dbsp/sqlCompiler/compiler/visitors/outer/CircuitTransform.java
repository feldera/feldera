package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;

import java.util.function.Function;

public interface CircuitTransform extends Function<DBSPCircuit, DBSPCircuit> {
    String getName();
}
