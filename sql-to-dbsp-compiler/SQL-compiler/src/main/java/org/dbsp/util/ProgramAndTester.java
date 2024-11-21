package org.dbsp.util;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.ir.DBSPFunction;

import javax.annotation.Nullable;

/** A pair of a Rust circuit representation and a tester function that can
 * exercise it. */
public record ProgramAndTester(@Nullable DBSPCircuit program, DBSPFunction tester) {}
