package org.dbsp.util;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.ir.DBSPFunction;

import javax.annotation.Nullable;

/** A pair of a Rust circuit representation and a tester function that can
 * exercise it. */
public class ProgramAndTester {
    @Nullable
    public final DBSPCircuit program;
    public final DBSPFunction tester;

    public ProgramAndTester(@Nullable DBSPCircuit program, DBSPFunction tester) {
        this.program = program;
        this.tester = tester;
    }
}
