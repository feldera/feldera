package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

public class OptimizerOrderTests {
    @Test
    public void snapshotIncrementalAndSetOutputsPreserveEveryPassPlacement() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustOrder"));
        for (boolean sets : List.of(false, true)) {
            for (boolean incremental : List.of(false, true)) {
                Assert.assertEquals(OptimizerStep.referenceOrder(sets, incremental),
                        RustCircuitPass.optimizerOrder(sets, incremental));
            }
        }
    }
}
