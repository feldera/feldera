package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RepeatTests {
    @Test
    public void nativeIterationsPreserveObservationOrderAcrossNestedPasses() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustRepeat"));
        var visits = new ArrayList<String>();
        int exhausted = RustCircuitPass.repeat(2, 3, outer -> {
            visits.add("outer " + outer);
            Assert.assertEquals(0, RustCircuitPass.repeat(1, 4, inner -> {
                visits.add("inner " + inner);
                return inner < outer;
            }));
            return true;
        });
        Assert.assertEquals(0, exhausted);
        Assert.assertEquals(List.of("outer 0", "inner 0", "outer 1", "inner 0", "inner 1",
                "outer 2", "inner 0", "inner 1", "inner 2"), visits);
        Assert.assertEquals(12, RustCircuitPass.repeat(12, Integer.MAX_VALUE, index -> true));
    }

    @Test
    public void callbackExceptionReachesItsCallerUnchanged() {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustRepeat"));
        var error = new IllegalArgumentException("failed pass");
        var caught = Assert.assertThrows(IllegalArgumentException.class,
                () -> RustCircuitPass.repeat(1, 4, index -> { throw error; }));
        Assert.assertSame(error, caught);
    }
}
