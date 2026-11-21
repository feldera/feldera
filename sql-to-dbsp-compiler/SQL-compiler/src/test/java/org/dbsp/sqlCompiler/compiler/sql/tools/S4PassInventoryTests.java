package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.junit.Assert;
import org.junit.Test;

public class S4PassInventoryTests {
    interface SharedState { String TOKEN = "constant"; }
    static class ParentState { private long state; }
    static class ChildState extends ParentState implements SharedState {
        private final Object state = new Object();
        static { if (Boolean.parseBoolean("true")) throw new AssertionError("Inventory initialized a class"); }
    }

    @Test public void manualChildrenIncludePrivateGcMapsAndNestedMergePasses() {
        var points = S4ManualPoints.declarations();
        Assert.assertEquals(20, points.size());
        var move = points.get(15);
        Assert.assertEquals("MoveGC", move.path("pass").asText());
        var names = new java.util.HashSet<String>();
        for (var field: move.path("fields")) names.add(field.path("name").asText());
        Assert.assertTrue(names.containsAll(java.util.List.of("leftGces", "rightGces", "original")));
        Assert.assertEquals(6, points.get(8).path("nested_transforms").size());
        for (int index: new int[]{4, 14, 17}) {
            Assert.assertEquals("in_place", points.get(index).path("kind").asText());
            Assert.assertTrue(points.get(index).path("implementation").isNull());
            Assert.assertFalse(points.get(index).path("local_and_shared_state").asText().isEmpty());
        }
    }

    @Test public void fieldsRetainHiddenInheritedAndStaticDeclarationsWithoutInitialization() {
        var fields = S4Fields.declarations(ChildState.class);
        Assert.assertEquals(3, fields.size());
        var identities = new java.util.HashSet<String>();
        for (var field: fields) identities.add(field.path("declaring_class").asText() + "." + field.path("name").asText());
        Assert.assertTrue(identities.contains(ParentState.class.getName() + ".state"));
        Assert.assertTrue(identities.contains(ChildState.class.getName() + ".state"));
        Assert.assertTrue(identities.contains(SharedState.class.getName() + ".TOKEN"));
        Assert.assertEquals(fields, S4Fields.declarations(ChildState.class));
    }

    @Test
    public void conditionalBodiesAreEnumeratedWithoutEvaluatingTheirPredicates() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var conditional = new Conditional(compiler, new DeadCode(compiler, true), () -> {
            throw new AssertionError("Inventory must not evaluate a runtime predicate");
        });
        var root = new Passes("Root", compiler, conditional, new Repeat(compiler, conditional));
        var points = S4PassInventory.paths(root);
        Assert.assertEquals(9, points.size());
        Assert.assertEquals("Conditional", points.get(1).path("path").get(1).path("container").asText());
        Assert.assertEquals(1, points.get(4).path("path").get(0).path("index").asInt());
        Assert.assertEquals("Repeat", points.get(5).path("path").get(1).path("container").asText());
        Assert.assertFalse(points.get(0).path("execution_proven").asBoolean());
    }

    @Test
    public void failingTransformEmitsNoSuccessfulS4ObservationAndRestoresThePath() {
        var options = new CompilerOptions();
        options.ioOptions.dumpProducerCommit = "test";
        options.ioOptions.dumpRuntimeAbi = "test";
        var compiler = new DBSPCompiler(options);
        var observations = new java.util.ArrayList<String>();
        compiler.circuitObservationHook = observations::add;
        var dumps = new org.dbsp.sqlCompiler.compiler.backend.CircuitDumps(compiler, 0);
        var circuit = new org.dbsp.sqlCompiler.circuit.DBSPCircuit(compiler.metadata, false);
        var failure = new org.dbsp.sqlCompiler.compiler.visitors.outer.Fail(compiler, "expected test failure");
        Assert.assertThrows(org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError.class,
                () -> dumps.apply("CheckNow", 1, 0, failure, circuit));
        Assert.assertTrue(observations.isEmpty());
        dumps.apply("AfterFailure", 0, 0, new Passes("Identity", compiler), circuit);
        Assert.assertEquals(1, observations.size());
        try {
            var path = org.dbsp.util.Utilities.deterministicObjectMapper().readTree(observations.getFirst())
                    .path("observation").path("path");
            Assert.assertEquals(1, path.size());
            Assert.assertEquals("AfterFailure", path.get(0).path("container").asText());
        } catch (java.io.IOException error) {
            throw new AssertionError(error);
        }
    }

    @Test
    public void activeCyclesFailInsteadOfSilentlyTruncatingTheInventory() {
        var compiler = new DBSPCompiler(new CompilerOptions());
        var root = new Passes("Cycle", compiler);
        root.add(root);
        Assert.assertThrows(IllegalStateException.class, () -> S4PassInventory.paths(root));
    }
}
