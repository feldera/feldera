package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S4StateProbeTests {
    static final class SyntheticPass implements CircuitTransform {
        final Map<String, Integer> state = new HashMap<>();
        boolean invoked;
        @Override public String getName() { return "SyntheticPass"; }
        @Override public DBSPCircuit apply(DBSPCircuit circuit) { this.invoked = true; this.state.put("item", 1); return circuit; }
    }

    @Test public void readsFieldsWithoutInvokingTransformOrChangingMaps() {
        var events = new ArrayList<ObjectNode>();
        var observer = new S4StateProbe(events::add);
        var pass = new SyntheticPass();
        observer.before("test", 0, 0, pass, null);
        observer.after("test", 0, 0, pass, null, true);
        Assert.assertFalse(pass.invoked);
        Assert.assertTrue(pass.state.isEmpty());
        Assert.assertTrue(observer.balanced());
        Assert.assertEquals(events.get(0).path("pass_object"), events.get(1).path("pass_object"));
    }

    @Test public void tracksIdentityAcrossRepeatedInvocationsAndMutation() {
        var events = new ArrayList<ObjectNode>();
        var observer = new S4StateProbe(events::add);
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.stateObserver = observer;
        var pass = new SyntheticPass();
        compiler.applyObservedPass("Repeat", 0, 0, pass, null);
        compiler.applyObservedPass("Repeat", 0, 1, pass, null);
        Assert.assertEquals(4, events.size());
        Assert.assertEquals(events.get(0).path("pass_object"), events.get(2).path("pass_object"));
        Assert.assertEquals(1, events.get(2).path("iteration").asInt());
        Assert.assertTrue(observer.balanced());
    }

    @Test public void throwingPassStillClosesObservationStack() {
        var events = new ArrayList<ObjectNode>();
        var observer = new S4StateProbe(events::add);
        var compiler = new DBSPCompiler(new CompilerOptions());
        compiler.stateObserver = observer;
        CircuitTransform throwing = new CircuitTransform() {
            @Override public String getName() { return "Failure"; }
            @Override public DBSPCircuit apply(DBSPCircuit circuit) { throw new IllegalStateException("expected"); }
        };
        Assert.assertThrows(IllegalStateException.class,
                () -> compiler.applyObservedPass("test", 0, 0, throwing, null));
        Assert.assertFalse(events.get(1).path("completed").asBoolean());
        Assert.assertTrue(observer.balanced());
    }

    @Test public void collectionSamplesHaveExplicitBounds() {
        var events = new ArrayList<ObjectNode>();
        var observer = new S4StateProbe(events::add);
        var pass = new SyntheticPass();
        for (int i = 0; i < 40; i++) pass.state.put("item" + i, i);
        observer.before("test", 0, 0, pass, null);
        var fields = events.get(0).path("fields");
        var state = java.util.stream.StreamSupport.stream(fields.spliterator(), false)
                .filter(field -> field.path("name").asText().equals("state")).findFirst().orElseThrow();
        Assert.assertEquals(40, state.path("size").asInt());
        Assert.assertEquals(32, state.path("sample_entries").size());
        Assert.assertFalse(state.path("sample_complete").asBoolean());
        Assert.assertEquals(40, pass.state.size());
    }
}
