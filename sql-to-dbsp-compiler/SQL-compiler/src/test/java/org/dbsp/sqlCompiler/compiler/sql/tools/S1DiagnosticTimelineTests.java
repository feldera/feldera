package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePosition;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class S1DiagnosticTimelineTests {
    @Test public void frontendMessagesInterleaveWithoutRestoringLoweringOutput() throws IOException {
        var original = new DBSPCompiler(new CompilerOptions());
        var context = new SourcePositionRange(new SourcePosition(1, 1), new SourcePosition(1, 3));
        var capture = new S1DiagnosticTimeline.Capture();
        original.setErrorContext(context);
        original.messages.reportProblem(context, true, false, "Frontend", "first");
        capture.before(original);
        original.messages.reportProblem(context, true, false, "Lowering", "must be regenerated");
        capture.lowered(original);
        original.messages.reportProblem(context, true, true, "Frontend", "continuation");
        capture.before(original); capture.lowered(original);
        original.setErrorContext(SourcePositionRange.INVALID);
        original.messages.reportProblem(context, true, false, "Frontend", "final check");
        capture.finished(original);
        var sources = S1ExpressionSources.of(new S1ExpressionSources.Source("test", "abc"));
        var wire = capture.encode(sources);
        Assert.assertFalse(wire.toString().contains("must be regenerated"));
        var restored = S1DiagnosticTimeline.decode(wire, sources, 2);
        var imported = new DBSPCompiler(new CompilerOptions());
        restored.before().get(0).apply(imported);
        imported.messages.reportProblem(context, true, false, "Lowering", "must be regenerated");
        restored.before().get(1).apply(imported); restored.after().apply(imported);
        Assert.assertEquals(original.messages.replaySnapshot(), imported.messages.replaySnapshot());
        // Restoring the same frontend warning preserves suppression identity.
        restored.before().get(0).apply(imported);
        Assert.assertEquals(4, imported.messages.messages.size());
        var missingLowering = new DBSPCompiler(new CompilerOptions());
        restored.before().forEach(boundary -> boundary.apply(missingLowering));
        restored.after().apply(missingLowering);
        Assert.assertNotEquals(original.messages.replaySnapshot(), missingLowering.messages.replaySnapshot());
        Assert.assertThrows(IOException.class, () -> S1DiagnosticTimeline.decode(wire, sources, 1));
    }
}
