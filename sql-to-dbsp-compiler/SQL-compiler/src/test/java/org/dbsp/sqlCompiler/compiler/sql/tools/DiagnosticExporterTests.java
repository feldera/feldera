package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

public class DiagnosticExporterTests {
    @Test public void fieldAttributionUsesJavaRegexAndFiltersWarningAssertions() {
        var messages = Utilities.deterministicObjectMapper().createArrayNode();
        messages.addObject().put("warning", false).put("message", "literal [x].")
                .put("error_type", "Compilation error").put("snippet", "SELECT x");
        messages.addObject().put("warning", true).put("message", "literal [x].")
                .put("error_type", "Unused declaration").put("snippet", "SELECT x");
        var matches = DiagnosticExporter.fieldMatches(messages, "\\Qliteral [x].\\E", true,
                List.of(1));
        Assert.assertEquals(1, matches.size());
        Assert.assertEquals(1, matches.get(0).path("diagnostic_index").intValue());
        Assert.assertEquals("[\"message\"]", matches.get(0).path("fields").toString());
        matches = DiagnosticExporter.fieldMatches(messages, "SELECT x", false,
                List.of(0, 1));
        Assert.assertEquals(2, matches.size());
        Assert.assertEquals("[\"snippet\"]", matches.get(0).path("fields").toString());
    }

    @Test public void rendererAndCrossMessageMatchesAreNotAssignedToAnIndividualBody() {
        var messages = Utilities.deterministicObjectMapper().createArrayNode();
        messages.addObject().put("warning", false).put("message", "first")
                .put("error_type", "Compilation error").put("snippet", "");
        messages.addObject().put("warning", false).put("message", "second")
                .put("error_type", "Compilation error").put("snippet", "");
        Assert.assertTrue(BaseSQLTests.matches("error: first\nerror: second", "first.*second", true));
        Assert.assertTrue(DiagnosticExporter.fieldMatches(messages, "first.*second", true,
                List.of(0, 1)).isEmpty());
        Assert.assertTrue(DiagnosticExporter.fieldMatches(messages, "error: first", false,
                List.of(0, 1)).isEmpty());
    }

    @Test public void quietWarningsCannotBecomeDependenciesOfAnErrorAssertion() {
        var options = new CompilerOptions();
        options.ioOptions.quiet = true;
        var compiler = new DBSPCompiler(options);
        compiler.reportWarning(SourcePositionRange.INVALID, "Warning", "incidental message");
        Assert.assertEquals(List.of(), DiagnosticExporter.visibleDiagnostics(compiler,
                DiagnosticExporter.AssertionKind.COMPILATION_ERROR));
        Assert.assertEquals(List.of(0), DiagnosticExporter.visibleDiagnostics(compiler,
                DiagnosticExporter.AssertionKind.WARNINGS));
        options.ioOptions.quiet = false;
        Assert.assertEquals(List.of(0), DiagnosticExporter.visibleDiagnostics(compiler,
                DiagnosticExporter.AssertionKind.COMPILATION_ERROR));
        options.ioOptions.quiet = true;
        options.ioOptions.emitJsonErrors = true;
        Assert.assertEquals(List.of(0), DiagnosticExporter.visibleDiagnostics(compiler,
                DiagnosticExporter.AssertionKind.COMPILATION_ERROR));
    }
}
