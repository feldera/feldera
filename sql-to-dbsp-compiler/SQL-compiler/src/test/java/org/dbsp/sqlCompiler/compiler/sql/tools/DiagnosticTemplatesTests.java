package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;

public class DiagnosticTemplatesTests {
    @Rule public final TemporaryFolder temporary = new TemporaryFolder();

    @Test public void parsesEscapesAndConcatenationWithoutExecutingOrResolvingCode() throws Exception {
        var root = this.temporary.newFolder().toPath();
        Files.writeString(root.resolve("Sample.java"), """
                class Sample {
                    static { System.exit(99); }
                    void example() {
                        reporter.reportError(position, "title", "Column " + Utilities.singleQuote(name) + " missing\\n", true);
                        throw new CompilationError("Count " + (1 + 2), position);
                    }
                }
                """);
        var sites = DiagnosticTemplates.extract(root).path("sites");
        Assert.assertEquals(2, sites.size());
        Assert.assertEquals("Column ", sites.get(0).path("parts").get(0).path("literal").asText());
        Assert.assertEquals("'", sites.get(0).path("parts").get(1).path("literal").asText());
        Assert.assertEquals("name", sites.get(0).path("parts").get(2).path("dynamic").asText());
        Assert.assertEquals(" missing\n", sites.get(0).path("parts").get(4).path("literal").asText());
        Assert.assertEquals("1 + 2", sites.get(1).path("parts").get(1).path("dynamic").asText());
    }

    @Test public void keepsDynamicMessagesAndRejectsInvalidSyntax() throws Exception {
        var root = this.temporary.newFolder().toPath();
        var source = root.resolve("Sample.java");
        Files.writeString(source, "class Sample { void f() { reporter.reportProblem(p, true, false, title, dangerous()); } }");
        var part = DiagnosticTemplates.extract(root).path("sites").get(0).path("parts").get(0);
        Assert.assertEquals("dangerous()", part.path("dynamic").asText());
        Files.writeString(source, "class Sample { void f( {");
        Assert.assertThrows(java.io.IOException.class, () -> DiagnosticTemplates.extract(root));
    }

    @Test public void recognizesConnectorWrappersAndKeepsReceiverScopesSeparate() throws Exception {
        var root = this.temporary.newFolder().toPath();
        Files.writeString(root.resolve("Sample.java"), """
                import org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter;
                class Sample {
                    void check(ConfigReporter reporter, Other unrelated) {
                        reporter.warn("Invalid configuration", "Both keys are required");
                        reporter.warnPath("key", "Invalid configuration", "Missing " + key);
                        unrelated.warn("Unrelated", "not a diagnostic");
                        class Nested {
                            Other reporter;
                            void check() { reporter.warn("Unrelated", "shadowed field"); }
                        }
                        reporter.warn("Invalid configuration", dynamicMessage());
                    }
                    void other(Other reporter) { reporter.warn("Unrelated", "different parameter"); }
                }
                """);
        Files.writeString(root.resolve("ConnectorValidator.java"), """
                package org.dbsp.sqlCompiler.compiler.frontend.connectors;
                class ConnectorValidator {
                    void check(Other unrelated) {
                        warnAt(json, path, position, reporter, "Invalid connector", "Missing name");
                        unrelated.warnAt(json, path, position, reporter, "Unrelated", "wrong receiver");
                    }
                }
                """);
        var sites = DiagnosticTemplates.extract(root).path("sites");
        Assert.assertEquals(4, sites.size());
        Assert.assertEquals("ConnectorValidator.warnAt", sites.get(0).path("callee").asText());
        Assert.assertEquals("Invalid connector", sites.get(0).path("error_type").asText());
        Assert.assertEquals("ConfigReporter.warn", sites.get(1).path("callee").asText());
        Assert.assertEquals("ConfigReporter.warnPath", sites.get(2).path("callee").asText());
        Assert.assertEquals("dynamicMessage()", sites.get(3).path("parts").get(0).path("dynamic").asText());
        for (var site: sites)
            Assert.assertTrue(site.has("wrapper_contract"));
    }

    @Test public void doesNotTreatUnrelatedReporterTypeAsConnectorReporter() throws Exception {
        var root = this.temporary.newFolder().toPath();
        Files.writeString(root.resolve("Other.java"), """
                import other.ConfigReporter;
                class Other {
                    void check(ConfigReporter reporter) { reporter.warn("Wrong", "unrelated"); }
                    void checkQualified(org.dbsp.sqlCompiler.compiler.frontend.connectors.ConfigReporter reporter) {
                        reporter.warn("Invalid configuration", "Known receiver");
                    }
                }
                """);
        var sites = DiagnosticTemplates.extract(root).path("sites");
        Assert.assertEquals(1, sites.size());
        Assert.assertEquals("Known receiver", sites.get(0).path("parts").get(0).path("literal").asText());
    }
}
