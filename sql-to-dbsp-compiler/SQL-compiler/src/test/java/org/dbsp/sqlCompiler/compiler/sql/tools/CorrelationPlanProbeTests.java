package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.junit.Assert;
import org.junit.Test;

public class CorrelationPlanProbeTests {
    @Test public void rejectsMissingAndDuplicateCaseIdentitiesBeforeCompletion() throws Exception {
        var root = java.nio.file.Files.createTempDirectory("correlation-identities-");
        try {
            for (boolean duplicate: new boolean[]{false, true}) {
                var input = CorrelationProbeTests.input("SELECT x FROM t");
                if (!duplicate) input.put("id", "");
                var source = root.resolve("input-" + duplicate + ".jsonl");
                java.nio.file.Files.writeString(source, input + "\n" + (duplicate ? input + "\n" : ""));
                var output = root.resolve("output-" + duplicate);
                var error = Assert.assertThrows(java.io.IOException.class, () -> CorrelationPlanProbe.main(
                        new String[]{source.toString(), output.toString()}));
                Assert.assertTrue(error.getMessage(), error.getMessage().contains("case identity"));
                Assert.assertFalse(java.nio.file.Files.exists(output.resolve("complete.json")));
            }
        } finally {
            try (var files = java.nio.file.Files.walk(root)) {
                for (var file: files.sorted(java.util.Comparator.reverseOrder()).toList())
                    java.nio.file.Files.delete(file);
            }
        }
    }

    @Test public void transportsCorrelatedScalarAggregateWithItsCatalog() throws Exception {
        var captured = CorrelationPlanProbe.capture(CorrelationProbeTests.input(
                "SELECT x, (SELECT COUNT(*) FROM u WHERE y = x) AS n FROM t"),
                new S1BuiltinOperatorRegistry());
        Assert.assertTrue(captured.failures().isEmpty());
        var records = captured.plans();
        Assert.assertFalse(records.isEmpty());
        boolean correlate = false;
        for (var record: records) {
            Assert.assertFalse(record.at("/catalog/tables").isEmpty());
            for (var relation: record.at("/producers/relations"))
                correlate |= relation.path("form").asText().equals("LogicalCorrelate");
        }
        Assert.assertTrue(correlate);
    }

    @Test public void uncorrelatedControlHasNoDecorrelatorInput() throws Exception {
        var captured = CorrelationPlanProbe.capture(CorrelationProbeTests.input("SELECT x FROM t"),
                new S1BuiltinOperatorRegistry());
        Assert.assertTrue(captured.plans().isEmpty());
        Assert.assertTrue(captured.failures().isEmpty());
    }
}
