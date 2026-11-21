package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;

public class OptimizerTraceTests extends SqlIoTest {
    @Test
    public void countsProductionsWithoutChangingTheSchema() throws IOException {
        String property = "feldera.optimizer.trace";
        String saved = System.getProperty(property);
        var directory = Files.createTempDirectory("optimizer-trace-");
        var output = directory.resolve("trace.jsonl");
        String program = """
                CREATE TABLE t(x INT NOT NULL, y INT);
                CREATE VIEW v AS SELECT x, COUNT(*) FROM t WHERE TRUE GROUP BY x;
                """;
        try {
            System.clearProperty(property);
            DBSPCompiler baseline = this.testCompiler();
            baseline.submitStatementsForCompilation(program);
            Assert.assertNotNull(baseline.getFinalCircuit(false));
            System.setProperty(property, output.toString());
            DBSPCompiler observed = this.testCompiler();
            observed.submitStatementsForCompilation(program);
            Assert.assertNotNull(observed.getFinalCircuit(false));
            Assert.assertEquals(baseline.metadata.asJson(), observed.metadata.asJson());
            long productions = 0;
            long configured = 0;
            int steps = 0;
            var mapper = Utilities.deterministicObjectMapper();
            for (String line: Files.readAllLines(output)) {
                var row = mapper.readTree(line);
                Assert.assertEquals(1, row.path("schema_version").asInt());
                Assert.assertTrue(row.path("completed").asBoolean());
                Assert.assertTrue(row.path("before").path("node_counts").isObject());
                Assert.assertTrue(row.path("after").path("correlates").isArray());
                for (var count: row.path("productions"))
                    productions += count.asLong();
                configured += row.path("configured_rules").size();
                steps++;
            }
            Assert.assertTrue(steps > 0);
            Assert.assertTrue(productions > 0);
            Assert.assertTrue(configured > 0);
        } finally {
            if (saved == null)
                System.clearProperty(property);
            else
                System.setProperty(property, saved);
            Files.deleteIfExists(output);
            Files.delete(directory);
        }
    }
}
