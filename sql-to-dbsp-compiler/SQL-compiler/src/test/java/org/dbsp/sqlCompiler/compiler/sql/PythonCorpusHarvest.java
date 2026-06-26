package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Compiles the SQL extracted from feldera's python test suites so the expression-oracle harvest
 * hook records their closures. Each {@code .sql} file is one suite's full program (tables plus
 * views); {@link BaseSQLTests#getCC} compiles it and {@code getCircuit} fires the harvest.
 *
 * <p>Driven by {@code PYTHON_CORPUS_SQL_DIR} (a directory of {@code .sql} files); the test is
 * skipped when that is unset, so a normal {@code mvn test} is unaffected. The harvest itself is
 * gated on {@code FELDERA_EXPR_ORACLE_DIR}. A file that fails to compile (a suite that expects an
 * error, or one using a construct the default compiler options reject) is skipped; the rest still
 * harvest.
 */
public class PythonCorpusHarvest extends BaseSQLTests {
    @Test
    public void harvestPythonCorpus() throws IOException {
        String dir = System.getenv("PYTHON_CORPUS_SQL_DIR");
        Assume.assumeTrue("PYTHON_CORPUS_SQL_DIR unset; skipping", dir != null && !dir.isEmpty());

        List<Path> sqlFiles = new ArrayList<>();
        try (Stream<Path> entries = Files.list(Path.of(dir))) {
            entries.filter(p -> p.toString().endsWith(".sql")).sorted().forEach(sqlFiles::add);
        }

        int compiled = 0;
        int skipped = 0;
        for (Path sqlFile : sqlFiles) {
            String sql = Files.readString(sqlFile);
            try {
                this.getCC(sql);
                compiled++;
            } catch (Throwable failure) {
                skipped++;
                System.err.println("PythonCorpusHarvest: skipped " + sqlFile.getFileName()
                        + ": " + failure.getMessage());
            }
        }
        System.err.println("PythonCorpusHarvest: compiled " + compiled + " / "
                + sqlFiles.size() + " sql file(s), skipped " + skipped);
    }
}
