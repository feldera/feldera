package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Tests that source positions reach the generated Rust code only through the source map. */
public class RustSourcePositionTests extends BaseSQLTests {
    /** The Rust code generated for a script, without the lines of the source map. */
    static String rustWithoutSourceMap(String sql) throws IOException, SQLException {
        File file = createInputScript(sql);
        CompilerMessages messages = CompilerMain.execute("-i", "-o", BaseSQLTests.TEST_FILE_PATH, file.getPath());
        Assert.assertEquals(messages.toString(), 0, messages.exitCode);
        String rust = Utilities.readFile(BaseSQLTests.TEST_FILE_PATH);
        return Arrays.stream(rust.split("\n"))
                .filter(line -> !line.contains("SourcePosition::new("))
                .collect(Collectors.joining("\n"));
    }

    @Test
    public void propertyPositions() throws IOException, SQLException {
        // Tables with and without a primary key, and views, all with properties
        String sql = """
                CREATE TABLE T (COL1 INT NOT NULL PRIMARY KEY, COL2 INT) WITH ('materialized' = 'true');
                CREATE TABLE S (COL1 INT LATENESS 1, COL2 INT) WITH ('materialized' = 'true');
                CREATE VIEW U AS SELECT * FROM T;
                CREATE VIEW V WITH ('emit_final' = 'col1') AS SELECT COL1, COUNT(*) AS C FROM S GROUP BY COL1;
                CREATE MATERIALIZED VIEW W WITH ('emit_final' = 'col1') AS SELECT COL1, COUNT(*) AS C FROM S GROUP BY COL1;""";
        String rust = rustWithoutSourceMap(sql);
        // The leading empty line moves every source position down by one line
        String shifted = rustWithoutSourceMap("\n" + sql);
        Assert.assertEquals(rust, shifted);
    }
}
