package org.dbsp.sqlCompiler.compiler.jit;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.TimeTests;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.junit.Ignore;
import org.junit.Test;

public class JitTimeTests extends TimeTests {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(false, true, true);
        return new DBSPCompiler(options);
    }

    @Test @Ignore("No support for intervals https://github.com/feldera/feldera/issues/309")
    public void timestampDiffTest() {
        String query =
                "SELECT timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 11:59:59'), " +
                        "timestampdiff(MONTH, TIMESTAMP'2021-02-28 12:00:00', TIMESTAMP'2021-03-28 12:00:00'), " +
                        "timestampdiff(YEAR, DATE'2021-01-01', DATE'1900-03-28')";
        this.testQuery(query,
                new DBSPI32Literal(0), new DBSPI32Literal(1), new DBSPI32Literal(-120)
        );
    }

    @Test @Ignore("No support for intervals https://github.com/feldera/feldera/issues/309")
    public void timestampAddTableTest() {
        String query =
                "SELECT TIMESTAMPADD(SECOND, 10, COL1), " +
                        " TIMESTAMPADD(HOUR, 1, COL1), " +
                        " TIMESTAMPADD(MINUTE, 10, COL1) FROM T";
        this.testQuery(query,
                new DBSPTimestampLiteral(10100),
                new DBSPTimestampLiteral(3600100),
                new DBSPTimestampLiteral(600100)
        );
    }

    @Test @Ignore("IString parsing not supported https://github.com/feldera/feldera/issues/338")
    public void castTimestampToStringToTimestamp() {
        String query = "SELECT CAST(CAST(T.COL1 AS STRING) AS Timestamp) FROM T";
        this.testQuery(query, new DBSPTimestampLiteral(0));
    }
}
