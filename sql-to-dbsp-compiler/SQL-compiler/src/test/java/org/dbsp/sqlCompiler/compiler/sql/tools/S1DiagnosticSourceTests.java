package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.junit.Assert;
import org.junit.Test;

public class S1DiagnosticSourceTests {
    @Test public void parsedTimestampFloorPrintsLikeItsValidatedSymbolWithoutMutation() throws Exception {
        for (String unit: new String[]{"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"}) {
            var parsed = (SqlCall) SqlParser.create("FLOOR(TIMESTAMP '2020-01-02 03:04:05' TO " + unit + ")").parseExpression();
            var qualifier = (SqlIntervalQualifier) parsed.operand(1);
            var validated = parsed.getOperator().createCall(parsed.getParserPosition(),
                    parsed.operand(0), SqlLiteral.createSymbol(qualifier.timeUnitRange, qualifier.getParserPosition()));
            String expected = CalciteObject.create(validated).toString();
            Assert.assertTrue(expected.contains("DATE_TRUNC"));
            Assert.assertEquals(expected, CalciteObject.create(parsed).toString());
            Assert.assertSame(qualifier, parsed.operand(1));
            var frozen = S1DiagnosticSource.snapshot(CalciteObject.create(parsed));
            Assert.assertEquals(expected, frozen.toString());
        }
    }

    @Test public void ordinaryExpressionsKeepPostgresqlDiagnosticFormatting() throws Exception {
        for (String sql: new String[]{"FLOOR(12.5)", "CEIL(12.5)", "1 + 2 * 3", "CAST(1 AS BIGINT)"}) {
            SqlNode parsed = SqlParser.create(sql).parseExpression();
            String expected = parsed.toSqlString(c -> c
                    .withDialect(org.apache.calcite.sql.SqlDialect.DatabaseProduct.POSTGRESQL.getDialect())
                    .withAlwaysUseParentheses(true).withSelectListItemsOnSeparateLines(false)
                    .withUpdateSetListNewline(false).withIndentation(0).withQuoteAllIdentifiers(false)).toString();
            Assert.assertEquals(expected, CalciteObject.create(parsed).toString());
        }
    }
}
