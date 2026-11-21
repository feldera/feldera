package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public class CalciteSqlNode extends CalciteObject {
    private static final SqlDialect DIAGNOSTIC_DIALECT = new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT) {
        @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            // Parsed FLOOR(ts TO unit) carries an interval qualifier. Calcite's
            // PostgreSQL printer expects the symbol literal produced by validation.
            // Render the same DATE_TRUNC form without mutating the parsed tree.
            if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2
                    && call.operand(1) instanceof SqlIntervalQualifier qualifier) {
                var printable = SqlFloorFunction.replaceTimeUnitOperand(
                        call, qualifier.timeUnitRange.name(), qualifier.getParserPosition());
                SqlFloorFunction.unparseDatetimeFunction(writer, printable, "DATE_TRUNC", false);
                return;
            }
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    };

    final SqlNode sqlNode;

    CalciteSqlNode(SqlNode sqlNode) {
        super(new SourcePositionRange(sqlNode.getParserPosition()));
        this.sqlNode = sqlNode;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public String toString() {
        return this.sqlNode.toSqlString(c -> c
                .withDialect(DIAGNOSTIC_DIALECT)
                .withAlwaysUseParentheses(true)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0)
                // Quote only identifiers that need it
                .withQuoteAllIdentifiers(false))
                .toString();
    }
}
