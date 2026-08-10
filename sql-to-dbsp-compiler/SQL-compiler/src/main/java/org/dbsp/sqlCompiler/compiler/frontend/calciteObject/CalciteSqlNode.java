package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public class CalciteSqlNode extends CalciteObject {
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
                .withDialect(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect())
                .withAlwaysUseParentheses(true)
                .withSelectListItemsOnSeparateLines(false)
                .withUpdateSetListNewline(false)
                .withIndentation(0)
                // Quote only identifiers that need it
                .withQuoteAllIdentifiers(false))
                .toString();
    }
}
