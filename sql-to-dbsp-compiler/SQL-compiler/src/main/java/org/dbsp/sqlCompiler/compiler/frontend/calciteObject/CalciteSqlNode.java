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
        return this.sqlNode.toSqlString(
                SqlDialect.DatabaseProduct.POSTGRESQL.getDialect(), true)
                .toString();
    }
}
