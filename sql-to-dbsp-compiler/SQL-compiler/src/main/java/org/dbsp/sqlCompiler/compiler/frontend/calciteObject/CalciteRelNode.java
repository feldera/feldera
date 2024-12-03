package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class CalciteRelNode extends CalciteObject {
    final RelNode relNode;
    public static final SqlDialect DIALECT = SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
    static final RelToSqlConverter CONVERTER = new RelToSqlConverter(DIALECT);

    CalciteRelNode(RelNode relNode) {
        this.relNode = relNode;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public String toString() {
        try {
            SqlNode node = CONVERTER.visitRoot(this.relNode).asStatement();
            return node.toString();
        } catch (Throwable ex) {
            // Sometimes Calcite crashes when converting rel to SQL
            return this.relNode.toString();
        }
    }

    @Override
    public String toInternalString() {
        return this.relNode.toString();
    }

    /** Convert a closed expression into SQL. */
    public static String toSqlString(RexNode node) {
        SqlImplementor.SimpleContext context = new SqlImplementor.SimpleContext(CalciteRelNode.DIALECT,
                // This function should never be called
                x -> new SqlIdentifier("", SqlParserPos.ZERO));
        SqlNode sql = context.toSql(null, node);
        return sql.toString();
    }
}
