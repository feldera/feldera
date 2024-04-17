package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

public class CalciteRelNode extends CalciteObject {
    final RelNode relNode;

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
            RelToSqlConverter converter =
                    new RelToSqlConverter(SqlDialect.DatabaseProduct.UNKNOWN.getDialect());
            SqlNode node = converter.visitRoot(this.relNode).asStatement();
            return node.toString();
        } catch (Exception ex) {
            // Sometimes Calcite crashes when converting rel to SQL
            return this.relNode.toString();
        }
    }

    @Override
    public String toInternalString() {
        return this.relNode.toString();
    }
}
