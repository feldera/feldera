package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.util.IIndentStream;

import java.util.Map;

public abstract class CalciteRelNode extends CalciteObject {
    public static final SqlDialect DIALECT = SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
    static final RelToSqlConverter CONVERTER = new RelToSqlConverter(DIALECT);

    protected CalciteRelNode() {
        super();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    /** Convert a closed expression into SQL. */
    public static String toSqlString(RexNode node) {
        SqlImplementor.SimpleContext context = new SqlImplementor.SimpleContext(CalciteRelNode.DIALECT,
                // This function should never be called
                x -> new SqlIdentifier("", SqlParserPos.ZERO));
        SqlNode sql = context.toSql(null, node);
        return sql.toString();
    }

    @Override
    public String getMessage() { return ""; }

    public abstract IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap);

    public abstract CalciteRelNode remove(RelNode node);

    public abstract boolean contains(RelNode node);

    /** Create a CalciteRelNode that represents a step running after 'after' */
    public abstract CalciteRelNode after(CalciteRelNode first);

    /** Return a version of this node where all final nodes are marked partial */
    public abstract CalciteRelNode intermediate();
}
