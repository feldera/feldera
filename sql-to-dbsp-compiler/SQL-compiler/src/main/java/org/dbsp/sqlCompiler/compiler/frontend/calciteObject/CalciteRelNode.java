package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
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

    public static String toSqlString(RelNode node) {
        SqlNode sql = CONVERTER.visitRoot(node).asStatement();
        final SqlWriter sqlWriter = new SqlPrettyWriter();
        sql.unparse(sqlWriter, 0, 0);
        return sqlWriter.toSqlString().toString();
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
