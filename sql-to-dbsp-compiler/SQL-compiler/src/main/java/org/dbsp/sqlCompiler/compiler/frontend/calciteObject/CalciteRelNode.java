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
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IHasId;
import org.dbsp.util.IIndentStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class CalciteRelNode extends CalciteObject implements IHasId {
    // Not clear these should be here
    public static final SqlDialect DIALECT = SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
    final List<SourcePositionRange> positions = new ArrayList<>();

    public List<SourcePositionRange> getSourcePositions() {
        return this.positions;
    }

    protected CalciteRelNode(SourcePositionRange pos) {
        super(pos);
        if (pos.isValid())
            this.positions.add(pos);
    }

    public CalciteRelNode addSourcePositions(Iterable<SourcePositionRange> positions) {
        for (var pos: positions)
            if (pos.isValid())
                this.positions.add(pos);
        return this;
    }

    public abstract CalciteRelNode copy();

    protected CalciteRelNode() { this(SourcePositionRange.INVALID); }

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

    /** Alias allocation belongs to one conversion. Reusing a converter retains
     * every previous alias, changing SQL text and making later renders slower. */
    static SqlNode convertToSql(RelNode node) {
        return new RelToSqlConverter(DIALECT).visitRoot(node).asStatement();
    }

    public static String toSqlString(RelNode node) {
        try {
            SqlNode sql = convertToSql(node);
            final SqlWriter sqlWriter = new SqlPrettyWriter();
            sql.unparse(sqlWriter, 0, 0);
            return sqlWriter.toSqlString().toString();
        } catch (Throwable ex) {
            return "<could not convert representation to SQL>";
        }
    }

    public abstract IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap);

    public abstract CalciteRelNode remove(RelReference node);

    public abstract boolean contains(RelReference node);

    /** Create a CalciteRelNode that represents a step running after 'after' */
    public abstract CalciteRelNode after(CalciteRelNode first);

    /** Return a version of this node where all final nodes are marked partial */
    public abstract CalciteRelNode intermediate();

    /** The Calcite relational operators that this node stands for, in execution order */
    public abstract List<RelReference> getReferences();
}
