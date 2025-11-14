package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public class CalciteRexNode extends CalciteObject {
    /** Some RexNode objects, such as RexInputRef, only make sense in the context of an external RelNode */
    @Nullable
    final RelNode context;
    final RexNode rexNode;

    CalciteRexNode(@Nullable RelNode relNode, RexNode rexNode) {
        super(extractPosition(rexNode));
        this.context = relNode;
        this.rexNode = rexNode;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    static SourcePositionRange extractPosition(RexNode rexNode) {
        if (rexNode instanceof RexCall)
            return new SourcePositionRange(((RexCall)rexNode).getParserPosition());
        return SourcePositionRange.INVALID;
    }

    @Override
    public String toString() {
        try {
            SqlImplementor.Context context = new SqlImplementor.SimpleContext(
                    CalciteRelNode.DIALECT,
                    i -> {
                        String colName;
                        if (this.context != null) {
                            RelDataTypeField field = this.context.getRowType().getFieldList().get(i);
                            colName = field.getName();
                        } else {
                            colName = "<col>";
                        }
                        return new SqlIdentifier(colName, SqlParserPos.ZERO);
                    });
            SqlNode node = context.toSql(null, rexNode);
            if (node != null) {
                SqlString string = node.toSqlString(CalciteRelNode.DIALECT);
                return string.toString() + this.position.toShortString();
            } else {
                return this.rexNode + this.position.toShortString();
            }
        } catch (Throwable ex) {
            return this.rexNode + this.position.toShortString();
        }
    }
}
