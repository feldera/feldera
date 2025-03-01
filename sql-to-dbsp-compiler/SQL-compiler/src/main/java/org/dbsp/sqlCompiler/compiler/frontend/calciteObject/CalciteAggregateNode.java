package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

/** A Calcite object representing an AggregateCall */
public class CalciteAggregateNode extends CalciteObject {
    @Nullable
    final RelNode context;
    final AggregateCall aggregateCall;

    CalciteAggregateNode(@Nullable RelNode relNode, AggregateCall aggregateCall) {
        this.context = relNode;
        this.aggregateCall = aggregateCall;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    public SourcePositionRange getPositionRange() {
        return new SourcePositionRange(this.aggregateCall.getParserPosition());
    }

    @Override
    public String toString() {
        try {
            SourcePositionRange pos = this.getPositionRange();
            if (pos.isValid())
                return "";

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
            SqlNode node = context.toSql(this.aggregateCall);
            if (node != null) {
                SqlString string = node.toSqlString(CalciteRelNode.DIALECT);
                return string.toString();
            } else {
                return this.aggregateCall.toString();
            }
        } catch (Throwable ex) {
            return this.aggregateCall.toString();
        }
    }
}
