package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/** Exact constructors for already resolved S1 expressions. Public builder
 * helpers may infer types or simplify calls; neither is an import operation. */
public final class FelderaS1ExpressionFactory {
    private FelderaS1ExpressionFactory() { }

    public static RexCall call(RelDataType type, SqlOperator operator, List<RexNode> arguments) {
        return call(SqlParserPos.ZERO, type, operator, arguments);
    }

    public static RexCall call(SqlParserPos position, RelDataType type, SqlOperator operator, List<RexNode> arguments) {
        return new RexCall(position, type, operator, arguments);
    }

    public static RexFieldAccess field(RexNode input, int index, RelDataType type) {
        return new RexFieldAccess(input, input.getType().getFieldList().get(index), type);
    }

    public static RexLambda lambda(List<RexLambdaRef> parameters, RexNode body) {
        return new RexLambda(parameters, body);
    }
}
