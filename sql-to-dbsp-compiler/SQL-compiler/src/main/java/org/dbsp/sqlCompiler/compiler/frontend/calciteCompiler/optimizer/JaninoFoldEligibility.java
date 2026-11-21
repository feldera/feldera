package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlOperator;
import org.locationtech.jts.geom.Geometry;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Arrays;

import java.util.List;

/** Translation-availability check for the test-only Rust fold adapter.
 * Janino compiles the entire reduction batch before evaluating any member.
 * A call without an implementor therefore leaves every member unchanged.
 * Inspecting the registry here executes neither Java value code nor UDFs. */
final class JaninoFoldEligibility extends RexVisitorImpl<Boolean> {
    private JaninoFoldEligibility() {
        super(false);
    }

    static boolean accepts(List<RexNode> expressions) {
        var visitor = new JaninoFoldEligibility();
        return expressions.stream().allMatch(expression -> Boolean.TRUE.equals(expression.accept(visitor)));
    }

    /** Identify built-ins represented by Calcite as UDFs by their exact backing
     * method, never by a user-visible name. Inspecting this metadata does not
     * execute a function or ask a user implementation for an implementor. */
    @Nullable static String builtin(SqlOperator operator) {
        if (!(operator instanceof SqlUserDefinedFunction udf)
                || !(udf.getFunction() instanceof ScalarFunctionImpl function)
                || function.method.getDeclaringClass() != SpatialTypeFunctions.class)
            return null;
        var method = function.method;
        if (method.getName().equals("ST_Point")
                && Arrays.equals(method.getParameterTypes(), new Class<?>[]{BigDecimal.class, BigDecimal.class}))
            return "calcite-point-2d-v1";
        if (method.getName().equals("ST_Distance")
                && Arrays.equals(method.getParameterTypes(), new Class<?>[]{Geometry.class, Geometry.class}))
            return "calcite-point-distance-v1";
        return null;
    }

    @Override public Boolean visitLiteral(RexLiteral literal) {
        return true;
    }

    @Override public Boolean visitCall(RexCall call) {
        var operator = call.getOperator();
        // RexImpTable.get asks UDFs for an implementor. The pure registry must
        // not invoke those hooks, even for an unreachable branch.
        if (operator instanceof SqlUserDefinedFunction && builtin(operator) == null)
            return false;
        // RexToLixTranslator handles these three operators before its table.
        boolean special = operator == SqlStdOperatorTable.CASE
                || operator == SqlStdOperatorTable.SEARCH
                || operator == SqlStdOperatorTable.PREV;
        if (!special && RexImpTable.INSTANCE.get(operator) == null)
            return false;
        return call.getOperands().stream()
                .allMatch(operand -> Boolean.TRUE.equals(operand.accept(this)));
    }
}
