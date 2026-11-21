package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/** Pin the Calcite expression invariants used by the draft Rust verifier. */
public class S1ExpressionContractTests {
    @Test public void lambdaTypeIsItsBodyResultRatherThanAFunctionType() {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        var parameter = new RexLambdaRef(0, "item", integer);
        var lambda = (RexLambda) builder.makeLambdaCall(parameter, List.of(parameter));
        Assert.assertSame(integer, lambda.getType());
        Assert.assertSame(parameter, lambda.getExpression());
        Assert.assertSame(parameter, lambda.getParameters().get(0));

        var nullable = factory.createTypeWithNullability(integer, true);
        var body = builder.makeNullLiteral(nullable);
        var constantLambda = builder.makeLambdaCall(body, List.of(parameter));
        Assert.assertSame(body.getType(), constantLambda.getType());
        Assert.assertTrue(constantLambda.getType().isNullable());
    }

    @Test public void lambdaDeclarationAndBodyCanBeEqualButDistinctObjects() {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        var declaration = new RexLambdaRef(0, "item", integer);
        var reference = new RexLambdaRef(0, "item", integer);
        Assert.assertEquals(declaration, reference);
        Assert.assertNotSame(declaration, reference);
        var lambda = (RexLambda) builder.makeLambdaCall(reference, List.of(declaration));
        Assert.assertSame(declaration, lambda.getParameters().get(0));
        Assert.assertSame(reference, lambda.getExpression());
        Assert.assertNotSame(lambda.getExpression(), lambda.getParameters().get(0));
    }

    @Test public void callConstructionRetainsResolvedOperatorAndOrderedSharing() {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        var input = builder.makeInputRef(integer, 0);
        var call = (RexCall) builder.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(input, input));
        Assert.assertSame(SqlStdOperatorTable.PLUS, call.getOperator());
        Assert.assertSame(integer, call.getType());
        Assert.assertSame(input, call.getOperands().get(0));
        Assert.assertSame(input, call.getOperands().get(1));
    }
}
