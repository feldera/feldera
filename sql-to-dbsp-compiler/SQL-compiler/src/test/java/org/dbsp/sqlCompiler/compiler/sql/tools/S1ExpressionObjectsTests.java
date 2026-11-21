package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.FelderaS1ExpressionFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

public class S1ExpressionObjectsTests {
    private static S1ExpressionObjectsEncoder.Encoded fixture(boolean lambdas) throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
        var first = LogicalValues.createOneRow(cluster); var second = LogicalValues.createOneRow(cluster);
        var type = first.getRowType().getFieldList().get(0).getType();
        var shared = new RexInputRef(0, type);
        var call = FelderaS1ExpressionFactory.call(type, SqlStdOperatorTable.PLUS, List.of(shared, shared));
        var equal = FelderaS1ExpressionFactory.call(type, SqlStdOperatorTable.PLUS, List.of(shared, shared));
        var reference = new RexLambdaRef(0, "item", type);
        var left = lambdas ? FelderaS1ExpressionFactory.lambda(List.of(new RexLambdaRef(0, "item", type)), reference) : call;
        var right = lambdas ? FelderaS1ExpressionFactory.lambda(List.of(new RexLambdaRef(0, "item", type)), reference) : equal;
        var one = LogicalProject.create(first, List.of(), List.of(left, right), List.of("x", "y"));
        var two = LogicalProject.create(second, List.of(), List.of(left), List.of("x"));
        return S1ExpressionObjectsEncoder.encode(S1ExpressionSharingProbe.collect(List.of(one, two)), new S1BuiltinOperatorRegistry(), null);
    }

    @Test public void sharedCallAndEqualDistinctCallsKeepIdentityAcrossRelations() throws IOException {
        var encoded = fixture(false);
        var decoded = S1ExpressionObjectsTable.decode(encoded.expressions(), encoded.inputs().table(), new S1BuiltinOperatorRegistry());
        var roots = encoded.expressions().at("/relation_uses/0/roots");
        int first = roots.get(0).intValue(), second = roots.get(1).intValue();
        Assert.assertEquals(first, encoded.expressions().at("/relation_uses/2/roots/0").intValue());
        Assert.assertNotEquals(first, second);
        Assert.assertEquals(decoded.expressions().get(first), decoded.expressions().get(second));
        Assert.assertNotSame(decoded.expressions().get(first), decoded.expressions().get(second));
        var call = (RexCall) decoded.expressions().get(first);
        Assert.assertSame(call.getOperands().get(0), call.getOperands().get(1));
    }

    @Test public void lambdaReferenceCanBeSharedAcrossDistinctDeclarations() throws IOException {
        var encoded = fixture(true);
        var decoded = S1ExpressionObjectsTable.decode(encoded.expressions(), encoded.inputs().table(), new S1BuiltinOperatorRegistry());
        var roots = encoded.expressions().at("/relation_uses/0/roots");
        var first = (RexLambda) decoded.expressions().get(roots.get(0).intValue());
        var second = (RexLambda) decoded.expressions().get(roots.get(1).intValue());
        Assert.assertSame(first.getExpression(), second.getExpression());
        Assert.assertNotSame(first.getParameters().get(0), second.getParameters().get(0));
        Assert.assertNotSame(first.getParameters().get(0), first.getExpression());
    }

    @Test public void searchExpansionPreservesSharedParentsAndUnchangedOperands() {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var type = builder.getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
        var input = new RexInputRef(0, type);
        var ranges = com.google.common.collect.ImmutableRangeSet.of(com.google.common.collect.Range.closed(BigDecimal.ONE, BigDecimal.TEN));
        var sarg = org.apache.calcite.util.Sarg.of(org.apache.calcite.rex.RexUnknownAs.UNKNOWN, ranges);
        var search = builder.makeCall(SqlStdOperatorTable.SEARCH, input, builder.makeSearchArgumentLiteral(sarg, type));
        var parent = (RexCall) builder.makeCall(SqlStdOperatorTable.AND, search, search);
        var normalizer = new S1ExpressionNormalizer(builder);
        var expanded = (RexCall) normalizer.apply(parent);
        Assert.assertNotSame(parent, expanded);
        Assert.assertSame(expanded, normalizer.apply(parent));
        Assert.assertTrue(org.apache.calcite.rex.RexUtil.isFlat(expanded));
        Assert.assertEquals(4, expanded.getOperands().size());
        Assert.assertSame(expanded.getOperands().get(0), expanded.getOperands().get(2));
        Assert.assertSame(expanded.getOperands().get(1), expanded.getOperands().get(3));
        Assert.assertSame(input, normalizer.apply(input));
        var equal = FelderaS1ExpressionFactory.call(parent.getType(), parent.getOperator(), parent.getOperands());
        Assert.assertNotSame(expanded, normalizer.apply(equal));
    }

    @Test public void generatedExpressionPositionsRetainDiagnosticVisibility() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var literal = builder.makeExactLiteral(BigDecimal.ONE);
        var coordinate = new org.apache.calcite.sql.parser.SqlParserPos(1, 1, 1, 5);
        for (Boolean internal: new Boolean[]{null, false, true}) {
            var position = internal == null ? coordinate
                    : new org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos(coordinate, internal);
            var call = FelderaS1ExpressionFactory.call(position, literal.getType(), SqlStdOperatorTable.PLUS, List.of(literal, literal));
            var captured = S1ExpressionSharingProbe.collect(List.of(), java.util.function.UnaryOperator.identity(), List.of(List.of(call)));
            var registry = new S1BuiltinOperatorRegistry();
            var encoded = S1ExpressionObjectsEncoder.encode(captured, registry, new S1ExpressionSources.Source("expression", "1 + 1"));
            var decoded = S1ExpressionObjectsTable.decode(encoded.expressions(), encoded.inputs().table(), registry);
            int id = encoded.expressions().at("/value_uses/0/roots/0").intValue();
            var restored = ((RexCall) decoded.expressions().get(id)).getParserPosition();
            Assert.assertEquals(position.getClass(), restored.getClass());
            Assert.assertEquals(new org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange(position),
                    new org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange(restored));
            if (internal != null)
                Assert.assertEquals(internal.booleanValue(), ((org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos) restored).internal);
        }
    }

    @Test public void malformedConstructionFailsBeforeCreatingObjects() throws IOException {
        for (String mutation: List.of("cycle", "signature", "position", "unknown")) {
            var encoded = fixture(false); ObjectNode wire = encoded.expressions().deepCopy();
            int root = wire.at("/relation_uses/0/roots/0").intValue();
            var expression = (ObjectNode) wire.path("expressions").get(root);
            switch (mutation) {
                case "cycle" -> expression.withObject("/kind").putArray("arguments").add(root).add(root);
                case "signature" -> ((ObjectNode) wire.at("/operators/0")).putArray("parameters");
                case "position" -> expression.withObject("/position").put("kind", "unknown");
                case "unknown" -> expression.put("unmodeled", true);
            }
            Assert.assertThrows(IOException.class, () -> S1ExpressionObjectsTable.decode(wire, encoded.inputs().table(), new S1BuiltinOperatorRegistry()));
        }
    }
    @Test public void statementValuesRetainSharedIdentityWithoutRelations() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var literal = builder.makeExactLiteral(BigDecimal.TEN);
        var call = builder.makeCall(SqlStdOperatorTable.PLUS, literal, literal);
        var captured = S1ExpressionSharingProbe.collect(List.of(), java.util.function.UnaryOperator.identity(),
                List.of(List.of(call, literal), List.of(call)));
        var registry = new S1BuiltinOperatorRegistry();
        var encoded = S1ExpressionObjectsEncoder.encode(captured, registry, null);
        var producers = S1InputProducers.encode(captured, encoded.inputs());
        Assert.assertTrue(producers.path("relations").isEmpty());
        var decoded = S1ExpressionObjectsTable.decode(encoded.expressions(), encoded.inputs().table(), registry);
        var uses = encoded.expressions().path("value_uses");
        Assert.assertEquals(uses.get(0).path("roots").get(0), uses.get(1).path("roots").get(0));
        var restored = (RexCall) decoded.expressions().get(uses.get(0).path("roots").get(0).intValue());
        Assert.assertSame(restored.getOperands().get(0), restored.getOperands().get(1));
        Assert.assertSame(restored.getOperands().get(0), decoded.expressions().get(uses.get(0).path("roots").get(1).intValue()));
        var invalid = encoded.inputs().table().deepCopy();
        ((ObjectNode) invalid.at("/environments/0/owner")).put("kind", "relations").putArray("inputs");
        Assert.assertThrows(IOException.class, () -> S1ExpressionObjectsTable.decode(encoded.expressions(), invalid, registry));
    }

    @Test public void uncalledFunctionDeclarationsKeepBodiesAndDistinctIdentities() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var type = builder.getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
        var parameter = new org.apache.calcite.rel.type.RelDataTypeFieldImpl("arg", 0, type);
        var body = new RexInputRef(0, type);
        var first = new org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction("f",
                org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject.EMPTY,
                org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange.INVALID, type, List.of(parameter), body, false, null);
        var second = new org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction("f",
                org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject.EMPTY,
                org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange.INVALID, type, List.of(parameter), null, false, null);
        var references = S1ExpressionSharingProbe.collect(List.of(), java.util.function.UnaryOperator.identity(),
                List.of(), List.of(first, second, first));
        var registry = new S1BuiltinOperatorRegistry();
        var graph = S1ExpressionObjectsEncoder.encode(references, registry, null);
        Assert.assertEquals(List.of(0, 1, 0), graph.declarations());
        var decoded = S1ExpressionObjectsTable.decode(graph.expressions(), graph.inputs().table(), registry);
        var restored = (org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction) decoded.operators().get(0);
        Assert.assertNotNull(restored.body);
        Assert.assertNotSame(decoded.operators().get(0), decoded.operators().get(1));
        Assert.assertEquals(1, graph.inputs().table().path("functions").intValue());
    }

}
