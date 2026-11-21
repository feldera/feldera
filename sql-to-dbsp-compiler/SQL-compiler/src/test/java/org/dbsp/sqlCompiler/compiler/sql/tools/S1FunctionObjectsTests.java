package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexCall;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class S1FunctionObjectsTests {
    static ObjectNode fixture() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("shared-sql-functions")).findFirst().orElseThrow();
        return S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry);
    }

    @Test public void bodiesAndCallersShareFunctionObjectsAndLiteralNodes() throws IOException {
        ObjectNode wire = fixture();
        var decoded = S1ExpressionTable.decode(wire, new S1BuiltinOperatorRegistry());
        var first = (ExternalFunction) ((RexCall) decoded.roots().get(0)).getOperator();
        var second = (ExternalFunction) ((RexCall) decoded.roots().get(1)).getOperator();
        Assert.assertSame(first, ((RexCall) second.body).getOperator());
        Assert.assertSame(decoded.roots().get(2), ((RexCall) first.body).getOperands().get(1));
        Assert.assertNotSame(((RexCall) decoded.roots().get(0)).getOperands().get(0), ((RexCall) first.body).getOperands().get(0));
        Assert.assertEquals("p", first.parameterList.get(0).getName());
        Assert.assertSame(first.returnType, decoded.types().get(wire.at("/operator_objects/0/return_type").intValue()));
        Assert.assertNull(first.structReturnType);
    }

    @Test public void bodyPositionsUseCapturedGeneratedSource() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("shared-sql-functions")).findFirst().orElseThrow();
        var original = (ExternalFunction) ((RexCall) sample.roots().get(0)).getOperator();
        var body = (RexCall) original.body;
        String generated = "SELECT\n  p + 1";
        var positioned = org.apache.calcite.rex.FelderaS1ExpressionFactory.call(
                new org.apache.calcite.sql.parser.SqlParserPos(2, 3, 2, 7),
                body.getType(), body.getOperator(), body.getOperands());
        var function = new ExternalFunction(original.getName(), original.node, original.position,
                original.returnType, original.parameterList, positioned, original.generated, generated);
        var caller = (RexCall) sample.roots().get(0);
        var call = org.apache.calcite.rex.FelderaS1ExpressionFactory.call(caller.getType(), function, caller.getOperands());
        ObjectNode wire = S1ExpressionEncoder.encode(List.of(call), sample.input(), registry,
                new S1ExpressionSources.Source("query.sql", "f"));
        var decoded = S1ExpressionTable.decode(wire, registry);
        var imported = (ExternalFunction) ((RexCall) decoded.roots().get(0)).getOperator();
        Assert.assertEquals(generated, imported.bodySource);
        Assert.assertEquals(positioned.getParserPosition(), ((RexCall) imported.body).getParserPosition());
        Assert.assertEquals(1, wire.at("/operator_objects/0/body/source/source").intValue());
        ((ObjectNode) wire.at("/operator_objects/0/body/source")).put("source", 999);
        Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(wire, registry));
    }

    @Test public void cyclesBetweenFunctionObjectsAndExpressionsFail() throws IOException {
        ObjectNode wire = fixture();
        ((ObjectNode) wire.at("/operator_objects/0/body")).put("root", wire.path("roots").get(0).intValue());
        var error = Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(wire, new S1BuiltinOperatorRegistry()));
        Assert.assertTrue(error.getMessage().contains("Cyclic"));
    }

    @Test public void malformedFunctionMetadataAndParameterScopesFail() throws IOException {
        List<Consumer<ObjectNode>> mutations = List.of(
                wire -> ((ObjectNode) wire.at("/operator_objects/0/body")).put("root", 999),
                wire -> ((ObjectNode) wire.at("/operator_objects/0/body")).put("input", 999),
                wire -> ((ObjectNode) wire.at("/operator_objects/0/body")).put("scope", 0).put("input", 0),
                wire -> ((ObjectNode) wire.at("/operator_objects/0/parameters/0")).put("name", "other"),
                wire -> ((ObjectNode) wire.at("/operator_objects/0/parameters/0")).put("ty", 999),
                wire -> ((ObjectNode) wire.at("/operator_objects/0")).put("return_type", 999),
                wire -> ((ObjectNode) wire.at("/operator_objects/0")).put("generated", "false"),
                wire -> ((ObjectNode) wire.at("/operator_objects/0/diagnostic")).put("class", "java.lang.Runtime"),
                wire -> ((ObjectNode) wire.at("/operator_objects/0/body")).put("kind", "sql_text"),
                wire -> ((ObjectNode) wire.at("/scopes/1")).put("parent", 0),
                wire -> ((ObjectNode) wire.at("/expressions/5/kind")).put("binding", 0));
        for (var mutation: mutations) {
            ObjectNode wire = fixture(); mutation.accept(wire);
            Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(wire, new S1BuiltinOperatorRegistry()));
        }
    }

    @Test public void externalFunctionsRetainGeneratedFlagAndExplicitAbsenceOfBody() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("external-function")).findFirst().orElseThrow();
        var decoded = S1ExpressionTable.decode(S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry), registry);
        var function = (ExternalFunction) ((RexCall) decoded.roots().get(0)).getOperator();
        Assert.assertTrue(function.generated); Assert.assertNull(function.body);
        Assert.assertNull(function.structReturnType);
    }

    @Test public void resolvedConstructorKeepsDiagnosticDataWithoutAnAst() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).get(0);
        var type = sample.input().getFieldList().get(0).getType();
        var range = new org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange(new org.apache.calcite.sql.parser.SqlParserPos(1, 1));
        var diagnostic = new org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject(range) {
            @Override public boolean isEmpty() { return false; }
            @Override public String toString() { return "display f"; }
            @Override public String toInternalString() { return "internal f"; }
        };
        var value = new org.apache.calcite.rex.RexInputRef(0, type);
        var parameter = new org.apache.calcite.rel.type.RelDataTypeFieldImpl("p", 0, type);
        var function = new ExternalFunction("f", diagnostic, range, type, List.of(parameter), value, false);
        function.structReturnType = new org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger(diagnostic, 32, true, false);
        var call = org.apache.calcite.rex.FelderaS1ExpressionFactory.call(type, function, List.of(new org.apache.calcite.rex.RexInputRef(0, type)));
        ObjectNode wire = S1ExpressionEncoder.encode(List.of(call), sample.input(), registry, new S1ExpressionSources.Source("query.sql", "f"));
        var imported = (ExternalFunction) ((RexCall) S1ExpressionTable.decode(wire, registry).roots().get(0)).getOperator();
        Assert.assertEquals("display f", imported.node.toString()); Assert.assertEquals("internal f", imported.node.toInternalString());
        Assert.assertEquals(range, imported.position); Assert.assertEquals(range, imported.node.getPositionRange());
        Assert.assertFalse(imported.node.isEmpty());
        Assert.assertNull(imported.structReturnType);
    }
}
