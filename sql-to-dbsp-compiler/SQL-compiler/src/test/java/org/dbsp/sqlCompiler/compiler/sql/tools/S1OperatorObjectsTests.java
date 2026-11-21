package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.FelderaS1Identifiers;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeConstructorFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class S1OperatorObjectsTests {
    @Test public void rowConstructorsRetainNullableFieldNamesAndBuiltinIdentity() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("row-constructor-fields")).findFirst().orElseThrow();
        ObjectNode wire = S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry);
        var decoded = S1ExpressionTable.decode(wire, registry);
        var first = (org.apache.calcite.sql.fun.SqlRowOperator) ((RexCall) decoded.roots().get(0)).getOperator();
        var second = (org.apache.calcite.sql.fun.SqlRowOperator) ((RexCall) decoded.roots().get(1)).getOperator();
        Assert.assertEquals(java.util.Arrays.asList("left", null), first.getFieldNames());
        Assert.assertNull(second.getFieldNames());
        Assert.assertNotSame(first, second);
        Assert.assertNotSame(second, org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW);
        Assert.assertSame(org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW, ((RexCall) decoded.roots().get(2)).getOperator());
    }

    static ObjectNode fixture() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).stream()
                .filter(item -> item.name().equals("constructor-object-sharing")).findFirst().orElseThrow();
        return S1ExpressionEncoder.encode(sample.roots(), sample.input(), registry, sample.source());
    }

    @Test public void sharedAndEqualDistinctConstructorObjectsRemainDistinct() throws IOException {
        var table = fixture();
        var decoded = S1ExpressionTable.decode(table, new S1BuiltinOperatorRegistry());
        var first = (RexCall) decoded.roots().get(0);
        var second = (RexCall) decoded.roots().get(1);
        var third = (RexCall) decoded.roots().get(2);
        Assert.assertSame(first.getOperator(), second.getOperator());
        Assert.assertNotSame(first.getOperator(), third.getOperator());
        Assert.assertSame(first, decoded.roots().get(3));
        var constructor = (SqlTypeConstructorFunction) first.getOperator();
        Assert.assertSame(first.getType(), constructor.inferReturnType((org.apache.calcite.sql.SqlOperatorBinding) null));
        Assert.assertEquals(List.of("public", "Pair"), constructor.getSqlIdentifier().names);
        Assert.assertTrue(constructor.getSqlIdentifier().getParserPosition().isQuoted());
        Assert.assertFalse(constructor.getSqlIdentifier().isComponentQuoted(0));
        Assert.assertTrue(constructor.getSqlIdentifier().isComponentQuoted(1));
    }

    @Test public void absentComponentPositionsAreNotReplacedWithAnExplicitList() throws IOException {
        ObjectNode table = fixture();
        var name = (ObjectNode) table.at("/operator_objects/0/name");
        name.putObject("components").put("kind", "absent");
        var decoded = S1ExpressionTable.decode(table, new S1BuiltinOperatorRegistry());
        var constructor = (SqlTypeConstructorFunction) decoded.operatorObjects().get(0);
        Assert.assertNull(FelderaS1Identifiers.components(constructor.getSqlIdentifier()));
    }

    @Test public void malformedDeclarationsAndObjectReferencesFail() throws IOException {
        List<Consumer<ObjectNode>> mutations = List.of(
                table -> ((ObjectNode) table.at("/operators/0")).put("object", 99),
                table -> ((ObjectNode) table.at("/operator_objects/0")).put("declared_type", 99),
                table -> ((ArrayNode) table.at("/operator_objects/0/name/names")).removeAll(),
                table -> ((ObjectNode) table.at("/operator_objects/0/name/position")).put("quoted", "true"),
                table -> ((ArrayNode) table.at("/operator_objects/0/name/components/positions")).removeAll(),
                table -> ((ObjectNode) table.at("/operator_objects/0/name")).put("class", "java.lang.Runtime"),
                table -> ((ObjectNode) table.at("/operator_objects/0")).put("kind", "arbitrary_class"),
                table -> table.put("schema_version", "expressions-v1"));
        for (var mutation: mutations) {
            ObjectNode table = fixture(); mutation.accept(table);
            Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(table, new S1BuiltinOperatorRegistry()));
        }
    }

    @Test public void duplicateBuiltinObjectsCannotCreateFalseIdentitySlots() throws IOException {
        ObjectNode table = S1ExpressionProbe.fixtureTable();
        ((ArrayNode) table.path("operator_objects")).add(table.path("operator_objects").get(0).deepCopy());
        Assert.assertThrows(IOException.class, () -> S1ExpressionTable.decode(table, new S1BuiltinOperatorRegistry()));
    }

    @Test public void collatedConstructorIdentifiersFailExplicitly() throws IOException {
        var registry = new S1BuiltinOperatorRegistry();
        var sample = S1ExpressionTransportProbe.samples(registry).get(0);
        var name = new SqlIdentifier("Pair", org.apache.calcite.sql.SqlCollation.IMPLICIT, SqlParserPos.ZERO);
        var constructor = new SqlTypeConstructorFunction(name, sample.input());
        var input = new org.apache.calcite.rex.RexInputRef(0, sample.input().getFieldList().get(0).getType());
        var call = org.apache.calcite.rex.FelderaS1ExpressionFactory.call(sample.input(), constructor, List.of(input));
        Assert.assertThrows(IOException.class, () -> S1ExpressionEncoder.encode(List.of(call), sample.input(), registry));
    }
}
