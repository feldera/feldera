package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S1TypeTableTests {
    static List<RelDataType> roots() {
        var factory = new S1TypeTable.Factory();
        RelDataType integer = new BasicSqlType(SqlToRelCompiler.TYPE_SYSTEM, SqlTypeName.INTEGER);
        RelDataType equalInteger = new BasicSqlType(SqlToRelCompiler.TYPE_SYSTEM, SqlTypeName.INTEGER);
        RelDataType nullable = factory.createTypeWithNullability(integer, true);
        var array = new ArraySqlType(nullable, true);
        var multiset = new MultisetSqlType(array, false);
        var map = new MapSqlType(integer, multiset, true);
        List<RelDataType> roots = new ArrayList<>();
        for (StructKind kind: List.of(StructKind.FULLY_QUALIFIED, StructKind.PEEK_FIELDS,
                StructKind.PEEK_FIELDS_DEFAULT, StructKind.PEEK_FIELDS_NO_EXPAND)) {
            var row = new RelRecordType(kind, List.of(
                    new RelDataTypeFieldImpl("A", 0, integer),
                    new RelDataTypeFieldImpl("A", 1, integer),
                    new RelDataTypeFieldImpl("a", 2, equalInteger),
                    new RelDataTypeFieldImpl("🦀", 3, map)), true);
            roots.add(row);
            roots.add(row); // Root order and repeated references are observable.
        }
        roots.add(integer);
        roots.add(equalInteger);
        roots.add(array);
        roots.add(map);
        return roots;
    }

    private static JsonNode wire(ObjectNode node) throws IOException {
        return Utilities.deterministicObjectMapper().readTree(node.toString());
    }

    @Test
    public void orderedArenaPreservesSharingDistinctEqualNodesAndNestedNullability() throws IOException {
        var original = roots();
        var table = S1TypeTable.encode(original);
        var decoded = S1TypeTable.decode(wire(table));
        for (int index = 0; index < original.size(); index++) {
            Assert.assertEquals(original.get(index), decoded.roots().get(index));
            Assert.assertEquals(original.get(index).getFullTypeString(), decoded.roots().get(index).getFullTypeString());
        }
        Assert.assertSame(decoded.roots().get(0), decoded.roots().get(1));
        var fields = decoded.roots().get(0).getFieldList();
        Assert.assertSame(fields.get(0).getType(), fields.get(1).getType());
        Assert.assertEquals(fields.get(0).getType(), fields.get(2).getType());
        Assert.assertNotSame(fields.get(0).getType(), fields.get(2).getType());
        Assert.assertSame(fields.get(0).getType(), decoded.roots().get(8));
        Assert.assertSame(fields.get(2).getType(), decoded.roots().get(9));
        Assert.assertFalse(fields.get(0).getType().isNullable());
        Assert.assertTrue(decoded.roots().get(10).getComponentType().isNullable());
        Assert.assertEquals(wire(table), wire(S1TypeTable.encodeArena(decoded.types(), decoded.roots())));
    }

    @Test
    public void importAcceptsArenaOrderAndChecksUnreachableEntries() throws IOException {
        var array = S1TypeTable.encode(List.of(new ArraySqlType(roots().get(8), false)));
        JsonNode old = array.get("types").deepCopy();
        array.putArray("types").add(old.get(1)).add(old.get(0));
        ((ObjectNode) array.at("/types/1/kind")).put("element", 0);
        array.putArray("roots").add(1).add(0).add(1);
        var decoded = S1TypeTable.decode(wire(array));
        Assert.assertSame(decoded.types().get(0), decoded.types().get(1).getComponentType());
        Assert.assertSame(decoded.roots().get(0), decoded.roots().get(2));
        array.putArray("roots"); // All entries are now unreachable.
        ((ObjectNode) array.at("/types/1/kind")).put("element", 1);
        Assert.assertThrows(IOException.class, () -> S1TypeTable.decode(wire(array)));
    }

    @Test
    public void malformedFieldsAndReferencesNeverPassThroughAsDefaults() throws IOException {
        var table = S1TypeTable.encode(roots());
        for (String pointer: List.of("", "/types/0", "/types/0/kind", "/types/0/kind/fields/0", "/types/0/kind/fields/0/name")) {
            ObjectNode changed = table.deepCopy();
            ((ObjectNode) changed.at(pointer)).put("extra", true);
            Assert.assertThrows(pointer, IOException.class, () -> S1TypeTable.decode(wire(changed)));
        }
        for (JsonNode reference: List.of(Utilities.deterministicObjectMapper().getNodeFactory().numberNode(-1),
                Utilities.deterministicObjectMapper().getNodeFactory().numberNode(1.0),
                Utilities.deterministicObjectMapper().getNodeFactory().textNode("1"),
                Utilities.deterministicObjectMapper().getNodeFactory().numberNode(Integer.MAX_VALUE))) {
            ObjectNode changed = table.deepCopy();
            ((ObjectNode) changed.at("/types/0/kind/fields/0")).set("ty", reference);
            Assert.assertThrows(IOException.class, () -> S1TypeTable.decode(wire(changed)));
        }
        ObjectNode quoted = table.deepCopy();
        ((ObjectNode) quoted.at("/types/0/kind/fields/0/name")).put("quoted", false);
        Assert.assertThrows(IOException.class, () -> S1TypeTable.decode(wire(quoted)));
    }

    @Test
    public void sharedSubtreesDoNotHideExcessiveDepth() throws IOException {
        List<RelDataType> chain = new ArrayList<>();
        chain.add(roots().get(8));
        for (int i = 1; i <= 256; i++)
            chain.add(new ArraySqlType(chain.getLast(), false));
        var valid = S1TypeTable.encodeArena(chain.subList(0, 256), List.of(chain.get(0), chain.get(255)));
        S1TypeTable.decode(wire(valid));
        var invalid = S1TypeTable.encodeArena(chain, List.of(chain.get(0), chain.get(256)));
        Assert.assertThrows(IOException.class, () -> S1TypeTable.decode(wire(invalid)));
        Assert.assertThrows(IOException.class, () -> S1TypeTable.encode(List.of(chain.get(0), chain.get(256))));
    }
    @Test public void encodedLookupUsesIdentityEvenForEqualTypes() throws IOException {
        var first = new ArraySqlType(roots().get(8), false);
        var second = new ArraySqlType(first.getComponentType(), false);
        Assert.assertEquals(first, second);
        Assert.assertNotSame(first, second);
        var encoded = S1TypeTable.encodeWithIds(List.of(first, second));
        Assert.assertNotEquals(encoded.id(first), encoded.id(second));
        Assert.assertThrows(IOException.class, () -> encoded.id(new ArraySqlType(first.getComponentType(), false)));
        Assert.assertThrows(UnsupportedOperationException.class, () -> encoded.ids().put(first, 999));
    }

}
