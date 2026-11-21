package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Properties;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class S1CatalogSchemaTests {
    @Test public void catalogSnapshotSurvivesLoweringMutationsAndRetainsValueIdentity() throws IOException {
        var builder = new RexBuilder(new S1TypeTable.Factory());
        var value = builder.makeExactLiteral(BigDecimal.TEN);
        var position = new SourcePositionRange(new SqlParserPos(1, 2, 1, 4));
        var node = new CalciteObject(position) {
            @Override public String toString() { return "display"; }
            @Override public String toInternalString() { return "internal"; }
        };
        var column = new RelColumnMetadata(node, new RelDataTypeFieldImpl("Quoted", 0, value.getType()),
                true, true, value, value, position);
        var columns = new ArrayList<>(List.of(column));
        var properties = Utilities.deterministicObjectMapper().createObjectNode();
        var property = properties.putObject("expected_size").put("value", "50");
        property.set("key_position", position.asJson()); property.set("value_position", position.asJson());
        var original = new S1CatalogSchema.Decoded(new ProgramIdentifier("Table", true), node, columns, Properties.fromJson(properties), 0);
        var captured = S1CatalogSchema.capture(original);
        column.setUnused(); columns.clear();
        var source = new S1ExpressionSources.Source("test", "abcdefgh");
        var references = S1ExpressionSharingProbe.collect(List.of(), java.util.function.UnaryOperator.identity(), List.of(captured.values()));
        var registry = new S1BuiltinOperatorRegistry();
        var graph = S1ExpressionObjectsEncoder.encode(references, registry, source, captured.types());
        var sources = S1ExpressionSources.decode(graph.inputs().table().at("/type_table/sources"));
        var wire = S1CatalogSchema.encode(captured, graph.inputs().types(), sources, 0);
        var expressions = S1ExpressionObjectsTable.decode(graph.expressions(), graph.inputs().table(), registry);
        var restored = S1CatalogSchema.decode(wire, expressions.types(), sources,
                graph.expressions().path("value_uses"), expressions.expressions());
        var imported = restored.columns().get(0);
        Assert.assertFalse(imported.unused); Assert.assertTrue(imported.nameIsQuoted); Assert.assertTrue(imported.isPrimaryKey);
        Assert.assertSame(imported.lateness, imported.defaultValue);
        Assert.assertEquals("display", imported.node.toString()); Assert.assertEquals("internal", imported.node.toInternalString());
        Assert.assertEquals(position.asJson(), imported.defaultValuePosition.asJson());
        Assert.assertEquals(properties, restored.properties().asJson());
        var ids = new java.util.IdentityHashMap<org.apache.calcite.rel.type.RelDataType, Integer>();
        for (int id = 0; id < expressions.types().size(); id++) ids.put(expressions.types().get(id), id);
        var restoredTypes = new S1TypeTable.Encoded(graph.inputs().types().table(), ids);
        Assert.assertEquals(wire, S1CatalogSchema.encode(S1CatalogSchema.capture(restored), restoredTypes, sources, 0));
        var invalid = wire.deepCopy(); invalid.withObject("/columns/0").put("default_value", 0);
        Assert.assertThrows(IOException.class, () -> S1CatalogSchema.decode(invalid, expressions.types(), sources,
                graph.expressions().path("value_uses"), expressions.expressions()));
    }
}
