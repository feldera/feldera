package org.dbsp.sqlCompiler.compiler.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.JsonProgramSchema;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitOptimizer;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUuid;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVariant;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeGeoPoint;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeUSize;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;
import org.dbsp.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

/** Metadata is compared independently of the structural circuit comparator. */
public class ProgramMetadataReplayTests {
    private static DBSPCompiler compile(String sql) {
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.inputCircuit = true;
        options.languageOptions.throwOnError = true;
        DBSPCompiler compiler = new DBSPCompiler(options);
        compiler.submitStatementsForCompilation(sql);
        return compiler;
    }

    private static JsonNode settings(DBSPCompiler compiler) throws Exception {
        JsonStream stream = new JsonStream(new IndentStream(new StringBuilder()));
        compiler.metadata.writeSettings(new ToJsonInnerVisitor(compiler, stream, 1));
        return Utilities.deterministicObjectMapper().readTree(stream.toString());
    }

    @Test
    public void circuitSchemaRoundTripNeedsNoTypeFactory() throws Exception {
        DBSPCompiler compiler = compile("""
                CREATE TYPE nested AS ("Field" INT);
                CREATE TABLE t (id INT NOT NULL PRIMARY KEY,
                    ts TIMESTAMP LATENESS INTERVAL 1 SECOND,
                    n nested, value INT DEFAULT 7);
                CREATE VIEW v AS SELECT * FROM t;
                """);
        DBSPCircuit original = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        ToJsonOuterVisitor writer = ToJsonOuterVisitor.create(compiler, 1);
        writer.apply(original);
        DBSPCircuit decoded = new JsonDecoder().decodeOuter(
                Utilities.deterministicObjectMapper().readTree(writer.getJsonString()), DBSPCircuit.class);
        Assert.assertEquals(original.metadata.asJson(), decoded.metadata.asJson());
        ProgramIdentifier table = new ProgramIdentifier("t", false);
        Assert.assertEquals(original.metadata.getTableDescription(table).asJson(true),
                decoded.metadata.getTableDescription(table).asJson(true));
        JsonNode before = decoded.metadata.asJson();
        decoded.metadata.getTableDescription(table).markColumnUnused(3);
        ObjectNode expected = before.deepCopy();
        for (JsonNode input: expected.path("inputs")) {
            if (input.path("name").asText().equals("t"))
                ((ObjectNode) input.path("fields").get(3)).put("unused", true);
        }
        Assert.assertEquals(expected, decoded.metadata.asJson());
        Assert.assertEquals(before, original.metadata.asJson());
    }

    @Test
    public void opaqueSchemaFieldsAndPositionsArePreserved() throws Exception {
        JsonNode input = Utilities.deterministicObjectMapper().readTree("""
                {"name":"T","case_sensitive":true,"fields":[
                 {"name":"x","case_sensitive":false,"columntype":{"type":"INTEGER","nullable":true},
                  "default":"1 + 2","lateness":"0","unused":false,
                  "position":{"start_line_number":3,"start_column":9,"end_line_number":3,"end_column":10}}],
                 "primary_key":["x"],"future_extension":{"value":[1,2,3]}}
                """);
        JsonProgramSchema schema = new JsonProgramSchema(input);
        Assert.assertEquals(input, schema.asJson(false));
        ((ObjectNode) input).put("name", "changed");
        ObjectNode output = (ObjectNode) schema.asJson(false);
        output.put("name", "also changed");
        Assert.assertEquals("T", schema.asJson(false).path("name").asText());
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> schema.markColumnUnused(1));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> schema.markColumnUnused(-1));
    }

    @Test
    public void seamEncodingPreservesColumnPositionsAndViewText() throws Exception {
        DBSPCompiler compiler = compile("""
                CREATE TABLE t (id INT NOT NULL, value INT DEFAULT 7);
                CREATE VIEW v AS SELECT id FROM t;
                """);
        DBSPCircuit original = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        ToJsonOuterVisitor writer = ToJsonOuterVisitor.createForReplay(compiler);
        writer.apply(original);
        DBSPCircuit decoded = new JsonDecoder().decodeOuter(
                Utilities.deterministicObjectMapper().readTree(writer.getJsonString()), DBSPCircuit.class);
        var before = Linq.list(original.getAllOperators());
        var after = Linq.list(decoded.getAllOperators());
        Assert.assertEquals(before.size(), after.size());
        for (int i = 0; i < before.size(); i++) {
            Assert.assertEquals(before.get(i).getSourcePositions(), after.get(i).getSourcePositions());
            Assert.assertEquals(before.get(i).getRelNode().isEmpty(), after.get(i).getRelNode().isEmpty());
            Assert.assertEquals(before.get(i).getNode().getPositionRange(), after.get(i).getNode().getPositionRange());
            if (before.get(i) instanceof DBSPSourceTableOperator source) {
                var columns = Linq.list(((DBSPSourceTableOperator) after.get(i)).metadata.getColumns());
                for (int c = 0; c < columns.size(); c++) {
                    Assert.assertEquals(source.metadata.getColumnMetadata(c).getPositionRange(),
                            columns.get(c).getPositionRange());
                    Assert.assertEquals(source.metadata.getColumnMetadata(c).defaultValuePosition,
                            columns.get(c).defaultValuePosition);
                }
            } else if (before.get(i) instanceof DBSPViewBaseOperator view) {
                Assert.assertEquals(view.query, ((DBSPViewBaseOperator) after.get(i)).query);
            }
        }
        ToJsonOuterVisitor publicWriter = ToJsonOuterVisitor.create(compiler, 1);
        publicWriter.apply(original);
        JsonNode publicJson = Utilities.deterministicObjectMapper().readTree(publicWriter.getJsonString());
        Assert.assertTrue(publicJson.findValues("sourcePositions").isEmpty());
        Assert.assertTrue(publicJson.findValues("defaultValuePosition").isEmpty());
        Assert.assertTrue(publicJson.findValues("query").isEmpty());
    }

    @Test
    public void viewPropertiesKeepKeyAndValueRangesOnlyInReplayEncoding() throws Exception {
        DBSPCompiler compiler = compile("""
                CREATE TABLE t(ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE VIEW v WITH ('emit_final' = 'ts') AS SELECT ts FROM t;
                """);
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        int checked = 0;
        for (var operator: circuit.getAllOperators()) {
            if (!(operator instanceof DBSPViewBaseOperator view) || view.metadata.properties == null)
                continue;
            Properties original = view.metadata.properties;
            if (!original.hasKey("emit_final"))
                continue;
            checked++;
            Assert.assertTrue(original.getPropertyKeyPosition("emit_final").isValid());
            JsonStream stream = new JsonStream(new IndentStream(new StringBuilder()));
            original.asJson(new ToJsonInnerVisitor(compiler, stream, 1, true));
            JsonNode saved = Utilities.deterministicObjectMapper().readTree(stream.toString());
            Assert.assertEquals(original.asJson(), Properties.fromJson(saved).asJson());
            stream = new JsonStream(new IndentStream(new StringBuilder()));
            original.asJson(new ToJsonInnerVisitor(compiler, stream, 1));
            JsonNode publicJson = Utilities.deterministicObjectMapper().readTree(stream.toString());
            Assert.assertEquals("ts", publicJson.path("emit_final").textValue());
            Assert.assertFalse(Properties.fromJson(publicJson).getPropertyKeyPosition("emit_final").isValid());
            ObjectNode malformed = saved.deepCopy();
            ((ObjectNode) malformed.path("emit_final")).remove("key_position");
            Assert.assertThrows(RuntimeException.class, () -> Properties.fromJson(malformed));
            ObjectNode nonString = saved.deepCopy();
            ((ObjectNode) nonString.path("emit_final")).put("value", 1);
            Assert.assertThrows(IllegalArgumentException.class, () -> Properties.fromJson(nonString));
        }
        Assert.assertTrue("test must exercise an emit_final property", checked > 0);
    }

    @Test
    public void settingsKeepValuesPositionsAndOptimizerWarnings() throws Exception {
        DBSPCompiler compiler = compile("""
                SET FELDERA_AVOID_STAR_JOINS = true;
                SET FELDERA_WINDOW_SHARING_THRESHOLD = 3;
                SET FELDERA_DISABLE_OPTIMIZATIONS = 'UnknownPass';
                CREATE VIEW v AS SELECT 1;
                """);
        compiler.getFinalCircuit(false);
        compiler.throwIfErrorsOccurred();
        JsonNode saved = settings(compiler);
        DBSPCompiler resumed = new DBSPCompiler(new CompilerOptions());
        resumed.metadata.readSettings(saved, new JsonDecoder());
        Assert.assertEquals(withoutAllocationIds(saved), withoutAllocationIds(settings(resumed)));
        Assert.assertEquals(compiler.metadata.noStarJoins(), resumed.metadata.noStarJoins());
        Assert.assertEquals(3, resumed.metadata.windowSharingThreshold());
        Assert.assertEquals(compiler.metadata.getPosition(ProgramMetadata.DISABLE_OPTIMIZATIONS),
                resumed.metadata.getPosition(ProgramMetadata.DISABLE_OPTIMIZATIONS));
        Assert.assertFalse(compiler.metadata.asJson().has("variables"));
        new CircuitOptimizer(compiler);
        new CircuitOptimizer(resumed);
        Assert.assertEquals(compiler.messages.messages, resumed.messages.messages);
    }

    /** These literal-only settings have no graph references. Compare their types,
     * values and positions, allowing the decoder to allocate new IR objects. */
    private static JsonNode withoutAllocationIds(JsonNode node) {
        JsonNode result = node.deepCopy();
        if (result instanceof ObjectNode object) {
            object.remove("id");
            for (var field: object.properties())
                object.set(field.getKey(), withoutAllocationIds(field.getValue()));
        } else if (result.isArray()) {
            var array = (com.fasterxml.jackson.databind.node.ArrayNode) result;
            for (int i = 0; i < array.size(); i++)
                array.set(i, withoutAllocationIds(array.get(i)));
        }
        return result;
    }

    @Test
    public void malformedSettingsAreRejected() throws Exception {
        DBSPCompiler compiler = compile("SET FELDERA_WINDOW_SHARING_THRESHOLD = 3; CREATE VIEW v AS SELECT 1;");
        compiler.getFinalCircuit(false);
        var saved = (com.fasterxml.jackson.databind.node.ArrayNode) settings(compiler);
        saved.add(saved.get(0).deepCopy());
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ProgramMetadata().readSettings(saved, new JsonDecoder()));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> new ProgramMetadata().readSettings(saved.get(0), new JsonDecoder()));
    }

    @Test
    public void decodedTypeDefinitionsStayDistinctAndReferencesStayShared() throws Exception {
        DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
        for (DBSPType type: List.of(DBSPTypeBool.create(false), DBSPTypeDate.create(false),
                DBSPTypeDouble.create(false), DBSPTypeReal.create(false), DBSPTypeTime.create(false),
                DBSPTypeTimestamp.create(false), DBSPTypeTimestampTz.create(false), DBSPTypeUuid.create(false),
                DBSPTypeVariant.create(false), DBSPTypeGeoPoint.create(
                        org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject.EMPTY, false),
                DBSPTypeUSize.create(false), DBSPTypeString.varchar(false), DBSPTypeRawTuple.EMPTY)) {
            JsonStream stream = new JsonStream(new IndentStream(new StringBuilder()));
            type.accept(new ToJsonInnerVisitor(compiler, stream, 1));
            ObjectNode first = (ObjectNode) Utilities.deterministicObjectMapper().readTree(stream.toString());
            first.put("id", 123);
            ObjectNode second = first.deepCopy();
            second.put("id", 124);
            JsonDecoder decoder = new JsonDecoder();
            DBSPType a = decoder.decodeInner(first, DBSPType.class);
            DBSPType b = decoder.decodeInner(second, DBSPType.class);
            Assert.assertTrue(type.sameType(a));
            Assert.assertTrue(a.sameType(b));
            Assert.assertNotSame(type.getClass().getSimpleName(), a, b);
            ObjectNode reference = Utilities.deterministicObjectMapper().createObjectNode().put("node", 123);
            Assert.assertSame(a, decoder.decodeInner(reference, DBSPType.class));
            JsonStream seam = new JsonStream(new IndentStream(new StringBuilder()));
            type.accept(new ToJsonInnerVisitor(compiler, seam, 1, true));
            JsonNode marked = Utilities.deterministicObjectMapper().readTree(seam.toString());
            Assert.assertTrue(marked.path("singleton").asBoolean());
            Assert.assertSame(type, new JsonDecoder().decodeInner(marked, DBSPType.class));
        }
    }
}
