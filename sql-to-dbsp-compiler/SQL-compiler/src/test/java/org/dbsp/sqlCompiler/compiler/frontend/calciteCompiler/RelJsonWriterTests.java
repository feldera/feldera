package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class RelJsonWriterTests {
    @Test
    public void spatialConstantsDoNotBreakDataflowExport() throws IOException {
        for (boolean gen2: List.of(false, true)) {
            CompilerOptions options = new CompilerOptions();
            options.ioOptions.gen2 = gen2;
            options.ioOptions.writeS3 = "capture.json";
            options.ioOptions.verbosity = 2;
            options.languageOptions.incrementalize = true;
            DBSPCompiler compiler = new DBSPCompiler(options);
            compiler.submitStatementsForCompilation("""
                    CREATE VIEW v AS SELECT CAST(ST_POINT(1, 2) AS VARIANT) AS p;
                    """);
            var circuit = compiler.getFinalCircuit(false);
            compiler.throwIfErrorsOccurred();
            Assert.assertNotNull(circuit);
            StringBuilder dataflow = new StringBuilder();
            compiler.getDataflow(dataflow, circuit);
            JsonNode plan = Utilities.deterministicObjectMapper().readTree(dataflow.toString());
            Assert.assertFalse(plan.get("calcite_plan").isEmpty());
            List<JsonNode> points = plan.findValues("geometry_type");
            Assert.assertFalse("the folded point must remain in the plan", points.isEmpty());
            Assert.assertTrue(points.stream().allMatch(p -> p.asText().equals("Point")));
        }
    }

    @Test
    public void pointEncodingRetainsCoordinatesAndSrid() {
        RelJsonWriter writer = RelJsonWriter.forReplay(new HashMap<>(), 2);
        var point = new GeometryFactory().createPoint(new Coordinate(1.25, -2.5, 3.75));
        point.setSRID(4326);
        JsonNode json = Utilities.deterministicObjectMapper().valueToTree(writer.relJson.toJson(point));
        Assert.assertEquals(4326, json.get("srid").asInt());
        Assert.assertEquals(3, json.get("dimension").asInt());
        Assert.assertEquals(0, json.get("measures").asInt());
        Assert.assertEquals(1.25, json.at("/coordinates/0/0").asDouble(), 0);
        Assert.assertEquals(-2.5, json.at("/coordinates/0/1").asDouble(), 0);
        Assert.assertEquals(3.75, json.at("/coordinates/0/2").asDouble(), 0);
    }

    @Test
    public void emptyPointAndNonFiniteOrdinatesRemainValidJson() throws IOException {
        RelJsonWriter writer = RelJsonWriter.forReplay(new HashMap<>(), 2);
        var factory = new GeometryFactory();
        var empty = factory.createPoint();
        JsonNode json = Utilities.deterministicObjectMapper().valueToTree(writer.relJson.toJson(empty));
        Assert.assertTrue(json.get("coordinates").isEmpty());
        var point = factory.createPoint(new Coordinate(Double.POSITIVE_INFINITY, 2));
        String encoded = writer.jsonBuilder.toJsonString(writer.relJson.toJson(point));
        JsonNode decoded = Utilities.deterministicObjectMapper().readTree(encoded);
        Assert.assertEquals("Infinity", decoded.at("/coordinates/0/0").asText());
        Assert.assertEquals("NaN", decoded.at("/coordinates/0/2").asText());
    }
}
