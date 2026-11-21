package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class S4CoverageTests {
    private static JsonNode observation(String container, String pass, int index, int iteration) {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("stage", "S4");
        var frame = result.putArray("path").addObject();
        frame.put("container", container);
        frame.put("pass", pass);
        frame.put("index", index);
        frame.put("iteration", iteration);
        return result;
    }

    @Test
    public void repeatsAggregateButDistinctPlacementsRemainSeparate() throws IOException {
        var coverage = new S4Coverage();
        coverage.accept(observation("Repeat", "Graph", 0, 0));
        coverage.accept(observation("Repeat", "Graph", 0, 3));
        coverage.accept(observation("Repeat", "Graph", 1, 0));
        coverage.accept(observation("Other", "Graph", 0, 0));
        var result = coverage.finish("case/0");
        Assert.assertEquals(4, result.path("observations").asInt());
        Assert.assertEquals(3, result.path("points").size());
        var repeated = result.path("points").get(1);
        Assert.assertEquals(2, repeated.path("observations").asInt());
        Assert.assertEquals(3, repeated.path("largest_iteration").asInt());
    }

    @Test
    public void unobservedPathsAreNotInvented() throws IOException {
        var coverage = new S4Coverage();
        var finalStage = Utilities.deterministicObjectMapper().createObjectNode();
        finalStage.put("stage", "S3");
        coverage.accept(finalStage);
        Assert.assertEquals(0, coverage.finish("case/0").path("points").size());
        var malformed = Utilities.deterministicObjectMapper().createObjectNode();
        malformed.put("stage", "S4");
        Assert.assertThrows(IOException.class, () -> coverage.accept(malformed));
        Assert.assertThrows(IOException.class,
                () -> coverage.accept(observation("Repeat", "Graph", -1, 0)));
    }
}
