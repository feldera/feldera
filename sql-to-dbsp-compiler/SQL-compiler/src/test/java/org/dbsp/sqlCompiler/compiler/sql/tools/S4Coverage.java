package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.TreeMap;

/** Per-compilation pass paths. Counts show execution, not complete state coverage. */
final class S4Coverage {
    private static final class Point {
        final JsonNode path;
        long observations;
        long largestIteration;

        Point(JsonNode path) {
            this.path = path;
        }
    }

    private final TreeMap<String, Point> points = new TreeMap<>();
    private long observations;

    void accept(JsonNode observation) throws IOException {
        if (!observation.path("stage").asText().equals("S4"))
            return;
        JsonNode path = observation.get("path");
        if (path == null || !path.isArray() || path.isEmpty())
            throw new IOException("S4 coverage requires a nonempty pass path");
        var normalized = Utilities.deterministicObjectMapper().createArrayNode();
        long largestIteration = 0;
        for (JsonNode frame: path) {
            if (!frame.path("container").isTextual() || frame.path("container").asText().isBlank()
                    || !frame.path("pass").isTextual() || frame.path("pass").asText().isBlank()
                    || !frame.path("index").canConvertToInt() || frame.path("index").asInt() < 0
                    || !frame.path("iteration").canConvertToInt() || frame.path("iteration").asInt() < 0)
                throw new IOException("Malformed S4 pass-path frame");
            ObjectNode node = normalized.addObject();
            node.set("container", frame.get("container"));
            node.set("pass", frame.get("pass"));
            node.set("index", frame.get("index"));
            largestIteration = Math.max(largestIteration, frame.get("iteration").asLong());
        }
        Point point = this.points.computeIfAbsent(normalized.toString(), ignored -> new Point(normalized));
        point.observations++;
        point.largestIteration = Math.max(point.largestIteration, largestIteration);
        this.observations++;
    }

    ObjectNode finish(String identity) {
        ObjectNode result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("id", identity);
        result.put("observations", this.observations);
        var points = result.putArray("points");
        for (Point point: this.points.values()) {
            ObjectNode node = points.addObject();
            node.set("path", point.path);
            node.put("observations", point.observations);
            node.put("largest_iteration", point.largestIteration);
        }
        return result;
    }
}
