package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolved function metadata. Bodies reference the shared expression arena;
 * diagnostic text is retained as data and is never parsed or executed. */
final class S1FunctionObjects {
    sealed interface Body permits ScopedBody, ObjectBody {
        int root();
        Integer source();
    }
    record ScopedBody(int scope, int input, int root, Integer source) implements Body { }
    record ObjectBody(int environment, int root, Integer source) implements Body { }
    record Usage(long references, long bytes) { }
    private S1FunctionObjects() { }

    static ObjectNode encode(ExternalFunction function, Map<RelDataType, Integer> types,
                             S1ExpressionSources sources, Body body) throws IOException {
        var entry = Utilities.deterministicObjectMapper().createObjectNode();
        entry.put("kind", "function").put("name", function.getName()).put("return_type", type(types, function.returnType))
                .put("generated", function.generated);
        entry.set("origin", sources.origin(position(function.position)));
        var diagnostic = entry.putObject("diagnostic");
        diagnostic.set("origin", sources.origin(position(function.node.getPositionRange())));
        diagnostic.put("text", function.node.toString()).put("internal_text", function.node.toInternalString())
                .put("empty", function.node.isEmpty());
        var parameters = entry.putArray("parameters");
        for (var parameter: function.parameterList)
            parameters.addObject().put("name", parameter.getName()).put("index", parameter.getIndex())
                    .put("ty", type(types, parameter.getType()));
        var value = entry.putObject("body");
        if (function.body == null) { check(body == null, "External function unexpectedly has a body reference"); value.put("kind", "external"); }
        else {
            check(body != null, "SQL function has no collected body");
            if (body instanceof ScopedBody scoped)
                value.put("kind", "sql").put("scope", scoped.scope()).put("input", scoped.input());
            else if (body instanceof ObjectBody objects)
                value.put("kind", "object_sql").put("environment", objects.environment());
            else throw new IOException("Unknown function body context");
            value.put("root", body.root());
            var source = value.putObject("source");
            if (body.source() == null) source.put("kind", "inherited");
            else source.put("kind", "captured").put("source", body.source());
        }
        return entry;
    }

    static Usage validate(JsonNode entry, List<RelDataType> types, S1ExpressionSources sources) throws IOException {
        fields(entry, "kind", "name", "return_type", "parameters", "generated", "origin", "diagnostic", "body");
        check(text(entry, "kind").equals("function") && !text(entry, "name").isEmpty(), "Invalid function name or kind");
        type(entry.path("return_type"), types); bool(entry.path("generated")); sources.position(entry.path("origin"));
        JsonNode diagnostic = entry.path("diagnostic");
        fields(diagnostic, "origin", "text", "internal_text", "empty");
        sources.position(diagnostic.path("origin")); bool(diagnostic.path("empty"));
        long bytes = bytes(text(entry, "name")) + bytes(text(diagnostic, "text")) + bytes(text(diagnostic, "internal_text"));
        long references = 3;
        JsonNode parameters = entry.path("parameters"); check(parameters.isArray() && parameters.size() <= 100_000, "Invalid function parameters");
        for (JsonNode parameter: parameters) {
            fields(parameter, "name", "index", "ty"); check(!text(parameter, "name").isEmpty(), "Empty function parameter name");
            unsigned(parameter.path("index")); type(parameter.path("ty"), types);
            bytes += bytes(text(parameter, "name")); references++;
        }
        JsonNode body = entry.path("body");
        if (text(body, "kind").equals("external")) fields(body, "kind");
        else {
            if (text(body, "kind").equals("sql")) {
                fields(body, "kind", "scope", "input", "root", "source");
                for (String field: List.of("scope", "input", "root")) unsigned(body.path(field));
                references += 3;
            } else {
                fields(body, "kind", "environment", "root", "source");
                check(text(body, "kind").equals("object_sql"), "Unsupported function body kind");
                unsigned(body.path("environment")); unsigned(body.path("root")); references += 2;
            }
            JsonNode source = body.path("source");
            if (text(source, "kind").equals("inherited")) fields(source, "kind");
            else {
                fields(source, "kind", "source");
                check(text(source, "kind").equals("captured"), "Unsupported function source kind");
                sources.text(unsigned(source.path("source"))); references++;
            }
        }
        return new Usage(references, bytes);
    }

    static ExternalFunction create(JsonNode entry, List<RelDataType> types,
                                   S1ExpressionSources sources, List<RexNode> nodes) throws IOException {
        var parameters = new ArrayList<org.apache.calcite.rel.type.RelDataTypeField>();
        for (JsonNode parameter: entry.path("parameters"))
            parameters.add(new RelDataTypeFieldImpl(text(parameter, "name"), unsigned(parameter.path("index")), type(parameter.path("ty"), types)));
        JsonNode diagnostic = entry.path("diagnostic");
        var range = new SourcePositionRange(sources.position(diagnostic.path("origin")));
        String display = text(diagnostic, "text"), internal = text(diagnostic, "internal_text");
        boolean empty = bool(diagnostic.path("empty"));
        CalciteObject node = new CalciteObject(range) {
            @Override public boolean isEmpty() { return empty; }
            @Override public String toString() { return display; }
            @Override public String toInternalString() { return internal; }
        };
        JsonNode body = entry.path("body");
        RexNode root = body.path("kind").asText().equals("external") ? null : nodes.get(unsigned(body.path("root")));
        check(root != null || body.path("kind").asText().equals("external"), "Function body dependency is not constructed");
        return new ExternalFunction(text(entry, "name"), node, new SourcePositionRange(sources.position(entry.path("origin"))),
                type(entry.path("return_type"), types), parameters, root, bool(entry.path("generated")),
                body.path("source").path("kind").asText().equals("captured")
                        ? sources.text(unsigned(body.path("source").path("source"))) : null);
    }

    private static SqlParserPos position(SourcePositionRange range) {
        return new SqlParserPos(range.start.line, range.start.column, range.end.line, range.end.column);
    }
    private static int type(Map<RelDataType, Integer> types, RelDataType value) throws IOException {
        Integer id = types.get(value); check(id != null, "Function type is outside its arena"); return id;
    }
    private static RelDataType type(JsonNode id, List<RelDataType> types) throws IOException {
        int index = unsigned(id); check(index < types.size(), "Function type is outside its arena"); return types.get(index);
    }
    private static boolean bool(JsonNode value) throws IOException { check(value.isBoolean(), "Missing function boolean"); return value.booleanValue(); }
    private static int unsigned(JsonNode value) throws IOException {
        check(value.isIntegralNumber() && value.canConvertToInt() && value.intValue() >= 0, "Invalid function reference or index"); return value.intValue();
    }
    private static String text(JsonNode node, String field) throws IOException {
        check(node.path(field).isTextual(), "Missing function string: " + field); return node.path(field).textValue();
    }
    private static long bytes(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }
    private static void fields(JsonNode node, String... names) throws IOException {
        check(node.isObject(), "Missing function object"); Set<String> actual = new HashSet<>(); node.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing function fields");
    }
    private static void check(boolean condition, String message) throws IOException { if (!condition) throw new IOException(message); }
}
