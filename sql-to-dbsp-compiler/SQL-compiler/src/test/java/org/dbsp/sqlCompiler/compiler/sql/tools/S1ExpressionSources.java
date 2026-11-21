package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Source records for an expression fragment. The caller supplies the text to
 * which Calcite coordinates refer; API submission boundaries must be handled
 * by a future whole-program source adapter, not guessed here. */
final class S1ExpressionSources {
    record Source(String name, String text) {
        Source {
            java.util.Objects.requireNonNull(name);
            java.util.Objects.requireNonNull(text);
        }
    }

    private static final class Arena {
        final List<Source> sources = new ArrayList<>();
        final List<String[]> lines = new ArrayList<>();
        long sourceBytes;
    }
    private final Arena arena;
    final int source;

    private S1ExpressionSources(Arena arena, int source) {
        this.arena = arena;
        this.source = source;
    }

    S1ExpressionSources inSource(int source) throws IOException {
        check(source >= 0 && source < this.arena.sources.size(), "Source reference is outside its arena");
        return new S1ExpressionSources(this.arena, source);
    }

    S1ExpressionSources forStatement(org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement statement)
            throws IOException {
        if (statement.visible()) return this.inSource(0);
        String text = statement.generatedSource();
        check(text != null, "Generated statement has no parser source");
        for (int id = 1; id < this.arena.sources.size(); id++)
            if (this.arena.sources.get(id).text().equals(text)) return this.inSource(id);
        return this.inSource(this.add(new Source("compiler-generated SQL", text)));
    }

    private S1ExpressionSources(List<Source> sources) throws IOException {
        this(new Arena(), 0);
        for (Source source: sources) this.add(source);
    }

    int add(Source source) throws IOException {
        check(this.arena.sources.size() < 10_000, "Source count exceeds limit");
        this.arena.sourceBytes += source.name().getBytes(StandardCharsets.UTF_8).length
                + source.text().getBytes(StandardCharsets.UTF_8).length;
        check(this.arena.sourceBytes <= 16 * 1024 * 1024, "Source bytes exceed limit");
        int id = this.arena.sources.size(); this.arena.sources.add(source); this.arena.lines.add(source.text().split("\n", -1)); return id;
    }

    void updateUserSource(Source source) throws IOException {
        var previous = this.arena.sources.get(0);
        check(previous.name().equals(source.name()) && source.text().startsWith(previous.text()),
                "User source changed instead of extending across submissions");
        long bytes = this.arena.sourceBytes - previous.text().getBytes(StandardCharsets.UTF_8).length
                + source.text().getBytes(StandardCharsets.UTF_8).length;
        check(bytes <= 16 * 1024 * 1024, "Source bytes exceed limit");
        this.arena.sources.set(0, source);
        this.arena.lines.set(0, source.text().split("\n", -1));
        this.arena.sourceBytes = bytes;
    }

    boolean hasSources() { return !this.arena.sources.isEmpty(); }

    String text(int source) throws IOException {
        check(source >= 0 && source < this.arena.sources.size(), "Function source reference is outside its arena");
        return this.arena.sources.get(source).text();
    }

    static S1ExpressionSources of(Source source) throws IOException {
        return new S1ExpressionSources(source == null ? List.of() : List.of(source));
    }

    static S1ExpressionSources decode(JsonNode wire) throws IOException {
        check(wire.isArray(), "Missing source array");
        var sources = new ArrayList<Source>();
        check(wire.size() <= 10_000, "Source count exceeds limit");
        for (JsonNode source: wire) {
            fields(source, "name", "text");
            check(source.path("name").isTextual() && source.path("text").isTextual(), "Invalid source record");
            sources.add(new Source(source.path("name").textValue(), source.path("text").textValue()));
        }
        return new S1ExpressionSources(sources);
    }

    void encode(ArrayNode output) {
        for (Source source: this.arena.sources)
            output.addObject().put("name", source.name()).put("text", source.text());
    }

    ObjectNode origin(SqlParserPos position) throws IOException {
        return this.origin(position, this.source);
    }

    ObjectNode origin(SqlParserPos position, int source) throws IOException {
        ObjectNode origin = Utilities.deterministicObjectMapper().createObjectNode();
        check(!position.isQuoted(), "Quoted parser positions require a separate contract");
        if (position.equals(SqlParserPos.ZERO)) return origin.put("kind", "absent");
        check(source >= 0 && source < this.arena.sources.size(), "Source-bearing call requires an explicit source record");
        boolean generated = position.getLineNum() == 0 || position.getColumnNum() == 0
                || position.getEndLineNum() == 0 || position.getEndColumnNum() == 0;
        origin.put("kind", generated ? "generated" : "source").put("source", source);
        origin.putObject("start").put("line", position.getLineNum()).put("column", position.getColumnNum());
        origin.putObject("end").put("line", position.getEndLineNum()).put("column", position.getEndColumnNum());
        check(this.position(origin).equals(position), "Parser position changed during source encoding");
        return origin;
    }

    SqlParserPos position(JsonNode origin) throws IOException {
        if (origin.path("kind").asText().equals("absent")) {
            fields(origin, "kind"); return SqlParserPos.ZERO;
        }
        fields(origin, "kind", "source", "start", "end");
        String kind = origin.path("kind").asText();
        boolean generated = kind.equals("generated");
        check(generated || kind.equals("source"), "Unknown source origin");
        int source = unsigned(origin.path("source"));
        check(source < this.arena.sources.size(), "Source reference is outside its arena");
        int[] start = this.coordinate(origin.path("start"), source, generated);
        int[] end = this.coordinate(origin.path("end"), source, generated);
        if (generated) {
            boolean hasUnknown = start[0] == 0 || start[1] == 0 || end[0] == 0 || end[1] == 0;
            boolean allUnknown = start[0] == 0 && start[1] == 0 && end[0] == 0 && end[1] == 0;
            check(hasUnknown && !allUnknown, "Generated ranges require partially unknown coordinates");
        }
        check(start[0] < end[0] || (start[0] == end[0] && start[1] <= end[1]), "Reversed source range");
        return new SqlParserPos(start[0], start[1], end[0], end[1]);
    }

    private int[] coordinate(JsonNode coordinate, int source, boolean generated) throws IOException {
        fields(coordinate, "line", "column");
        int line = unsigned(coordinate.path("line"));
        int column = unsigned(coordinate.path("column"));
        String[] lines = this.arena.lines.get(source);
        check((generated || line > 0) && line <= lines.length,
                "Source line " + line + " is outside its text (" + lines.length + " lines)");
        // Java String.length counts UTF-16 code units, as Calcite does. The
        // position immediately after a line is valid for newline/EOF anchors.
        check(generated || column > 0, "Source column must be one-based");
        if (line > 0)
            check(column <= lines[line - 1].length() + 1,
                    "Source column " + column + " is outside line " + line
                            + " (" + lines[line - 1].length() + " UTF-16 code units)");
        return new int[]{line, column};
    }

    private static int unsigned(JsonNode value) throws IOException {
        check(value.isIntegralNumber() && value.canConvertToInt() && value.intValue() >= 0, "Invalid source coordinate or ID");
        return value.intValue();
    }

    private static void fields(JsonNode node, String... names) throws IOException {
        check(node.isObject(), "Invalid source object");
        Set<String> actual = new HashSet<>(); node.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing source fields");
    }

    private static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
