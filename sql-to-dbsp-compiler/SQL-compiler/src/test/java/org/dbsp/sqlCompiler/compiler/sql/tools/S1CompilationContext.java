package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Source state and effective options are inputs to lowering, not inferred from
 * SQL text. Pending LATENESS checks run after the optimizer, as in production. */
final class S1CompilationContext {
    record Decoded(CompilerOptions options, JsonNode sources, DBSPCompiler.InputSource inputSource,
                   List<DBSPCompiler.UnresolvedViewLateness> lateness) {
        void restore(DBSPCompiler compiler) {
            compiler.sources.restoreSnapshot(this.sources);
            compiler.inputSources = this.inputSource;
            compiler.restoreUnresolvedViewLateness(this.lateness);
        }
    }

    static ObjectNode encode(DBSPCompiler compiler, S1ExpressionSources sources) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var wire = mapper.createObjectNode();
        wire.set("options", mapper.valueToTree(compiler.options));
        wire.set("sources", compiler.sources.replaySnapshot());
        wire.put("input_source", compiler.inputSources.name());
        var pending = wire.putArray("unresolved_view_lateness");
        for (var late: compiler.unresolvedViewLateness()) {
            var entry = pending.addObject().put("view", late.quotedView());
            var range = late.range();
            entry.set("range", sources.origin(new SqlParserPos(range.start.line, range.start.column,
                    range.end.line, range.end.column), 0));
        }
        return wire;
    }

    static Decoded decode(JsonNode wire, S1ExpressionSources sources) throws IOException {
        fields(wire, "options", "sources", "input_source", "unresolved_view_lateness");
        var mapper = Utilities.deterministicObjectMapper();
        var options = mapper.treeToValue(wire.path("options"), CompilerOptions.class);
        // Missing fields would otherwise acquire Java defaults. Require the
        // entire public option contract, including explicit nullable values.
        ObjectNode encoded = mapper.valueToTree(options);
        if (!wire.at("/options/ioOptions").has("rustOptimizerPasses")) {
            // Snapshots predating the Rust optimizer always used Java passes.
            encoded.withObject("/ioOptions").remove("rustOptimizerPasses");
        }
        check(encoded.equals(wire.path("options")), "Compiler option fields differ");
        var source = wire.path("sources"); fields(source, "file", "program", "lines");
        text(source.path("program")); array(source.path("lines"));
        check(source.path("file").isNull() || source.path("file").isTextual(), "Invalid source filename");
        var lines = new ArrayList<String>();
        for (var line: source.path("lines")) lines.add(text(line));
        check(String.join("\n", lines).equals(sources.text(0)), "Diagnostic lines differ from user source");
        DBSPCompiler.InputSource input;
        try { input = DBSPCompiler.InputSource.valueOf(text(wire.path("input_source"))); }
        catch (IllegalArgumentException error) { throw new IOException("Unknown input source", error); }
        // submitStatementForCompilation leaves the marker at None, unlike the
        // plural API. Preserve that state even when source text is present.
        array(wire.path("unresolved_view_lateness")); var pending = new ArrayList<DBSPCompiler.UnresolvedViewLateness>();
        for (var late: wire.path("unresolved_view_lateness")) {
            fields(late, "view", "range");
            var range = late.path("range");
            check(range.path("kind").asText().equals("absent") || unsigned(range.path("source")) == 0,
                    "Pending LATENESS must refer to user source");
            pending.add(new DBSPCompiler.UnresolvedViewLateness(text(late.path("view")), new SourcePositionRange(sources.position(range))));
        }
        return new Decoded(options, source.deepCopy(), input, List.copyOf(pending));
    }
}
