package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateFunctionStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.SetOptionStatement;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Ordered typed lowering inputs. Capture freezes mutable catalog metadata at
 * the actual lowering boundary, including internal statements and DML roots. */
public final class S1Program {
    private S1Program() { }
    record Decoded(List<RelStatement> statements, S1RelNodes.Decoded relations, S1DiagnosticTimeline.Decoded diagnostics, S1CompilationContext.Decoded context) { }

    static org.dbsp.sqlCompiler.compiler.S1LoweringCompiler loweringCompiler(Decoded decoded) {
        var compiler = new org.dbsp.sqlCompiler.compiler.S1LoweringCompiler(decoded.context().options(),
                new org.apache.calcite.rex.RexBuilder(org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.S1RelTypeFactory.create()),
                decoded.relations().expressions().operators());
        decoded.context().restore(compiler);
        return compiler;
    }

    static org.dbsp.sqlCompiler.circuit.DBSPCircuit lower(Decoded decoded, org.dbsp.sqlCompiler.compiler.S1LoweringCompiler compiler) {
        return compiler.lower(decoded.statements(), index -> decoded.diagnostics().before().get(index).apply(compiler),
                () -> decoded.diagnostics().after().apply(compiler));
    }

    static ObjectNode capture(JsonNode input, S1BuiltinOperatorRegistry registry) throws IOException {
        var statements = new ArrayList<S1Statements.Capture>();
        var diagnostics = new S1DiagnosticTimeline.Capture();
        // The user text is complete before the first lowering callback. Generated
        // statements register their actual parser inputs in this same arena.
        var sourceArena = new S1ExpressionSources[1];
        org.dbsp.sqlCompiler.compiler.DBSPCompiler compiler;
        try {
            compiler = S1ExpressionCorpusProbe.compile(input, instance -> {
                instance.relationalCompletionHook = () -> diagnostics.lowered(instance);
                instance.bindingCompletionHook = () -> diagnostics.finished(instance);
                instance.relationalObservationHook = statement -> {
                    diagnostics.before(instance);
                    try {
                        if (sourceArena[0] == null) sourceArena[0] = S1ExpressionSources.of(S1ExpressionCorpusProbe.source(instance));
                        else sourceArena[0].updateUserSource(S1ExpressionCorpusProbe.source(instance));
                        statements.add(S1Statements.capture(statement, sourceArena[0]));
                    } catch (IOException error) { throw new UncheckedIOException(error); }
                };
            });
        } catch (UncheckedIOException error) { throw error.getCause(); }
        var sources = sourceArena[0];
        check(sources != null, "Program has no lowering statements");
        var roots = new ArrayList<RelNode>();
        for (var statement: statements) { var root = S1Statements.relation(statement); if (root != null) roots.add(root); }
        java.util.function.UnaryOperator<RexNode> normalize = java.util.function.UnaryOperator.identity();
        var catalog = S1Catalog.capture(S1ExpressionSharingProbe.collect(roots, normalize), statements, schema -> {
            if (!(schema instanceof RelStatement statement)) return 0;
            try { return sources.forStatement(statement.parsedStatement).source; }
            catch (IOException error) { throw new UncheckedIOException(error); }
        });
        var schemaIds = new IdentityHashMap<S1CatalogSchema.Snapshot, Integer>();
        for (int id = 0; id < catalog.schemas().size(); id++) schemaIds.put(catalog.schemas().get(id), id);
        var values = new ArrayList<List<RexNode>>(catalog.values());
        var valueSources = new ArrayList<Integer>();
        catalog.schemas().forEach(schema -> valueSources.add(schema.source()));
        var relationSources = new IdentityHashMap<RelNode, Integer>();
        for (var statement: statements) {
            var root = S1Statements.relation(statement);
            if (root != null) registerSource(root, statement.source().path("source").intValue(), relationSources);
        }
        var valueIds = new IdentityHashMap<S1Statements.Capture, Integer>();
        var declarations = new ArrayList<org.apache.calcite.sql.SqlOperator>();
        var declarationIds = new IdentityHashMap<S1Statements.Capture, Integer>();
        var additionalTypes = new ArrayList<RelDataType>(catalog.types());
        for (var statement: statements) {
            additionalTypes.addAll(S1Statements.types(statement));
            if (statement.statement() instanceof SetOptionStatement) {
                valueIds.put(statement, values.size()); values.add(S1Statements.values(statement));
                valueSources.add(statement.source().path("source").intValue());
            }
            if (statement.statement() instanceof CreateFunctionStatement function) {
                declarationIds.put(statement, declarations.size()); declarations.add(function.function);
            } else if (statement.statement() instanceof org.dbsp.sqlCompiler.compiler.frontend.statements.CreateAggregateStatement aggregate) {
                declarationIds.put(statement, declarations.size()); declarations.add(aggregate.function);
            }
        }
        var captured = S1ExpressionSharingProbe.collect(roots, normalize, values, declarations);
        var graph = S1ExpressionObjectsEncoder.encode(captured, registry, sources, additionalTypes, relation -> {
            Integer source = relationSources.get(relation);
            if (source == null) throw new IllegalStateException("Relation has no source context");
            return source;
        }, valueSources);
        var relationIds = new IdentityHashMap<RelNode, Integer>();
        for (int id = 0; id < captured.relationNodes.size(); id++) relationIds.put(captured.relationNodes.get(id), id);
        var wire = Utilities.deterministicObjectMapper().createObjectNode(); wire.set("id", input.path("id"));
        wire.put("schema_version", "statement-program-v5");
        wire.set("table", graph.inputs().table()); wire.set("expressions", graph.expressions());
        wire.set("producers", S1InputProducers.encode(captured, graph.inputs()));
        // Relative table-scan creation order is observable in RelAnd provenance ordering.
        // Carry a permutation of arena IDs, not JVM-global allocation numbers.
        var construction = wire.putArray("table_scan_order");
        captured.relationNodes.stream().filter(relation -> relation instanceof org.apache.calcite.rel.core.TableScan)
                .sorted(java.util.Comparator.comparingInt(RelNode::getId))
                .forEach(relation -> construction.add(relationIds.get(relation)));
        wire.set("properties", S1RelationProperties.encodeShared(captured, graph.inputs(), graph.operators()));
        wire.set("catalog", S1Catalog.encode(catalog, graph));
        wire.set("diagnostics", diagnostics.encode(sources));
        wire.set("context", S1CompilationContext.encode(compiler, sources));
        var ordered = wire.putArray("statements");
        for (var statement: statements) {
            Integer declaration = declarationIds.get(statement);
            ordered.add(S1Statements.encode(statement, relationIds, graph.inputs().types(),
                    statement.schema() == null ? -1 : schemaIds.get(statement.schema()), valueIds.getOrDefault(statement, -1),
                    declaration == null ? null : graph.declarations().get(declaration)));
        }
        return wire;
    }

    private static void registerSource(RelNode root, int source, IdentityHashMap<RelNode, Integer> sources) {
        var pending = new java.util.ArrayDeque<RelNode>(); pending.add(root);
        while (!pending.isEmpty()) {
            var relation = pending.removeFirst();
            // A previously defined view may be reused by a later statement. Its
            // original evaluation context still owns its expression coordinates.
            if (sources.putIfAbsent(relation, source) == null) pending.addAll(relation.getInputs());
        }
    }

    static Decoded decode(JsonNode wire, S1BuiltinOperatorRegistry registry) throws IOException {
        fields(wire, "id", "schema_version", "table", "expressions", "producers", "properties", "catalog", "statements", "diagnostics", "context", "table_scan_order");
        text(wire.path("id")); check(text(wire.path("schema_version")).equals("statement-program-v5"), "Unknown statement program version");
        var relations = S1RelNodes.decodeCatalog(wire, registry);
        var expressions = relations.expressions();
        var sources = S1ExpressionSources.decode(wire.at("/table/type_table/sources"));
        var schemas = relations.catalog().schemas();
        array(wire.path("statements")); var statements = new ArrayList<RelStatement>();
        for (var statement: wire.path("statements")) statements.add(S1Statements.decode(statement, schemas,
                relations.nodes(), expressions, sources, wire.at("/expressions/value_uses")));
        var decoded = new Decoded(List.copyOf(statements), relations,
                S1DiagnosticTimeline.decode(wire.path("diagnostics"), sources, statements.size()),
                S1CompilationContext.decode(wire.path("context"), sources));
        verify(wire, decoded, sources);
        return decoded;
    }
    private static void verify(JsonNode wire, Decoded decoded, S1ExpressionSources sources) throws IOException {
        var ids = new IdentityHashMap<RelDataType, Integer>();
        var types = decoded.relations().expressions().types();
        for (int id = 0; id < types.size(); id++) ids.put(types.get(id), id);
        var typeTable = new S1TypeTable.Encoded((ObjectNode) wire.at("/table/type_table"), ids);
        var relations = new IdentityHashMap<RelNode, Integer>();
        for (int id = 0; id < decoded.relations().nodes().size(); id++) relations.put(decoded.relations().nodes().get(id), id);
        for (int id = 0; id < decoded.statements().size(); id++) {
            var expected = wire.path("statements").get(id);
            var restored = S1Statements.encode(S1Statements.capture(decoded.statements().get(id), sources), relations, typeTable,
                    expected.path("schema").asInt(-1), expected.path("values").asInt(-1),
                    expected.has("function") ? expected.path("function").intValue() : null);
            check(expected.equals(restored), "Reconstructed statement fields differ");
        }
        for (int id = 0; id < decoded.relations().catalog().schemas().size(); id++) {
            var expected = wire.at("/catalog/schemas").get(id);
            var restored = S1CatalogSchema.encode(S1CatalogSchema.capture(decoded.relations().catalog().schemas().get(id)),
                    typeTable, sources, expected.path("values").intValue());
            check(expected.equals(restored), "Reconstructed catalog fields differ");
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3 || (!args[0].equals("export") && !args[0].equals("import")))
            throw new IOException("usage: S1Program export|import INPUT.jsonl OUTPUT_DIR");
        boolean exporting = args[0].equals("export");
        var output = java.nio.file.Path.of(args[2]); java.nio.file.Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper().copy().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var registry = new S1BuiltinOperatorRegistry(); long cases = 0, statements = 0;
        try (var reader = java.nio.file.Files.newBufferedReader(java.nio.file.Path.of(args[1]));
             var writer = java.nio.file.Files.newBufferedWriter(output.resolve("cases.jsonl"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                var input = mapper.readTree(line); ObjectNode record;
                try {
                    record = exporting ? capture(input, registry) : (ObjectNode) input;
                    var decoded = decode(record, registry); statements += decoded.statements().size();
                } catch (RuntimeException | IOException error) { throw new IOException("Statement transport failed for " + input.path("id"), error); }
                writer.write(record.toString()); writer.newLine(); writer.flush(); cases++;
            }
        }
        check(cases > 0, "Empty statement corpus");
        var complete = mapper.createObjectNode().put("complete", true).put("cases", cases).put("statements", statements)
                .put("S1_parity_proven", false).put("scope", "ordered typed statements and catalog-backed relations");
        java.nio.file.Files.writeString(output.resolve("complete.json"), complete.toPrettyString());
    }

}
