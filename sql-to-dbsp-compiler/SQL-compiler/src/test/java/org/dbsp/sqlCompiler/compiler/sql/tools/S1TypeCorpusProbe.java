package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexNodeAndFieldIndex;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;

/** Audit actual optimized relational/expression types. Unsupported transport is
 * counted separately; compiler failures abort rather than becoming exclusions. */
public final class S1TypeCorpusProbe {
    private S1TypeCorpusProbe() { }

    private static final class Types extends RexShuttle {
        final List<RelDataType> roots = new ArrayList<>();
        final IdentityHashMap<RelDataType, Boolean> seen = new IdentityHashMap<>();
        final ArrayDeque<RelNode> relations = new ArrayDeque<>();
        final List<RexLiteral> literals = new ArrayList<>();
        final IdentityHashMap<RexLiteral, Boolean> seenLiterals = new IdentityHashMap<>();
        RexBuilder searchBuilder;
        int expandedSearchCalls;

        void add(RelDataType type) {
            if (this.seen.put(type, true) == null)
                this.roots.add(type);
        }
        @Override public RexNode visitOver(RexOver node) {
            this.add(node.getType());
            return super.visitOver(node);
        }
        @Override public RexNode visitCall(RexCall node) {
            if (this.searchBuilder != null && node.getKind() == SqlKind.SEARCH) {
                RexNode expanded = RexUtil.expandSearch(this.searchBuilder, null, node);
                if (expanded == node)
                    throw new IllegalStateException("SEARCH remained unexpanded");
                this.expandedSearchCalls++;
                expanded.accept(this);
                return node;
            }
            this.add(node.getType());
            return super.visitCall(node);
        }
        @Override public RexNode visitCorrelVariable(RexCorrelVariable node) {
            this.add(node.getType());
            return super.visitCorrelVariable(node);
        }
        @Override public RexNode visitFieldAccess(RexFieldAccess node) {
            this.add(node.getType());
            return super.visitFieldAccess(node);
        }
        @Override public RexNode visitInputRef(RexInputRef node) {
            this.add(node.getType());
            return super.visitInputRef(node);
        }
        @Override public RexNode visitLocalRef(RexLocalRef node) {
            this.add(node.getType());
            return super.visitLocalRef(node);
        }
        @Override public RexNode visitLiteral(RexLiteral node) {
            if (this.seenLiterals.put(node, true) == null)
                this.literals.add(node);
            this.add(node.getType());
            return super.visitLiteral(node);
        }
        @Override public RexNode visitDynamicParam(RexDynamicParam node) {
            this.add(node.getType());
            return super.visitDynamicParam(node);
        }
        @Override public RexNode visitRangeRef(RexRangeRef node) {
            this.add(node.getType());
            return super.visitRangeRef(node);
        }
        @Override public RexNode visitLambda(RexLambda node) {
            this.add(node.getType());
            return super.visitLambda(node);
        }
        @Override public RexNode visitLambdaRef(RexLambdaRef node) {
            this.add(node.getType());
            return super.visitLambdaRef(node);
        }
        @Override public RexNode visitTableInputRef(RexTableInputRef node) {
            this.add(node.getType());
            return super.visitTableInputRef(node);
        }
        @Override public RexNode visitPatternFieldRef(RexPatternFieldRef node) {
            this.add(node.getType());
            return super.visitPatternFieldRef(node);
        }
        @Override public RexNode visitNodeAndFieldIndex(RexNodeAndFieldIndex node) {
            this.add(node.getType());
            return super.visitNodeAndFieldIndex(node);
        }
        @Override public RexNode visitSubQuery(RexSubQuery node) {
            this.add(node.getType());
            this.relations.add(node.rel);
            return super.visitSubQuery(node);
        }

        void collect(DBSPCompiler compiler) {
            var plans = compiler.getPlans(new IndentStream(new StringBuilder()));
            plans.entrySet().stream().sorted(Comparator.comparingInt(java.util.Map.Entry::getValue))
                    .forEach(entry -> this.relations.add(entry.getKey()));
            var visited = new IdentityHashMap<RelNode, Boolean>();
            while (!this.relations.isEmpty()) {
                RelNode node = this.relations.removeFirst();
                if (visited.put(node, true) != null)
                    continue;
                this.add(node.getRowType());
                S1RelExpressions.visit(node, this, this::add);
                this.relations.addAll(node.getInputs());
            }
        }
    }

    record LiteralCapture(List<RexLiteral> literals, int expandedSearchCalls) { }

    static LiteralCapture literals(DBSPCompiler compiler) {
        Types types = new Types();
        // The lowering uses this exact expansion before compiling Rex values.
        types.searchBuilder = compiler.getSqlToRelCompiler().getRexBuilder();
        types.collect(compiler);
        return new LiteralCapture(List.copyOf(types.literals), types.expandedSearchCalls);
    }

    private static ObjectNode audit(JsonNode input) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var options = mapper.treeToValue(input.get("options"), CompilerOptions.class);
        options.ioOptions.inputCircuit = true;
        var compiler = new DBSPCompiler(options);
        JsonNode submissions = input.get("submissions");
        if (submissions == null || !submissions.isArray() || submissions.isEmpty())
            throw new IOException("Missing ordered SQL submissions");
        for (JsonNode submission: submissions) {
            if (submission.path("batch").intValue() != 0)
                throw new IOException("Multiple compilation batches require explicit replay");
            if (submission.path("many").booleanValue())
                compiler.submitStatementsForCompilation(submission.path("sql").textValue());
            else
                compiler.submitStatementForCompilation(submission.path("sql").textValue());
        }
        if (compiler.getFinalCircuit(false) == null)
            throw new IOException("Corpus case produced no circuit: " + input.path("id"));
        compiler.throwIfErrorsOccurred();
        Types types = new Types();
        types.collect(compiler);
        var result = mapper.createObjectNode();
        result.set("id", input.get("id"));
        result.put("type_roots", types.roots.size());
        var unsupported = result.putArray("unsupported");
        List<RelDataType> supported = new ArrayList<>();
        for (RelDataType type: types.roots) {
            try {
                S1TypeTable.encode(List.of(type));
                supported.add(type);
            } catch (IOException ex) {
                String message = ex.getMessage();
                if (message == null || !(message.startsWith("Unsupported scalar type:")
                        || message.startsWith("Scalar type export would lose type identity:")
                        || message.startsWith("Named time frames") || message.startsWith("Partial character metadata")))
                    throw ex;
                var missing = unsupported.addObject();
                missing.put("class", type.getClass().getName());
                missing.put("type", type.getFullTypeString());
                missing.put("reason", message);
            }
        }
        result.put("supported_roots", supported.size());
        result.set("type_table", S1TypeTable.encode(supported));
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: S1TypeCorpusProbe INPUT.jsonl OUTPUT_DIR");
        Path output = Path.of(args[1]);
        Files.createDirectory(output);
        var mapper = Utilities.deterministicObjectMapper();
        mapper.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        int cases = 0;
        long totalRoots = 0;
        long unsupportedRoots = 0;
        try (var reader = Files.newBufferedReader(Path.of(args[0]));
             var writer = Files.newBufferedWriter(output.resolve("cases.jsonl"), StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ObjectNode result = audit(mapper.readTree(line));
                writer.write(result.toString());
                writer.newLine();
                writer.flush();
                cases++;
                totalRoots += result.path("type_roots").intValue();
                unsupportedRoots += result.path("unsupported").size();
            }
        }
        var complete = mapper.createObjectNode();
        complete.put("schema_version", 1);
        complete.put("complete", true);
        complete.put("cases", cases);
        complete.put("type_roots", totalRoots);
        complete.put("unsupported_roots", unsupportedRoots);
        complete.put("runtime_executed", false);
        complete.put("S1_parity_proven", false);
        Files.writeString(output.resolve("complete.json"), complete.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}
