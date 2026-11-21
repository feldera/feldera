package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlToRelCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.aggregate.LinearAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.MinMaxAggregate;
import org.dbsp.sqlCompiler.ir.aggregate.NonLinearAggregate;
import org.dbsp.util.IndentStream;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;

/** Observes actual validated syntax and the immediate SQL-to-Rel result.
 * Never derives types, rewrites nodes, or substitutes a Java inference result
 * into the Rust strategy. SQL-to-Rel can already simplify expressions, so its
 * graph is kept separate from the earlier validated syntax graph. */
public final class TypeBindingProbe implements SqlToRelCompiler.BindingObserver {
    private final S1BuiltinOperatorRegistry registry;
    final ArrayNode observations = Utilities.deterministicObjectMapper().createArrayNode();
    final ArrayNode errors = Utilities.deterministicObjectMapper().createArrayNode();

    private TypeBindingProbe(DBSPCompiler compiler) {
        this.registry = new S1BuiltinOperatorRegistry(compiler.getCustomFunctions().getDescriptions());
    }

    private ObjectNode type(RelDataType type) {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("sql_type", type.getSqlTypeName().name())
                .put("nullable", type.isNullable())
                .put("precision", type.getPrecision()).put("scale", type.getScale())
                .put("full", type.getFullTypeString());
        if (type.getCharset() != null) result.put("charset", type.getCharset().name());
        if (type.getCollation() != null) {
            result.put("collation", type.getCollation().getCollationName());
            result.put("coercibility", type.getCollation().getCoercibility().name());
        }
        if (type.isStruct()) {
            var fields = result.putArray("fields");
            type.getFieldList().forEach(field -> fields.addObject().put("name", field.getName())
                    .put("index", field.getIndex()).set("type", this.type(field.getType())));
        }
        if (type.getComponentType() != null) result.set("component", this.type(type.getComponentType()));
        if (type.getKeyType() != null) result.set("key", this.type(type.getKeyType()));
        if (type.getValueType() != null) result.set("value", this.type(type.getValueType()));
        return result;
    }

    private ObjectNode operator(SqlOperator operator, boolean structural) {
        var result = Utilities.deterministicObjectMapper().createObjectNode()
                .put("name", operator.getName()).put("kind", operator.getKind().name())
                .put("class", operator.getClass().getName()).put("syntax", operator.getSyntax().name());
        if (structural) return result.put("identity_status", "syntax_container");
        try {
            var identity = this.registry.identify(operator);
            if (this.registry.resolve(identity) != operator)
                throw new IOException("Operator registry did not preserve object identity");
            result.put("identity_status", "registered").put("registry", identity.registry())
                    .put("registry_version", identity.version()).put("key", identity.key());
        } catch (IOException exception) {
            result.put("identity_status", "unregistered");
            this.errors.add(exception.getMessage());
        }
        return result;
    }

    @Override public void validated(SqlNode root, SqlValidator validator) {
        var record = this.observations.addObject().put("stage", "validated_sql");
        var nodes = record.putArray("nodes");
        var pending = new ArrayList<SqlNode>();
        var ids = new IdentityHashMap<SqlNode, Integer>();
        java.util.function.ToIntFunction<SqlNode> intern = node -> {
            Integer id = ids.get(node);
            if (id == null) { id = pending.size(); ids.put(node, id); pending.add(node); }
            return id;
        };
        record.put("root", intern.applyAsInt(root));
        for (int id = 0; id < pending.size(); id++) {
            var node = pending.get(id);
            var entry = nodes.addObject().put("id", id).put("class", node.getClass().getName())
                    .put("kind", node.getKind().name());
            var pos = node.getParserPosition();
            entry.putObject("position").put("line", pos.getLineNum()).put("column", pos.getColumnNum())
                    .put("end_line", pos.getEndLineNum()).put("end_column", pos.getEndColumnNum());
            var known = validator.getValidatedNodeTypeIfKnown(node);
            entry.put("type_known", known != null);
            if (known != null) entry.set("type", this.type(known));
            if (node instanceof SqlCall call) {
                entry.set("operator", this.operator(call.getOperator(), call instanceof SqlSelect));
                var operands = entry.putArray("operands");
                for (var operand: call.getOperandList()) {
                    if (operand == null) operands.addNull();
                    else operands.add(intern.applyAsInt(operand));
                }
            } else if (node instanceof SqlNodeList list) {
                var elements = entry.putArray("elements");
                for (var element: list) elements.add(intern.applyAsInt(element));
            } else if (node instanceof SqlIdentifier identifier) {
                var names = entry.putArray("names");
                var quoted = entry.putArray("quoted");
                for (int part = 0; part < identifier.names.size(); part++) {
                    names.add(identifier.names.get(part)); quoted.add(identifier.isComponentQuoted(part));
                }
            } else if (node instanceof SqlLiteral literal) {
                entry.put("literal_type", literal.getTypeName().name()).put("sql", literal.toString());
            } else if (node instanceof SqlDataTypeSpec specification) {
                entry.put("sql", specification.toString());
            } else {
                this.errors.add("Uncaptured validated syntax class: " + node.getClass().getName());
            }
        }
    }

    @Override public void converted(RelRoot root) {
        var record = this.observations.addObject().put("stage", "converted_rel");
        record.set("validated_row_type", this.type(root.validatedRowType));
        var relations = record.putArray("relations");
        var expressions = record.putArray("expressions");
        var pending = new ArrayList<RelNode>();
        var ids = new IdentityHashMap<RelNode, Integer>();
        var rexPending = new ArrayList<RexNode>();
        var rexIds = new IdentityHashMap<RexNode, Integer>();
        java.util.function.ToIntFunction<RelNode> intern = node -> {
            Integer id = ids.get(node);
            if (id == null) { id = pending.size(); ids.put(node, id); pending.add(node); }
            return id;
        };
        java.util.function.ToIntFunction<RexNode> rexIntern = node -> {
            Integer id = rexIds.get(node);
            if (id == null) { id = rexPending.size(); rexIds.put(node, id); rexPending.add(node); }
            return id;
        };
        record.put("root", intern.applyAsInt(root.rel));
        for (int id = 0; id < pending.size(); id++) {
            var node = pending.get(id);
            var entry = relations.addObject().put("id", id).put("kind", node.getRelTypeName());
            entry.set("row_type", this.type(node.getRowType()));
            var inputs = entry.putArray("inputs");
            node.getInputs().forEach(input -> inputs.add(intern.applyAsInt(input)));
            var roots = entry.putArray("expressions");
            var declaredTypes = entry.putArray("local_types");
            S1RelExpressions.forEachRoot(node, expression -> roots.add(rexIntern.applyAsInt(expression)),
                    declared -> declaredTypes.add(this.type(declared)));
            if (node instanceof org.apache.calcite.rel.core.Aggregate aggregate) {
                var group = entry.putArray("group_set");
                aggregate.getGroupSet().forEach(group::add);
                var calls = entry.putArray("aggregate_calls");
                for (var call: aggregate.getAggCallList()) {
                    var item = calls.addObject().put("distinct", call.isDistinct())
                            .put("approximate", call.isApproximate()).put("ignore_nulls", call.ignoreNulls())
                            .put("filter_argument", call.filterArg);
                    item.set("operator", this.operator(call.getAggregation(), false));
                    item.set("type", this.type(call.getType()));
                    var arguments = item.putArray("arguments");
                    for (int argument: call.getArgList()) {
                        arguments.addObject().put("input_field", argument).set("type",
                                this.type(aggregate.getInput().getRowType().getFieldList().get(argument).getType()));
                    }
                    var extra = item.putArray("rex_arguments");
                    call.rexList.forEach(expression -> extra.add(rexIntern.applyAsInt(expression)));
                }
            }
        }
        for (int id = 0; id < rexPending.size(); id++) {
            var node = rexPending.get(id);
            var entry = expressions.addObject().put("id", id).put("class", node.getClass().getName())
                    .put("kind", node.getKind().name());
            entry.set("type", this.type(node.getType()));
            if (node instanceof RexCall call) {
                entry.set("operator", this.operator(call.getOperator(), false));
                var operands = entry.putArray("operands");
                call.getOperands().forEach(operand -> operands.add(rexIntern.applyAsInt(operand)));
            } else if (node instanceof RexInputRef ref) {
                entry.put("index", ref.getIndex());
            } else if (node instanceof RexLiteral literal) {
                entry.put("literal", literal.toString());
            } else if (node instanceof RexFieldAccess access) {
                entry.put("field", access.getField().getIndex())
                        .put("reference", rexIntern.applyAsInt(access.getReferenceExpr()));
            } else {
                this.errors.add("Uncaptured converted expression class: " + node.getClass().getName());
            }
        }
    }

    /** Standalone single-JVM collector; the input is an explicit selected manifest. */
    public static void main(String[] args) throws IOException {
        boolean s2Expressions = args.length == 4 && args[3].equals("--s2-expressions");
        if (args.length != 3 && !s2Expressions)
            throw new IllegalArgumentException(
                    "usage: TypeBindingProbe INPUT.jsonl OUTPUT_DIRECTORY EXPECTED_COUNT [--s2-expressions]");
        long expected = Long.parseLong(args[2]);
        if (expected < 1 || expected > 100_000) throw new IllegalArgumentException("Invalid expected count");
        var mapper = Utilities.deterministicObjectMapper()
                .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        var directory = java.nio.file.Path.of(args[1]);
        java.nio.file.Files.createDirectory(directory);
        long count = 0, accepted = 0, bugs = 0, captureErrors = 0, projections = 0;
        long setProjections = 0, aggregateProjections = 0;
        var seen = new java.util.HashSet<Long>();
        try (var reader = java.nio.file.Files.newBufferedReader(java.nio.file.Path.of(args[0]));
             var writer = java.nio.file.Files.newBufferedWriter(
                     directory.resolve(s2Expressions ? "s2-expressions.jsonl" : "bindings.jsonl"),
                     java.nio.file.StandardOpenOption.CREATE_NEW)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > 65_536 || count >= expected)
                    throw new IOException("Oversized or excess input record");
                var probe = mapper.readTree(line);
                if (!probe.path("schema_version").isIntegralNumber() || probe.path("schema_version").asLong() != 1
                        || !probe.path("id").isIntegralNumber() || !probe.path("id").canConvertToLong()
                        || probe.path("id").longValue() < 0
                        || !seen.add(probe.path("id").longValue()) || !probe.path("program").isTextual())
                    throw new IOException("Invalid or duplicate probe record");
                var result = compile(probe.path("program").textValue(), !s2Expressions, false, s2Expressions);
                result.put("schema_version", 1).put("runtime_verified", false);
                result.set("probe", probe);
                count++;
                if (result.path("accepted").asBoolean()) accepted++;
                if (result.path("compiler_bug").asBoolean()) bugs++;
                if (!result.path("capture_complete").asBoolean()) captureErrors++;
                if (result.path("s2_projection").path("status").asText().equals("captured")) projections++;
                if (result.path("s2_projection").path("status").asText().equals("set_maps")) setProjections++;
                if (result.path("s2_projection").path("status").asText().equals("aggregate")) aggregateProjections++;
                writer.write(mapper.writeValueAsString(result)); writer.newLine();
                if (count % 1000 == 0) System.err.println("Binding probes: " + count);
            }
        }
        if (count != expected) throw new IOException("Truncated probe input: " + count + " != " + expected);
        var manifest = mapper.createObjectNode().put("schema_version", 1)
                .put("expected_probes", expected).put("probes", count).put("accepted", accepted)
                .put("rejected", count - accepted).put("compiler_bugs", bugs).put("capture_errors", captureErrors)
                .put("complete", bugs == 0 && captureErrors == 0).put("runtime_verified", false)
                .put("scope", s2Expressions ? "Unoptimized Java S2 scalar projections and set coercions"
                        : "Validated SQL and immediate converted Rel; no Rust cast/function comparison or C9 claim");
        if (s2Expressions) manifest.put("captured_projections", projections)
                .put("captured_set_map_projections", setProjections)
                .put("captured_aggregate_components", aggregateProjections)
                .put("accepted_outside_projection_scope", accepted - projections - setProjections - aggregateProjections);
        java.nio.file.Files.writeString(directory.resolve("complete.json"), mapper.writeValueAsString(manifest),
                java.nio.file.StandardOpenOption.CREATE_NEW);
        if (bugs != 0 || captureErrors != 0) throw new IOException("Incomplete binding capture: " + manifest);
    }

    static ObjectNode compile(String program, boolean observe, boolean includeCircuit) throws IOException {
        return compile(program, observe, includeCircuit, false);
    }

    static ObjectNode compile(String program, boolean observe, boolean includeCircuit, boolean s2Expressions)
            throws IOException {
        DBSPVariablePath.reset();
        var options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.ioOptions.inputCircuit = true;
        options.languageOptions.optimizationLevel = 0;
        options.languageOptions.throwOnError = false;
        options.languageOptions.generateInputForEveryTable = true;
        var compiler = new DBSPCompiler(options);
        var collector = new TypeBindingProbe(compiler);
        if (observe) compiler.getSqlToRelCompiler().bindingObserver = collector;
        compiler.submitStatementsForCompilation(program);
        var circuit = compiler.getFinalCircuit(false);
        var mapper = Utilities.deterministicObjectMapper();
        var result = mapper.createObjectNode();
        result.put("accepted", circuit != null && !compiler.hasErrors());
        result.put("compiler_bug", compiler.messages.messages.stream().anyMatch(message ->
                message.errorType.equals("Compiler error")
                        || message.errorType.startsWith("This is a bug in the compiler")));
        result.set("diagnostics", compiler.messages.toJson(compiler.sources));
        result.set("schema", compiler.metadata.asJson());
        result.set("options", mapper.valueToTree(options));
        result.set("bindings", collector.observations);
        result.set("capture_errors", collector.errors);
        result.put("capture_complete", collector.errors.isEmpty());
        if (s2Expressions && circuit != null && !compiler.hasErrors())
            result.set("s2_projection", s2Projection(compiler, circuit));
        if (includeCircuit && circuit != null && !compiler.hasErrors()) {
            var stream = new IndentStream(new StringBuilder()).setIndentAmount(0);
            var visitor = new ToJsonOuterVisitor(compiler, 1,
                    new ToJsonInnerVisitor(compiler, new JsonStream(stream), 1));
            visitor.apply(circuit);
            result.set("s2", mapper.readTree(visitor.getJsonString()));
        }
        return result;
    }

    private static ObjectNode s2Projection(DBSPCompiler compiler, DBSPCircuit circuit) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var result = mapper.createObjectNode();
        DBSPViewOperator selected = null;
        for (var operator: circuit.getAllOperators()) {
            if (operator instanceof DBSPViewOperator view && view.viewName.name().equals("v")) {
                if (selected != null)
                    throw new IOException("More than one view named v");
                selected = view;
            }
        }
        if (selected == null)
            return result.put("status", "outside_scope").put("reason", "missing view v");
        var source = selected.inputs.get(0).node();
        if (source instanceof DBSPMapOperator map
                && map.getClosureFunction().body instanceof DBSPTupleExpression tuple && tuple.size() == 1)
            return printScalar(compiler, tuple.fields[0]).put("status", "captured");
        var reachable = java.util.Collections.newSetFromMap(new IdentityHashMap<DBSPOperator, Boolean>());
        var pending = new java.util.ArrayDeque<DBSPOperator>();
        pending.add(source);
        while (!pending.isEmpty()) {
            var next = pending.remove();
            if (reachable.add(next))
                next.inputs.forEach(input -> pending.add(input.node()));
        }
        var maps = mapper.createArrayNode();
        var aggregateParts = mapper.createArrayNode();
        boolean setShape = true;
        for (var operator: circuit.getAllOperators()) {
            if (!reachable.contains(operator)) continue;
            if (operator instanceof DBSPMapOperator map
                    && map.getClosureFunction().body instanceof DBSPTupleExpression tuple && tuple.size() == 1) {
                maps.add(printScalar(compiler, tuple.fields[0]).put("operator", "map"));
            } else if (operator instanceof DBSPFilterOperator filter) {
                maps.add(printScalar(compiler, filter.getClosureFunction().body).put("operator", "filter"));
            } else if (operator instanceof DBSPStreamAggregateOperator aggregate
                    && aggregate.aggregateList != null && aggregate.aggregateList.aggregates.size() == 1) {
                var component = aggregate.aggregateList.aggregates.get(0);
                var parts = aggregateParts.addObject().put("kind", component.getClass().getSimpleName());
                parts.set("empty", printScalar(compiler, component.getEmptySetResult()));
                if (component instanceof LinearAggregate linear) {
                    parts.set("map", printScalar(compiler, linear.map.body));
                    parts.set("post", printScalar(compiler, linear.postProcess.body));
                } else if (component instanceof NonLinearAggregate nonLinear) {
                    parts.set("zero", printScalar(compiler, nonLinear.zero));
                    parts.set("increment", printScalar(compiler, nonLinear.increment.body));
                    if (nonLinear.postProcess == null)
                        parts.putNull("post");
                    else
                        parts.set("post", printScalar(compiler, nonLinear.postProcess.body));
                    parts.put("semigroup", ToRustInnerVisitor.toRustString(compiler, nonLinear.semigroup, null, true));
                    if (component instanceof MinMaxAggregate comparison)
                        parts.set("compared_value", printScalar(compiler, comparison.comparedValue.body));
                } else {
                    throw new IOException("Unexpected S2 aggregate component " + component.getClass().getName());
                }
            } else if (!(operator instanceof DBSPSourceMultisetOperator)
                    && !java.util.Set.of("DBSPSumOperator", "DBSPNegateOperator", "DBSPPositiveOperator",
                            "DBSPDifferentiateOperator", "DBSPIntegrateOperator")
                            .contains(operator.getClass().getSimpleName())) {
                setShape = false;
            }
        }
        if (!aggregateParts.isEmpty()) {
            result.set("components", aggregateParts);
            return result.put("status", "aggregate");
        }
        if (setShape && maps.size() >= 2) {
            result.set("maps", maps);
            return result.put("status", "set_maps");
        }
        return result.put("status", "outside_scope")
                .put("reason", "expected a scalar projection or set map cone")
                .put("input_operator", source.getClass().getSimpleName());
    }

    private static ObjectNode printScalar(DBSPCompiler compiler, DBSPExpression scalar) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var stream = new IndentStream(new StringBuilder()).setIndentAmount(0);
        var visitor = new ToJsonInnerVisitor(compiler, new JsonStream(stream), 1);
        scalar.accept(visitor);
        var result = mapper.createObjectNode()
                .put("expression_text", ToRustInnerVisitor.toRustString(compiler, scalar, null, true))
                .put("type_text", ToRustInnerVisitor.toRustString(compiler, scalar.getType(), null, true));
        result.set("expression", mapper.readTree(visitor.getJsonString()));
        return result;
    }
}
