package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;

/** One object arena for all local relational expressions and SQL function bodies.
 * Evaluation sources and input rows belong to uses, not shared objects. */
final class S1ExpressionObjectsEncoder {
    record Encoded(S1InputUsesTable.Encoded inputs, ObjectNode expressions, List<Integer> declarations,
                   java.util.Map<org.apache.calcite.sql.SqlOperator, Integer> operators) { }
    private record Entry(RexNode original, ObjectNode kind) { }
    private record Signature(int object, RexCall call) { }
    private record Environment(String input, Integer source, List<RexNode> roots) { }
    private record RelationUse(int relation, int environment, List<Integer> roots) { }
    private record ValueUse(int environment, List<Integer> roots) { }

    private final S1ExpressionSharingProbe.References captured;
    private final S1ExpressionSources sources;
    private final S1OperatorObjects objects;
    private final IdentityHashMap<RexNode, Integer> ids = new IdentityHashMap<>();
    private final IdentityHashMap<RexInputRef, Integer> inputs = new IdentityHashMap<>();
    private final List<Entry> entries = new ArrayList<>();
    private final List<Signature> signatures = new ArrayList<>();
    private final List<Environment> environments = new ArrayList<>();
    private final List<RelationUse> relations = new ArrayList<>();
    private final List<ValueUse> values = new ArrayList<>();
    private final List<Integer> declarations = new ArrayList<>();
    private final List<RelDataType> additionalTypes;
    private final IdentityHashMap<ExternalFunction, S1FunctionObjects.Body> bodies = new IdentityHashMap<>();

    private S1ExpressionObjectsEncoder(S1ExpressionSharingProbe.References captured,
                                      S1BuiltinOperatorRegistry registry, S1ExpressionSources sources,
                                      List<RelDataType> additionalTypes) throws IOException {
        this.additionalTypes = List.copyOf(additionalTypes);
        this.captured = captured; this.sources = sources;
        this.objects = new S1OperatorObjects(registry, this.sources);
        for (int id = 0; id < captured.uses.size(); id++) this.inputs.put(captured.uses.get(id).reference, id);
    }

    static Encoded encode(S1ExpressionSharingProbe.References captured, S1BuiltinOperatorRegistry registry,
                          S1ExpressionSources.Source source) throws IOException {
        return encode(captured, registry, source, List.of());
    }

    static Encoded encode(S1ExpressionSharingProbe.References captured, S1BuiltinOperatorRegistry registry,
                          S1ExpressionSources.Source source, List<RelDataType> additionalTypes) throws IOException {
        Integer context = source == null ? null : 0;
        return encode(captured, registry, S1ExpressionSources.of(source), additionalTypes,
                relation -> context, java.util.Collections.nCopies(captured.values.size(), context));
    }

    static Encoded encode(S1ExpressionSharingProbe.References captured, S1BuiltinOperatorRegistry registry,
                          S1ExpressionSources sources, List<RelDataType> additionalTypes,
                          java.util.function.Function<org.apache.calcite.rel.RelNode, Integer> relationSource,
                          List<Integer> valueSources) throws IOException {
        if (valueSources.size() != captured.values.size()) throw new IOException("Value source count differs");
        var encoder = new S1ExpressionObjectsEncoder(captured, registry, sources, additionalTypes);
        encoder.collect(relationSource, valueSources);
        var result = encoder.finish();
        var decoded = S1ExpressionObjectsTable.decode(result.expressions(), result.inputs().table(), registry);
        encoder.objects.verify(decoded.operators(), result.inputs().types().ids(), decoded.types(), encoder.bodies, decoded.expressions());
        var distinct = new IdentityHashMap<RexNode, Boolean>();
        for (int id = 0; id < encoder.entries.size(); id++) {
            RexNode original = encoder.entries.get(id).original(), restored = decoded.expressions().get(id);
            if (distinct.put(restored, true) != null || original.getClass() != restored.getClass()
                    || restored.getType() != decoded.types().get(result.inputs().types().id(original.getType())))
                throw new IOException("Expression class, sharing, or type identity changed");
        }
        return result;
    }

    private void collect(java.util.function.Function<org.apache.calcite.rel.RelNode, Integer> relationSource,
                         List<Integer> valueSources) throws IOException {
        Integer declarationSource = this.sources.hasSources() ? 0 : null;
        for (var function: this.captured.declarations)
            this.declarations.add(this.objects.add(function, declarationSource == null ? -1 : declarationSource));
        for (int id = 0; id < this.captured.relationNodes.size(); id++) {
            var relation = this.captured.relationNodes.get(id);
            Integer source = relationSource.apply(relation);
            if (relation instanceof org.apache.calcite.rel.core.Aggregate aggregate)
                for (var call: aggregate.getAggCallList())
                    this.objects.add(call.getAggregation(), source == null ? -1 : source);
            var local = this.captured.relationExpressions.get(relation);
            int environment = this.environments.size();
            this.environments.add(new Environment(this.captured.relationEnvironments.get(relation), source, local));
            var roots = new ArrayList<Integer>();
            for (RexNode root: local) roots.add(this.collect(root, source, 1));
            this.relations.add(new RelationUse(id, environment, roots));
        }
        for (int groupId = 0; groupId < this.captured.values.size(); groupId++) {
            var group = this.captured.values.get(groupId);
            Integer source = valueSources.get(groupId);
            int environment = this.environments.size();
            this.environments.add(new Environment("constant", source, group));
            var roots = new ArrayList<Integer>();
            for (RexNode root: group) roots.add(this.collect(root, source, 1));
            this.values.add(new ValueUse(environment, roots));
        }
        for (int id = 0; id < this.captured.functions.size(); id++) {
            var function = this.captured.functions.get(id);
            Integer bodySource = function.bodySource == null ? null : this.sources.add(
                    new S1ExpressionSources.Source("function " + function.getName() + " (generated)", function.bodySource));
            Integer contextSource = bodySource == null ? declarationSource : bodySource;
            RexNode body = this.captured.functionBodies.get(function);
            int environment = this.environments.size();
            this.environments.add(new Environment("function:" + id, contextSource, List.of(body)));
            this.bodies.put(function, new S1FunctionObjects.ObjectBody(environment, this.collect(body, contextSource, 1), bodySource));
        }
    }

    private int collect(RexNode rex, Integer source, int depth) throws IOException {
        if (depth > 256) throw new IOException("Expression graph exceeds depth limit");
        Integer previous = this.ids.get(rex);
        if (previous != null) return previous;
        if (this.entries.size() >= 100_000) throw new IOException("Expression graph exceeds object limit");
        int id = this.entries.size();
        var kind = Utilities.deterministicObjectMapper().createObjectNode();
        this.entries.add(new Entry(rex, kind)); this.ids.put(rex, id);
        if (rex.getClass() == RexInputRef.class) {
            Integer object = this.inputs.get((RexInputRef) rex);
            if (object == null) throw new IOException("Expression input was not collected in its evaluation environment");
            kind.put("kind", "input_ref").put("object", object);
        } else if (rex.getClass() == RexCorrelVariable.class) {
            kind.put("kind", "correlation").put("identity", ((RexCorrelVariable) rex).id.getId());
        } else if (rex.getClass() == RexFieldAccess.class) {
            var field = (RexFieldAccess) rex;
            kind.put("kind", "field_access").put("input", this.collect(field.getReferenceExpr(), source, depth + 1))
                    .put("field", field.getField().getIndex());
        } else if (rex.getClass() == RexLiteral.class
                && ((RexLiteral) rex).getTypeName() == org.apache.calcite.sql.type.SqlTypeName.SARG) {
            kind.setAll(S1SearchArguments.encode((RexLiteral) rex));
        } else if (rex.getClass() == RexLiteral.class) {
            kind.put("kind", "literal");
            var literal = kind.putObject("literal");
            literal.putObject("origin").put("kind", "absent");
            literal.set("value", S1LiteralPayload.encode((RexLiteral) rex));
        } else if (rex.getClass() == RexCall.class || rex.getClass() == org.apache.calcite.rel.core.Window.RexWinAggCall.class) {
            var call = (RexCall) rex;
            int signature = this.signatures.size();
            this.signatures.add(new Signature(this.objects.add(call.getOperator(), source == null ? -1 : source), call));
            kind.put("kind", "call").put("operator", signature);
            if (call instanceof org.apache.calcite.rel.core.Window.RexWinAggCall window)
                kind.put("kind", "window_aggregate").put("ordinal", window.ordinal)
                        .put("distinct", window.distinct).put("ignore_nulls", window.ignoreNulls);
            var arguments = kind.putArray("arguments");
            for (RexNode argument: call.getOperands()) arguments.add(this.collect(argument, source, depth + 1));
        } else if (rex.getClass() == RexLambdaRef.class) {
            var reference = (RexLambdaRef) rex;
            kind.put("kind", "lambda_ref").put("index", reference.getIndex()).put("name", reference.getName());
        } else if (rex.getClass() == RexLambda.class) {
            var lambda = (RexLambda) rex;
            kind.put("kind", "lambda");
            var parameters = kind.putArray("parameters");
            for (RexLambdaRef parameter: lambda.getParameters()) parameters.add(this.collect(parameter, source, depth + 1));
            kind.put("body", this.collect(lambda.getExpression(), source, depth + 1));
        } else throw new IOException("Unsupported resolved expression class: " + rex.getClass().getName());
        return id;
    }

    private Encoded finish() throws IOException {
        var types = new ArrayList<RelDataType>();
        for (Entry entry: this.entries) types.add(entry.original().getType());
        types.addAll(this.objects.declaredTypes());
        types.addAll(this.additionalTypes);
        var inputs = S1InputUsesTable.encode(this.captured, types);
        this.sources.encode(inputs.types().table().withArray("sources"));
        var wire = Utilities.deterministicObjectMapper().createObjectNode().put("schema_version", "expression-objects-v2");
        wire.set("operator_objects", this.objects.encode(inputs.types().ids(), this.bodies));
        var signatures = wire.putArray("operators");
        for (Signature signature: this.signatures) {
            var entry = signatures.addObject().put("object", signature.object()).put("result", inputs.types().id(signature.call().getType()));
            var parameters = entry.putArray("parameters");
            for (RexNode operand: signature.call().getOperands()) parameters.add(inputs.types().id(operand.getType()));
        }
        var expressions = wire.putArray("expressions");
        for (Entry entry: this.entries) {
            int type = inputs.types().id(entry.original().getType());
            var expression = expressions.addObject().put("ty", type);
            var position = expression.putObject("position");
            if (entry.original() instanceof RexCall call && (!call.getParserPosition().equals(SqlParserPos.ZERO)
                    || call.getParserPosition() instanceof org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos)) {
                position.put("kind", "present").set("position", S1RelationMetadata.position(call.getParserPosition()));
                if (call.getParserPosition() instanceof org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos extended)
                    position.put("internal", extended.internal);
                else position.putNull("internal");
            } else position.put("kind", "absent");
            expression.set("kind", entry.kind());
            if (entry.kind().path("kind").asText().equals("search_argument")) {
                for (var range: entry.kind().path("ranges"))
                    for (String side: List.of("lower", "upper"))
                        if (range.path(side).has("literal")) ((ObjectNode) range.path(side).path("literal")).put("ty", type);
            } else if (entry.original() instanceof RexLiteral) ((ObjectNode) entry.kind().path("literal")).put("ty", type);
        }
        var inputIds = new LinkedHashMap<String, Integer>();
        this.captured.environments.keySet().forEach(name -> inputIds.put(name, inputIds.size()));
        var environments = wire.putArray("environments");
        for (Environment environment: this.environments) {
            var entry = environments.addObject().put("input", inputIds.get(environment.input()));
            var source = entry.putObject("source");
            if (environment.source() == null) source.put("kind", "absent");
            else source.put("kind", "present").put("source", environment.source());
            var correlations = new LinkedHashMap<Integer, RelDataType>();
            var visited = new IdentityHashMap<RexNode, Boolean>();
            for (RexNode root: environment.roots()) this.correlations(root, correlations, visited);
            var bindings = entry.putArray("correlations");
            for (var correlation: correlations.entrySet())
                bindings.addObject().put("identity", correlation.getKey()).put("row", inputs.types().id(correlation.getValue()));
        }
        var uses = wire.putArray("relation_uses");
        for (RelationUse relation: this.relations) {
            var use = uses.addObject().put("relation", relation.relation()).put("environment", relation.environment());
            var roots = use.putArray("roots"); relation.roots().forEach(roots::add);
        }
        var values = wire.putArray("value_uses");
        for (ValueUse value: this.values) {
            var use = values.addObject().put("environment", value.environment());
            var roots = use.putArray("roots"); value.roots().forEach(roots::add);
        }
        return new Encoded(inputs, wire, List.copyOf(this.declarations), this.objects.identities());
    }

    private void correlations(RexNode node, LinkedHashMap<Integer, RelDataType> bindings,
                              IdentityHashMap<RexNode, Boolean> visited) throws IOException {
        if (visited.put(node, true) != null) return;
        if (node instanceof RexCorrelVariable correlation) {
            RelDataType previous = bindings.putIfAbsent(correlation.id.getId(), correlation.getType());
            if (previous != null && previous != correlation.getType()) throw new IOException("Correlation type identity differs between uses");
        } else if (node instanceof RexCall call) {
            for (RexNode operand: call.getOperands()) this.correlations(operand, bindings, visited);
        } else if (node instanceof RexFieldAccess field) this.correlations(field.getReferenceExpr(), bindings, visited);
        else if (node instanceof RexLambda lambda) this.correlations(lambda.getExpression(), bindings, visited);
    }
}
