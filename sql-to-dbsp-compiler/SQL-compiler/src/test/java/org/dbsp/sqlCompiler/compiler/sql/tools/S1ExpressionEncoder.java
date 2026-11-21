package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.dbsp.util.Utilities;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Contextual exporter for the expression fragment. Unsupported subclasses,
 * calls without their source records, and unregistered operators fail explicitly. This does
 * not infer catalog context or rewrite expressions into supported forms. */
final class S1ExpressionEncoder {
    private record Binding(String kind, RelDataType type, RexLambdaRef parameter) { }
    private record Scope(Integer parent, List<Integer> bindings, int source) { }
    private record Entry(RexNode expression, int scope, ObjectNode kind) { }
    private record Operator(int object, RexCall call) { }
    private record Wire(ObjectNode table, IdentityHashMap<RelDataType, Integer> typeIds) { }

    private final S1OperatorObjects operatorObjects;
    private final S1ExpressionSources sources;
    private final IdentityHashMap<RexNode, Integer> ids = new IdentityHashMap<>();
    private final IdentityHashMap<RexNode, Boolean> active = new IdentityHashMap<>();
    private final Map<CorrelationId, Integer> correlations = new HashMap<>();
    private final List<Binding> bindings = new ArrayList<>();
    private final List<Scope> scopes = new ArrayList<>();
    private final List<Entry> expressions = new ArrayList<>();
    private final IdentityHashMap<ExternalFunction, S1FunctionObjects.Body> functionBodies = new IdentityHashMap<>();
    private final List<Operator> operators = new ArrayList<>();

    private S1ExpressionEncoder(RelDataType input, S1BuiltinOperatorRegistry registry,
                                S1ExpressionSources sources) {
        this.operatorObjects = new S1OperatorObjects(registry, sources);
        this.sources = sources;
        this.bindings.add(new Binding("input", input, null));
        this.scopes.add(new Scope(null, new ArrayList<>(List.of(0)), 0));
    }

    static ObjectNode encode(List<RexNode> roots, RelDataType input,
                             S1BuiltinOperatorRegistry registry) throws IOException {
        return encode(roots, input, registry, null);
    }

    static ObjectNode encode(List<RexNode> roots, RelDataType input,
                             S1BuiltinOperatorRegistry registry,
                             S1ExpressionSources.Source source) throws IOException {
        var encoder = new S1ExpressionEncoder(input, registry, S1ExpressionSources.of(source));
        var rootIds = new ArrayList<Integer>();
        for (RexNode root: roots) rootIds.add(encoder.collect(root, 0, 1));
        encoder.collectFunctionBodies();
        Wire wire = encoder.finish(rootIds);
        var decoded = S1ExpressionTable.decode(wire.table(), registry);
        encoder.verify(decoded, wire.typeIds());
        return wire.table();
    }

    private int collect(RexNode rex, int scope, int depth) throws IOException {
        if (depth > 256 || this.active.containsKey(rex))
            throw new IOException("Cyclic or overdeep expression graph");
        Integer previous = this.ids.get(rex);
        if (previous != null) return previous;
        if (this.expressions.size() >= 100_000)
            throw new IOException("Expression count exceeds limit");
        ObjectNode kind = Utilities.deterministicObjectMapper().createObjectNode();
        int id = this.expressions.size();
        this.expressions.add(new Entry(rex, scope, kind)); this.ids.put(rex, id);
        this.active.put(rex, true);
        // Exact classes matter: RexOver and RexSubQuery require their own
        // adapters. Extracted window aggregate calls have an explicit variant.
        if (rex.getClass() == RexInputRef.class) {
            kind.put("kind", "input_ref").put("binding", this.input(scope)).put("field", ((RexInputRef) rex).getIndex());
        } else if (rex.getClass() == RexCorrelVariable.class) {
            RexCorrelVariable correlation = (RexCorrelVariable) rex;
            Integer binding = this.correlations.get(correlation.id);
            if (binding == null) {
                binding = this.bindings.size();
                this.bindings.add(new Binding("correlation", rex.getType(), null));
                this.scopes.get(0).bindings().add(binding);
                this.correlations.put(correlation.id, binding);
            } else if (this.bindings.get(binding).type() != rex.getType()) {
                throw new IOException("Correlation references disagree on type identity");
            }
            kind.put("kind", "correlation").put("binding", binding);
        } else if (rex.getClass() == RexFieldAccess.class) {
            var field = (RexFieldAccess) rex;
            kind.put("kind", "field_access").put("input", this.collect(field.getReferenceExpr(), scope, depth + 1))
                    .put("field", field.getField().getIndex());
        } else if (rex.getClass() == RexLiteral.class) {
            kind.put("kind", "literal");
            ObjectNode literal = kind.putObject("literal");
            literal.putObject("origin").put("kind", "absent");
            literal.set("value", S1LiteralPayload.encode((RexLiteral) rex));
        } else if (rex.getClass() == RexCall.class || rex.getClass() == org.apache.calcite.rel.core.Window.RexWinAggCall.class) {
            RexCall call = (RexCall) rex;
            this.sources.origin(call.getParserPosition(), this.scopes.get(scope).source());
            int operator = this.operators.size();
            this.operators.add(new Operator(this.operatorObjects.add(call.getOperator(), this.scopes.get(scope).source()), call));
            kind.put("kind", "call").put("operator", operator);
            if (call instanceof org.apache.calcite.rel.core.Window.RexWinAggCall window)
                kind.put("kind", "window_aggregate").put("ordinal", window.ordinal)
                        .put("distinct", window.distinct).put("ignore_nulls", window.ignoreNulls);
            var arguments = kind.putArray("arguments");
            for (RexNode argument: call.getOperands()) arguments.add(this.collect(argument, scope, depth + 1));
        } else if (rex.getClass() == RexLambdaRef.class) {
            kind.put("kind", "lambda_ref").put("binding", this.parameter((RexLambdaRef) rex, scope));
        } else if (rex.getClass() == RexLambda.class) {
            RexLambda lambda = (RexLambda) rex;
            int bodyScope = this.scopes.size();
            var bodyBindings = new ArrayList<Integer>();
            this.scopes.add(new Scope(scope, bodyBindings, this.scopes.get(scope).source()));
            kind.put("kind", "lambda").put("body_scope", bodyScope);
            // Declare all parameters before visiting either declarations or body.
            for (RexLambdaRef parameter: lambda.getParameters()) {
                bodyBindings.add(this.bindings.size());
                this.bindings.add(new Binding("lambda_parameter", parameter.getType(), parameter));
            }
            var parameters = kind.putArray("parameters");
            for (int ordinal = 0; ordinal < lambda.getParameters().size(); ordinal++) {
                RexLambdaRef parameter = lambda.getParameters().get(ordinal);
                parameters.addObject().put("declaration", this.collect(parameter, bodyScope, depth + 1))
                        .put("binding", bodyBindings.get(ordinal)).put("name", parameter.getName()).put("index", parameter.getIndex());
            }
            kind.put("body", this.collect(lambda.getExpression(), bodyScope, depth + 1));
        } else {
            throw new IOException("Unsupported resolved expression class: " + rex.getClass().getName());
        }
        this.active.remove(rex);
        return id;
    }

    private int input(int scope) throws IOException {
        Integer cursor = scope;
        while (cursor != null) {
            for (int binding: this.scopes.get(cursor).bindings())
                if (this.bindings.get(binding).kind().equals("input")) return binding;
            cursor = this.scopes.get(cursor).parent();
        }
        throw new IOException("Expression has no input context");
    }

    private void collectFunctionBodies() throws IOException {
        for (int index = 0; index < this.operatorObjects.size(); index++) {
            ExternalFunction function = this.operatorObjects.function(index);
            if (function == null || function.body == null) continue;
            int scope = this.scopes.size(), input = this.bindings.size();
            var row = new org.apache.calcite.rel.type.RelRecordType(org.apache.calcite.rel.type.StructKind.FULLY_QUALIFIED,
                    function.parameterList, false);
            this.bindings.add(new Binding("input", row, null));
            Integer source = function.bodySource == null ? null : this.sources.add(
                    new S1ExpressionSources.Source("function " + function.getName() + " (generated)", function.bodySource));
            this.scopes.add(new Scope(null, new ArrayList<>(List.of(input)), source == null ? 0 : source));
            int root = this.collect(function.body, scope, 1);
            this.functionBodies.put(function, new S1FunctionObjects.ScopedBody(scope, input, root, source));
        }
    }

    private int parameter(RexLambdaRef reference, int scope) throws IOException {
        Integer cursor = scope;
        while (cursor != null) {
            Integer match = null;
            for (int binding: this.scopes.get(cursor).bindings()) {
                RexLambdaRef declaration = this.bindings.get(binding).parameter();
                if (declaration != null && declaration.getIndex() == reference.getIndex()
                        && declaration.getName().equals(reference.getName())) {
                    if (match != null) throw new IOException("Ambiguous lambda parameter identity");
                    if (declaration.getType() != reference.getType())
                        throw new IOException("Lambda reference differs from declaration type identity");
                    match = binding;
                }
            }
            if (match != null) return match;
            cursor = this.scopes.get(cursor).parent();
        }
        throw new IOException("Lambda reference has no lexical declaration");
    }

    private Wire finish(List<Integer> roots) throws IOException {
        var typeRoots = new ArrayList<RelDataType>();
        var typeOrdinals = new IdentityHashMap<RelDataType, Integer>();
        for (Binding binding: this.bindings) addType(binding.type(), typeRoots, typeOrdinals);
        for (Entry entry: this.expressions) addType(entry.expression().getType(), typeRoots, typeOrdinals);
        for (RelDataType declared: this.operatorObjects.declaredTypes()) addType(declared, typeRoots, typeOrdinals);
        ObjectNode typeTable = S1TypeTable.encode(typeRoots);
        this.sources.encode(typeTable.withArray("sources"));
        var typeIds = new IdentityHashMap<RelDataType, Integer>();
        for (int index = 0; index < typeRoots.size(); index++)
            typeIds.put(typeRoots.get(index), typeTable.path("roots").get(index).intValue());
        ObjectNode table = Utilities.deterministicObjectMapper().createObjectNode();
        table.put("schema_version", "expressions-v4"); table.set("type_table", typeTable);
        table.set("operator_objects", this.operatorObjects.encode(typeIds, this.functionBodies));
        var operators = table.putArray("operators");
        for (Operator operator: this.operators) {
            var entry = operators.addObject();
            entry.put("object", operator.object()).put("result", typeIds.get(operator.call().getType()));
            var parameters = entry.putArray("parameters");
            for (RexNode argument: operator.call().getOperands()) parameters.add(typeIds.get(argument.getType()));
        }
        var bindings = table.putArray("bindings");
        for (Binding binding: this.bindings) bindings.addObject().put("kind", binding.kind()).put("ty", typeIds.get(binding.type()));
        var scopes = table.putArray("scopes");
        for (Scope scope: this.scopes) {
            var entry = scopes.addObject();
            if (scope.parent() == null) entry.putNull("parent"); else entry.put("parent", scope.parent());
            var declared = entry.putArray("bindings"); scope.bindings().forEach(declared::add);
        }
        var expressions = table.putArray("expressions");
        for (Entry expression: this.expressions) {
            var entry = expressions.addObject(); int type = typeIds.get(expression.expression().getType());
            entry.put("ty", type).put("scope", expression.scope());
            if (expression.expression() instanceof RexCall call)
                entry.set("origin", this.sources.origin(call.getParserPosition(), this.scopes.get(expression.scope()).source()));
            else entry.putObject("origin").put("kind", "absent");
            entry.set("kind", expression.kind());
            if (expression.expression() instanceof RexLiteral)
                ((ObjectNode) expression.kind().path("literal")).put("ty", type);
        }
        var rootIds = table.putArray("roots"); roots.forEach(rootIds::add);
        return new Wire(table, typeIds);
    }

    private static void addType(RelDataType type, List<RelDataType> roots, IdentityHashMap<RelDataType, Integer> ordinals) {
        if (!ordinals.containsKey(type)) { ordinals.put(type, roots.size()); roots.add(type); }
    }

    private void verify(S1ExpressionTable.Decoded decoded, IdentityHashMap<RelDataType, Integer> typeIds) throws IOException {
        this.operatorObjects.verify(decoded.operatorObjects(), typeIds, decoded.types(), this.functionBodies, decoded.expressions());
        var seen = new IdentityHashMap<RexNode, Boolean>();
        for (int id = 0; id < this.expressions.size(); id++) {
            RexNode original = this.expressions.get(id).expression();
            RexNode imported = decoded.expressions().get(id);
            if (seen.put(imported, true) != null)
                throw new IOException("Distinct expression objects were interned");
            if (original.getClass() != imported.getClass() || !original.getType().equals(imported.getType()))
                throw new IOException("Expression class or type changed");
            if (original instanceof RexCall) {
                int signature = this.expressions.get(id).kind().path("operator").intValue();
                if (((RexCall) imported).getOperator() != decoded.operatorObjects().get(this.operators.get(signature).object()))
                    throw new IOException("Call operator object sharing changed");
            }
            if (original instanceof org.apache.calcite.rel.core.Window.RexWinAggCall window) {
                var copy = (org.apache.calcite.rel.core.Window.RexWinAggCall) imported;
                if (window.ordinal != copy.ordinal || window.distinct != copy.distinct || window.ignoreNulls != copy.ignoreNulls)
                    throw new IOException("Window aggregate metadata changed");
            }
            if (original instanceof RexCall call && !call.getParserPosition().equals(((RexCall) imported).getParserPosition()))
                throw new IOException("Call parser position changed");
            if (original instanceof RexLiteral literal
                    && !S1LiteralPayload.encode(literal).equals(S1LiteralPayload.encode((RexLiteral) imported)))
                throw new IOException("Literal storage changed");
        }
    }
}
