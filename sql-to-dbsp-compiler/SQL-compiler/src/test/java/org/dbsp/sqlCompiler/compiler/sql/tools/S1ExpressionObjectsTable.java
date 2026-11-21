package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.FelderaS1ExpressionFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Reconstruct one Java object per expression ID, in dependency order.
 * The Rust reader validates relational ownership and contextual evaluation;
 * this reader independently checks the intrinsic construction contract. */
final class S1ExpressionObjectsTable {
    record Decoded(List<RelDataType> types, List<RexNode> expressions, List<SqlOperator> operators) { }
    private final JsonNode wire;
    private final JsonNode inputs;
    private final List<RelDataType> types;
    private final S1ExpressionSources sources;
    private final List<SqlOperator> operators;
    private final List<List<Integer>> graph = new ArrayList<>();
    private long references;
    private long identifierBytes;

    private S1ExpressionObjectsTable(JsonNode wire, JsonNode inputs, S1BuiltinOperatorRegistry registry) throws IOException {
        fields(wire, "schema_version", "operator_objects", "operators", "expressions", "environments", "relation_uses", "value_uses");
        check(wire.path("schema_version").asText().equals("expression-objects-v2"), "Unknown expression object version");
        this.wire = wire; this.inputs = inputs;
        long entries = 0;
        for (String name: List.of("operator_objects", "operators", "expressions", "environments", "relation_uses", "value_uses")) {
            array(wire.path(name)); entries += wire.path(name).size();
        }
        check(entries <= 100_000, "Expression object arenas exceed entry limit");
        S1InputUsesTable.decode(inputs);
        this.types = S1TypeTable.decodeWithSources(inputs.path("type_table")).types();
        this.sources = S1ExpressionSources.decode(inputs.at("/type_table/sources"));
        var operators = S1OperatorObjects.decode(wire.path("operator_objects"), this.types, this.sources, registry);
        this.operators = new ArrayList<>(operators.objects());
        this.references = operators.references(); this.identifierBytes = operators.identifierBytes();
        this.readSignatures(); this.readExpressions(); this.readFunctions(); this.readUses();
        check(this.identifierBytes <= 1024 * 1024, "Expression identifiers exceed byte limit");
    }

    static Decoded decode(JsonNode wire, JsonNode inputs, S1BuiltinOperatorRegistry registry) throws IOException {
        return new S1ExpressionObjectsTable(wire, inputs, registry).reconstruct();
    }

    private void readSignatures() throws IOException {
        for (var signature: this.wire.path("operators")) {
            fields(signature, "object", "parameters", "result");
            this.reference(signature.path("object"), this.operators.size()); this.type(signature.path("result"));
            array(signature.path("parameters"));
            for (var parameter: signature.path("parameters")) this.type(parameter);
        }
    }

    private void readExpressions() throws IOException {
        for (var expression: this.wire.path("expressions")) {
            fields(expression, "ty", "position", "kind"); this.type(expression.path("ty"));
            JsonNode kind = expression.path("kind"); String tag = text(kind.path("kind"));
            position(expression.path("position"));
            if (!Set.of("call", "window_aggregate").contains(tag))
                check(expression.at("/position/kind").asText().equals("absent"), "Non-call expression has parser position");
            var children = new ArrayList<Integer>();
            switch (tag) {
                case "input_ref" -> {
                    fields(kind, "kind", "object"); int object = this.reference(kind.path("object"), this.inputs.path("objects").size());
                    check(this.inputs.path("objects").get(object).path("ty").equals(expression.path("ty")), "Input object type differs");
                }
                case "correlation" -> {
                    fields(kind, "kind", "identity"); unsigned(kind.path("identity"));
                    check(this.types.get(unsigned(expression.path("ty"))).isStruct(), "Correlation requires row type");
                }
                case "search_argument" -> {
                    S1SearchArguments.decode(kind, this.types.get(unsigned(expression.path("ty"))), unsigned(expression.path("ty")));
                }
                case "literal" -> {
                    fields(kind, "kind", "literal"); fields(kind.path("literal"), "ty", "origin", "value");
                    check(kind.at("/literal/ty").equals(expression.path("ty")), "Literal type differs");
                    fields(kind.at("/literal/origin"), "kind");
                    check(kind.at("/literal/origin/kind").asText().equals("absent"), "Literal origin must be absent");
                }
                case "field_access" -> {
                    fields(kind, "kind", "input", "field"); children.add(this.expression(kind.path("input"))); unsigned(kind.path("field"));
                }
                case "call", "window_aggregate" -> {
                    if (tag.equals("call")) fields(kind, "kind", "operator", "arguments");
                    else {
                        fields(kind, "kind", "operator", "arguments", "ordinal", "distinct", "ignore_nulls");
                        unsigned(kind.path("ordinal")); bool(kind.path("distinct")); bool(kind.path("ignore_nulls"));
                    }
                    int signature = this.reference(kind.path("operator"), this.wire.path("operators").size());
                    children.add(this.wire.path("expressions").size() + unsigned(this.wire.path("operators").get(signature).path("object")));
                    array(kind.path("arguments"));
                    for (var argument: kind.path("arguments")) children.add(this.expression(argument));
                }
                case "lambda_ref" -> {
                    fields(kind, "kind", "index", "name"); unsigned(kind.path("index"));
                    String name = text(kind.path("name")); check(!name.isEmpty(), "Empty lambda reference name");
                    this.identifierBytes += name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                }
                case "lambda" -> {
                    fields(kind, "kind", "parameters", "body"); array(kind.path("parameters"));
                    var parameters = new HashSet<Integer>(); var declarations = new HashSet<List<Object>>();
                    for (var parameter: kind.path("parameters")) {
                        int id = this.expression(parameter); children.add(id);
                        var declaration = this.wire.path("expressions").get(id).path("kind");
                        check(declaration.path("kind").asText().equals("lambda_ref") && parameters.add(id)
                                && declarations.add(List.of(text(declaration.path("name")), unsigned(declaration.path("index")))),
                                "Invalid or ambiguous lambda declaration");
                    }
                    children.add(this.expression(kind.path("body")));
                }
                default -> throw new IOException("Unknown expression object kind: " + tag);
            }
            this.graph.add(children);
        }
    }

    private void readFunctions() throws IOException {
        for (var operator: this.wire.path("operator_objects")) {
            var children = new ArrayList<Integer>();
            if (operator.path("kind").asText().equals("function")) {
                var body = operator.path("body");
                if (!body.path("kind").asText().equals("external")) {
                    check(body.path("kind").asText().equals("object_sql"), "Scoped function body in expression object arena");
                    children.add(this.expression(body.path("root")));
                    this.reference(body.path("environment"), this.wire.path("environments").size());
                }
            }
            this.graph.add(children);
        }
    }

    private void readUses() throws IOException {
        for (var environment: this.wire.path("environments")) {
            fields(environment, "input", "source", "correlations");
            this.reference(environment.path("input"), this.inputs.path("environments").size());
            var source = environment.path("source");
            if (source.path("kind").asText().equals("absent")) fields(source, "kind");
            else {
                fields(source, "kind", "source"); check(source.path("kind").asText().equals("present"), "Invalid source context");
                this.reference(source.path("source"), this.inputs.at("/type_table/sources").size());
            }
            array(environment.path("correlations")); var seen = new HashSet<Integer>();
            for (var correlation: environment.path("correlations")) {
                fields(correlation, "identity", "row");
                check(seen.add(unsigned(correlation.path("identity"))), "Duplicate correlation declaration");
                check(this.types.get(this.type(correlation.path("row"))).isStruct(), "Correlation requires row type");
            }
        }
        for (var use: this.wire.path("relation_uses")) {
            fields(use, "relation", "environment", "roots");
            this.reference(use.path("relation"), unsigned(this.inputs.path("relations")));
            this.reference(use.path("environment"), this.wire.path("environments").size()); array(use.path("roots"));
            for (var root: use.path("roots")) this.expression(root);
        }
        for (var use: this.wire.path("value_uses")) {
            fields(use, "environment", "roots");
            int environment = this.reference(use.path("environment"), this.wire.path("environments").size());
            var context = this.wire.path("environments").get(environment);
            var input = this.inputs.path("environments").get(unsigned(context.path("input")));
            check(input.at("/owner/kind").asText().equals("constant") && context.path("correlations").isEmpty(),
                    "Statement value requires an empty constant environment");
            array(use.path("roots"));
            for (var root: use.path("roots")) this.expression(root);
        }
    }

    private Decoded reconstruct() throws IOException {
        int count = this.wire.path("expressions").size();
        var nodes = new ArrayList<RexNode>(Collections.nCopies(count, null));
        var builder = new RexBuilder(new S1TypeTable.Factory());
        for (int id: S1ArenaGraph.order(this.graph)) {
            if (id >= count) {
                int operator = id - count;
                var definition = this.wire.path("operator_objects").get(operator);
                if (definition.path("kind").asText().equals("function"))
                    this.operators.set(operator, S1FunctionObjects.create(definition, this.types, this.sources, nodes));
                continue;
            }
            var expression = this.wire.path("expressions").get(id); var kind = expression.path("kind");
            int typeId = unsigned(expression.path("ty")); var type = this.types.get(typeId);
            RexNode node;
            switch (kind.path("kind").asText()) {
                case "input_ref" -> node = new RexInputRef(unsigned(this.inputs.path("objects").get(unsigned(kind.path("object"))).path("field")), type);
                case "correlation" -> node = builder.makeCorrel(type, new CorrelationId(unsigned(kind.path("identity"))));
                case "lambda_ref" -> node = new RexLambdaRef(unsigned(kind.path("index")), text(kind.path("name")), type);
                case "literal" -> node = S1LiteralPayload.decode(kind.at("/literal/value"), type);
                case "search_argument" -> node = S1SearchArguments.decode(kind, type, typeId);
                case "field_access" -> {
                    int input = unsigned(kind.path("input")), field = unsigned(kind.path("field"));
                    var inputType = nodes.get(input).getType();
                    check(inputType.isStruct() && field < inputType.getFieldCount(), "Field access outside input row");
                    var declared = this.inputs.at("/type_table/types").get(unsigned(this.wire.path("expressions").get(input).path("ty")))
                            .at("/kind/fields").get(field).path("ty");
                    check(this.referenceTypeMatches(unsigned(declared), typeId), "Field access result type differs");
                    node = FelderaS1ExpressionFactory.field(nodes.get(input), field, type);
                }
                case "call", "window_aggregate" -> {
                    var signature = this.wire.path("operators").get(unsigned(kind.path("operator")));
                    check(signature.path("result").equals(expression.path("ty"))
                            && signature.path("parameters").size() == kind.path("arguments").size(), "Call signature differs");
                    var arguments = new ArrayList<RexNode>();
                    for (int index = 0; index < kind.path("arguments").size(); index++) {
                        int child = unsigned(kind.path("arguments").get(index));
                        check(signature.path("parameters").get(index).equals(this.wire.path("expressions").get(child).path("ty")),
                                "Call parameter type differs"); arguments.add(nodes.get(child));
                    }
                    var operator = this.operators.get(unsigned(signature.path("object")));
                    boolean search = operator == org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH;
                    if (search) check(arguments.size() == 2 && arguments.get(1) instanceof RexLiteral literal
                            && literal.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.SARG,
                            "SEARCH requires a typed range argument");
                    for (int argument = 0; argument < arguments.size(); argument++)
                        if (arguments.get(argument) instanceof RexLiteral literal && literal.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.SARG)
                            check(search && argument == 1, "Search argument used outside SEARCH");
                    var position = position(expression.path("position"));
                    if (kind.path("kind").asText().equals("window_aggregate")) {
                        check(operator instanceof SqlAggFunction, "Window aggregate requires aggregate operator");
                        node = new org.apache.calcite.rel.core.Window.RexWinAggCall(position, (SqlAggFunction) operator, type,
                                arguments, unsigned(kind.path("ordinal")), kind.path("distinct").booleanValue(), kind.path("ignore_nulls").booleanValue());
                    } else node = FelderaS1ExpressionFactory.call(position, type, operator, arguments);
                }
                case "lambda" -> {
                    var parameters = new ArrayList<RexLambdaRef>();
                    for (var parameter: kind.path("parameters")) parameters.add((RexLambdaRef) nodes.get(unsigned(parameter)));
                    node = FelderaS1ExpressionFactory.lambda(parameters, nodes.get(unsigned(kind.path("body"))));
                }
                default -> throw new IOException("Unknown expression construction");
            }
            check(node.getType() == type, "Expression type arena identity changed"); nodes.set(id, node);
        }
        return new Decoded(List.copyOf(this.types), List.copyOf(nodes), List.copyOf(this.operators));
    }

    private boolean referenceTypeMatches(int declared, int actual) {
        if (declared == actual) return true;
        var source = this.inputs.at("/type_table/types").get(declared); var target = this.inputs.at("/type_table/types").get(actual);
        return !source.at("/kind/kind").asText().equals("named") && source.path("kind").equals(target.path("kind"))
                && (!source.path("nullable").booleanValue() || target.path("nullable").booleanValue());
    }

    private static SqlParserPos position(JsonNode wire) throws IOException {
        if (wire.path("kind").asText().equals("absent")) { fields(wire, "kind"); return SqlParserPos.ZERO; }
        fields(wire, "kind", "position", "internal"); check(wire.path("kind").asText().equals("present"), "Unknown expression position");
        var position = S1RelationMetadata.position(wire.path("position"));
        if (wire.path("internal").isNull()) return position;
        check(wire.path("internal").isBoolean() && !position.isQuoted(), "Invalid extended expression position");
        return new org.dbsp.sqlCompiler.compiler.frontend.ExtendedSqlParserPos(position, wire.path("internal").booleanValue());
    }
    private int expression(JsonNode id) throws IOException { return this.reference(id, this.wire.path("expressions").size()); }
    private int type(JsonNode id) throws IOException { return this.reference(id, this.types.size()); }
    private int reference(JsonNode id, int size) throws IOException {
        int value = unsigned(id); check(value < size, "Expression reference outside arena");
        check(++this.references <= 1_000_000, "Expression references exceed limit"); return value;
    }
    private static int unsigned(JsonNode value) throws IOException {
        check(value.isIntegralNumber() && value.canConvertToInt() && value.intValue() >= 0, "Invalid unsigned expression index"); return value.intValue();
    }
    private static String text(JsonNode value) throws IOException { check(value.isTextual(), "Expected expression text"); return value.textValue(); }
    private static void bool(JsonNode value) throws IOException { check(value.isBoolean(), "Expected expression boolean"); }
    private static void array(JsonNode value) throws IOException { check(value.isArray() && value.size() <= 1_000_000, "Invalid or oversized expression array"); }
    private static void fields(JsonNode value, String... names) throws IOException {
        check(value.isObject(), "Expected expression object"); var actual = new HashSet<String>(); value.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing expression fields");
    }
    private static void check(boolean condition, String message) throws IOException { if (!condition) throw new IOException(message); }
}
