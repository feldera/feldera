package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.FelderaS1ExpressionFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Test-only reader for the resolved expression fragment. It accepts arbitrary
 * arena ordering and validates every entry, including unreachable entries.
 * Source-bearing calls use explicit source records. Statement/catalog context
 * is not supported; this is not the whole-program S1 importer. */
final class S1ExpressionTable {
    private static final int MAX_ENTRIES = 100_000;
    private static final int MAX_REFERENCES = 1_000_000;
    private static final int MAX_IDENTIFIER_BYTES = 1024 * 1024;

    record Decoded(List<RelDataType> types, List<RexNode> expressions, List<RexNode> roots, List<SqlOperator> operatorObjects) {
        Decoded {
            types = List.copyOf(types);
            expressions = List.copyOf(expressions);
            roots = List.copyOf(roots);
            operatorObjects = List.copyOf(operatorObjects);
        }
    }

    private final JsonNode table;
    private final List<RelDataType> types;
    private final S1ExpressionSources sources;
    private final List<SqlOperator> operatorObjects;
    private final List<List<Integer>> edges = new ArrayList<>();
    private final int[] owners;
    private final JsonNode[] parameters;
    private long references;
    private long identifierBytes;

    private S1ExpressionTable(JsonNode table, S1BuiltinOperatorRegistry registry) throws IOException {
        fields(table, "schema_version", "type_table", "operator_objects", "operators", "bindings", "scopes", "expressions", "roots");
        check(text(table, "schema_version").equals("expressions-v4"), "Unsupported expression schema version");
        long entries = 0;
        for (String name: List.of("operator_objects", "operators", "bindings", "scopes", "expressions")) {
            array(table.path(name));
            entries += table.path(name).size();
        }
        check(entries <= MAX_ENTRIES, "Expression arena exceeds entry limit");
        array(table.path("roots"));
        this.table = table;
        this.sources = S1ExpressionSources.decode(table.path("type_table").path("sources"));
        // The type fragment still requires absent type/field origins. Validate
        // sources here and give that reader its supported source-free projection.
        ObjectNode typeTable = table.path("type_table").deepCopy();
        typeTable.putArray("sources");
        this.types = S1TypeTable.decode(typeTable).types();
        this.owners = new int[table.path("bindings").size()];
        Arrays.fill(this.owners, -1);
        this.parameters = new JsonNode[this.owners.length];
        var objects = S1OperatorObjects.decode(table.path("operator_objects"), this.types, this.sources, registry);
        for (var operator: objects.objects())
            check(operator == null || operator.getKind() != org.apache.calcite.sql.SqlKind.SEARCH,
                    "SEARCH must be expanded in the scoped fragment contract");
        for (var object: table.path("operator_objects"))
            check(!object.path("body").path("kind").asText().equals("object_sql"),
                    "Whole-plan function environment in expression fragment");
        this.operatorObjects = new ArrayList<>(objects.objects());
        this.references = objects.references(); this.identifierBytes = objects.identifierBytes();
        this.readOperators();
        this.readScopes();
        this.readExpressions();
    }

    static Decoded read(Path input, S1BuiltinOperatorRegistry registry) throws IOException {
        // Read at most the bound plus one, rather than trusting a prior stat.
        byte[] bytes;
        try (var stream = Files.newInputStream(input)) {
            bytes = stream.readNBytes(16 * 1024 * 1024 + 1);
        }
        check(bytes.length <= 16 * 1024 * 1024, "Expression arena exceeds 16 MiB");
        var mapper = Utilities.deterministicObjectMapper().copy();
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        try (var parser = mapper.createParser(bytes)) {
            JsonNode wire = mapper.readTree(parser);
            check(parser.nextToken() == null, "Trailing expression document");
            return decode(wire, registry);
        }
    }

    static Decoded decode(JsonNode wire, S1BuiltinOperatorRegistry registry) throws IOException {
        return new S1ExpressionTable(wire, registry).reconstruct();
    }

    private void readOperators() throws IOException {
        for (JsonNode operator: this.table.path("operators")) {
            fields(operator, "object", "parameters", "result");
            int object = this.reference(operator.path("object"), this.operatorObjects.size());
            array(operator.path("parameters"));
            for (JsonNode type: operator.path("parameters")) this.type(type);
            this.type(operator.path("result"));

        }
    }

    private void readScopes() throws IOException {
        var parents = new ArrayList<List<Integer>>();
        for (int scope = 0; scope < this.table.path("scopes").size(); scope++) {
            JsonNode declaration = this.table.path("scopes").get(scope);
            fields(declaration, "parent", "bindings");
            array(declaration.path("bindings"));
            JsonNode parent = declaration.get("parent");
            parents.add(parent.isNull() ? List.of() : List.of(this.scope(parent)));
            for (JsonNode binding: declaration.path("bindings")) {
                int id = this.binding(binding);
                check(this.owners[id] == -1, "Binding has multiple owners");
                this.owners[id] = scope;
            }
        }
        S1ArenaGraph.order(parents);
        for (int binding = 0; binding < this.owners.length; binding++) {
            check(this.owners[binding] != -1, "Binding has no owning scope");
            JsonNode declaration = this.table.path("bindings").get(binding);
            fields(declaration, "kind", "ty");
            String kind = text(declaration, "kind");
            check(Set.of("input", "correlation", "lambda_parameter").contains(kind), "Unknown binding kind");
            int type = this.type(declaration.path("ty"));
            if (!kind.equals("lambda_parameter"))
                check(this.types.get(type).isStruct(), "Input or correlation binding requires a row type");
        }
    }

    private void readExpressions() throws IOException {
        var lambdaScopes = new HashSet<Integer>();
        for (JsonNode expression: this.table.path("expressions")) {
            fields(expression, "ty", "scope", "origin", "kind");
            this.type(expression.path("ty"));
            this.scope(expression.path("scope"));
            JsonNode kind = expression.path("kind");
            if (Set.of("call", "window_aggregate").contains(kind.path("kind").asText())) this.sources.position(expression.path("origin"));
            else absent(expression.path("origin"));
            var children = new ArrayList<Integer>();
            switch (text(kind, "kind")) {
                case "input_ref" -> {
                    fields(kind, "kind", "binding", "field");
                    this.binding(kind.path("binding")); unsigned(kind.path("field"));
                }
                case "correlation", "lambda_ref" -> {
                    fields(kind, "kind", "binding"); this.binding(kind.path("binding"));
                }
                case "literal" -> {
                    fields(kind, "kind", "literal");
                    JsonNode literal = kind.path("literal");
                    fields(literal, "ty", "origin", "value");
                    check(literal.path("ty").equals(expression.path("ty"))
                            && literal.path("origin").equals(expression.path("origin")), "Literal header differs");
                }
                case "field_access" -> {
                    fields(kind, "kind", "input", "field");
                    children.add(this.expression(kind.path("input"))); unsigned(kind.path("field"));
                }
                case "call", "window_aggregate" -> {
                    if (text(kind, "kind").equals("call")) fields(kind, "kind", "operator", "arguments");
                    else {
                        fields(kind, "kind", "operator", "arguments", "ordinal", "distinct", "ignore_nulls");
                        unsigned(kind.path("ordinal"));
                        check(kind.path("distinct").isBoolean() && kind.path("ignore_nulls").isBoolean(),
                                "Invalid window aggregate flags");
                    }
                    this.reference(kind.path("operator"), this.table.path("operators").size());
                    array(kind.path("arguments"));
                    for (JsonNode argument: kind.path("arguments")) children.add(this.expression(argument));
                }
                case "lambda" -> {
                    fields(kind, "kind", "parameters", "body_scope", "body");
                    int scope = this.scope(kind.path("body_scope"));
                    check(lambdaScopes.add(scope), "Lambda scope has multiple owners");
                    JsonNode bodyScope = this.table.path("scopes").get(scope);
                    check(bodyScope.path("parent").equals(expression.path("scope")), "Lambda scope has wrong parent");
                    array(kind.path("parameters"));
                    check(kind.path("parameters").size() == bodyScope.path("bindings").size(), "Lambda scope has wrong bindings");
                    int ordinal = 0;
                    for (JsonNode parameter: kind.path("parameters")) {
                        fields(parameter, "declaration", "binding", "name", "index");
                        text(parameter, "name");
                        this.addIdentifierBytes(text(parameter, "name"));
                        unsigned(parameter.path("index"));
                        int binding = this.binding(parameter.path("binding"));
                        check(parameter.path("binding").equals(bodyScope.path("bindings").get(ordinal++)), "Lambda parameter order differs");
                        check(this.parameters[binding] == null, "Lambda parameter has multiple definitions");
                        this.parameters[binding] = parameter;
                        children.add(this.expression(parameter.path("declaration")));
                    }
                    children.add(this.expression(kind.path("body")));
                }
                default -> throw new IOException("Unsupported resolved expression kind");
            }
            this.edges.add(children);
        }
        for (JsonNode root: this.table.path("roots")) this.expression(root);
        for (int binding = 0; binding < this.owners.length; binding++) {
            if (this.table.path("bindings").get(binding).path("kind").asText().equals("lambda_parameter"))
                check(this.parameters[binding] != null, "Lambda parameter has no owning lambda");
        }
    }

    private Decoded reconstruct() throws IOException {
        List<RexNode> nodes = new ArrayList<>(Collections.nCopies(this.edges.size(), null));
        boolean[] independent = new boolean[this.edges.size()];
        var builder = new RexBuilder(new S1TypeTable.Factory());
        for (int id: S1ArenaGraph.order(this.constructionEdges())) {
            if (id >= this.edges.size()) {
                int object = id - this.edges.size();
                JsonNode definition = this.table.path("operator_objects").get(object);
                if (definition.path("kind").asText().equals("function")) {
                    this.operatorObjects.set(object, S1FunctionObjects.create(definition, this.types, this.sources, nodes));
                    JsonNode body = definition.path("body");
                    if (body.path("kind").asText().equals("sql")) {
                        int root = unsigned(body.path("root"));
                        check(independent[root] || this.table.path("expressions").get(root).path("scope").equals(body.path("scope")),
                                "Function body crosses a parameter scope");
                    }
                }
                continue;
            }
            JsonNode expression = this.table.path("expressions").get(id);
            JsonNode kind = expression.path("kind");
            int typeId = unsigned(expression.path("ty"));
            RelDataType type = this.types.get(typeId);
            int scope = unsigned(expression.path("scope"));
            String tag = text(kind, "kind");
            independent[id] = tag.equals("literal") || ((tag.equals("call") || tag.equals("field_access"))
                    && this.edges.get(id).stream().allMatch(child -> independent[child]));
            int childScope = tag.equals("lambda") ? unsigned(kind.path("body_scope")) : scope;
            for (int child: this.edges.get(id))
                check(independent[child] || unsigned(this.table.path("expressions").get(child).path("scope")) == childScope,
                        "Expression crosses an evaluation scope");
            RexNode node;
            switch (tag) {
                case "input_ref" -> {
                    int bindingType = this.visibleBinding(kind, "input", scope);
                    int field = unsigned(kind.path("field"));
                    this.checkField(bindingType, field, typeId);
                    // RexBuilder adds charset/collation metadata and can clone
                    // character types. Import retains the declared arena object.
                    node = new org.apache.calcite.rex.RexInputRef(field, type);
                }
                case "correlation" -> {
                    check(this.visibleBinding(kind, "correlation", scope) == typeId, "Correlation type differs");
                    node = builder.makeCorrel(type, new CorrelationId(unsigned(kind.path("binding"))));
                }
                case "lambda_ref" -> {
                    check(this.visibleBinding(kind, "lambda_parameter", scope) == typeId, "Lambda reference type differs");
                    JsonNode parameter = this.parameters[unsigned(kind.path("binding"))];
                    node = new RexLambdaRef(unsigned(parameter.path("index")), text(parameter, "name"), type);
                }
                case "literal" -> node = S1LiteralPayload.decode(kind.path("literal").path("value"), type);
                case "field_access" -> {
                    int input = unsigned(kind.path("input"));
                    int field = unsigned(kind.path("field"));
                    this.checkField(unsigned(this.table.path("expressions").get(input).path("ty")), field, typeId);
                    node = FelderaS1ExpressionFactory.field(nodes.get(input), field, type);
                }
                case "call", "window_aggregate" -> {
                    int operator = unsigned(kind.path("operator"));
                    JsonNode signature = this.table.path("operators").get(operator);
                    check(unsigned(signature.path("result")) == typeId, "Call result differs from signature");
                    check(signature.path("parameters").size() == kind.path("arguments").size(), "Call arity differs from signature");
                    var arguments = new ArrayList<RexNode>();
                    for (int index = 0; index < kind.path("arguments").size(); index++) {
                        int child = unsigned(kind.path("arguments").get(index));
                        check(signature.path("parameters").get(index).equals(this.table.path("expressions").get(child).path("ty")),
                                "Call argument differs from signature");
                        arguments.add(nodes.get(child));
                    }
                    var resolved = this.operatorObjects.get(unsigned(signature.path("object")));
                    var position = this.sources.position(expression.path("origin"));
                    if (tag.equals("window_aggregate")) {
                        check(resolved instanceof org.apache.calcite.sql.SqlAggFunction,
                                "Window aggregate requires an aggregate operator");
                        node = new org.apache.calcite.rel.core.Window.RexWinAggCall(position,
                                (org.apache.calcite.sql.SqlAggFunction) resolved, type, arguments,
                                unsigned(kind.path("ordinal")), kind.path("distinct").booleanValue(),
                                kind.path("ignore_nulls").booleanValue());
                    } else node = FelderaS1ExpressionFactory.call(position, type, resolved, arguments);
                }
                case "lambda" -> {
                    var parameters = new ArrayList<RexLambdaRef>();
                    for (JsonNode parameter: kind.path("parameters")) {
                        int declaration = unsigned(parameter.path("declaration"));
                        JsonNode declarationKind = this.table.path("expressions").get(declaration).path("kind");
                        check(declarationKind.path("kind").asText().equals("lambda_ref")
                                && declarationKind.path("binding").equals(parameter.path("binding")), "Invalid lambda declaration");
                        parameters.add((RexLambdaRef) nodes.get(declaration));
                    }
                    node = FelderaS1ExpressionFactory.lambda(parameters, nodes.get(unsigned(kind.path("body"))));
                }
                default -> throw new IOException("Unsupported resolved expression kind");
            }
            check(node.getType() == type, "Expression type arena identity changed");
            nodes.set(id, node);
        }
        var roots = new ArrayList<RexNode>();
        for (JsonNode root: this.table.path("roots")) roots.add(nodes.get(unsigned(root)));
        return new Decoded(this.types, nodes, roots, this.operatorObjects);
    }

    private List<List<Integer>> constructionEdges() throws IOException {
        int offset = this.edges.size();
        var graph = new ArrayList<List<Integer>>();
        for (int id = 0; id < offset; id++) {
            var children = new ArrayList<>(this.edges.get(id));
            JsonNode kind = this.table.path("expressions").get(id).path("kind");
            if (Set.of("call", "window_aggregate").contains(kind.path("kind").asText())) {
                int signature = unsigned(kind.path("operator"));
                children.add(offset + unsigned(this.table.path("operators").get(signature).path("object")));
            }
            graph.add(children);
        }
        var owners = new HashSet<Integer>();
        var publicScopes = new HashSet<Integer>();
        for (JsonNode entry: this.table.path("roots"))
            publicScopes.add(unsigned(this.table.path("expressions").get(unsigned(entry)).path("scope")));
        for (JsonNode object: this.table.path("operator_objects")) {
            var dependencies = new ArrayList<Integer>();
            if (object.path("kind").asText().equals("function") && object.path("body").path("kind").asText().equals("sql")) {
                JsonNode body = object.path("body");
                int scope = this.scope(body.path("scope")), input = this.binding(body.path("input"));
                int root = this.expression(body.path("root"));
                JsonNode declaration = this.table.path("scopes").get(scope);
                check(owners.add(scope) && declaration.path("parent").isNull()
                        && declaration.path("bindings").size() == 1
                        && declaration.path("bindings").get(0).equals(body.path("input")), "Function requires its own isolated input scope");
                check(!publicScopes.contains(scope), "Function parameter scope cannot own a public entry root");
                JsonNode binding = this.table.path("bindings").get(input);
                check(binding.path("kind").asText().equals("input"), "Function parameters require an input binding");
                JsonNode row = this.table.path("type_table").path("types").get(unsigned(binding.path("ty"))).path("kind");
                JsonNode parameters = object.path("parameters");
                check(row.path("kind").asText().equals("row") && row.path("fields").size() == parameters.size(), "Function parameter input differs");
                for (int index = 0; index < parameters.size(); index++) {
                    JsonNode field = row.path("fields").get(index), parameter = parameters.get(index);
                    check(field.path("ty").equals(parameter.path("ty")) && field.path("name").path("name").equals(parameter.path("name")),
                            "Function input differs from its ordered parameters");
                }
                dependencies.add(root);
            }
            graph.add(dependencies);
        }
        return graph;
    }

    private void checkField(int row, int field, int result) throws IOException {
        JsonNode type = this.table.path("type_table").path("types").get(row).path("kind");
        check(type.path("kind").asText().equals("row") && field < type.path("fields").size(), "Invalid row field");
        int declared = unsigned(type.path("fields").get(field).path("ty"));
        if (declared == result) return;
        JsonNode source = this.table.path("type_table").path("types").get(declared);
        JsonNode target = this.table.path("type_table").path("types").get(result);
        check(!source.path("kind").path("kind").asText().equals("named")
                && source.path("kind").equals(target.path("kind"))
                && (!source.path("nullable").booleanValue() || target.path("nullable").booleanValue()),
                "Field shape differs or reference narrows nullability");
    }

    private int visibleBinding(JsonNode kind, String expected, int scope) throws IOException {
        int binding = unsigned(kind.path("binding"));
        JsonNode declaration = this.table.path("bindings").get(binding);
        check(text(declaration, "kind").equals(expected), "Wrong binding kind");
        int cursor = scope;
        while (cursor != this.owners[binding]) {
            JsonNode parent = this.table.path("scopes").get(cursor).path("parent");
            check(!expected.equals("input") && !parent.isNull(), "Binding is outside its lexical scope");
            cursor = unsigned(parent);
        }
        return unsigned(declaration.path("ty"));
    }


    private int type(JsonNode value) throws IOException { return this.reference(value, this.types.size()); }
    private int scope(JsonNode value) throws IOException { return this.reference(value, this.table.path("scopes").size()); }
    private int binding(JsonNode value) throws IOException { return this.reference(value, this.owners.length); }
    private int expression(JsonNode value) throws IOException { return this.reference(value, this.table.path("expressions").size()); }

    private int reference(JsonNode value, int count) throws IOException {
        check(++this.references <= MAX_REFERENCES, "Expression references exceed limit");
        int id = unsigned(value);
        check(id < count, "Reference is outside its arena");
        return id;
    }

    private String identifier(JsonNode node, String name) throws IOException {
        String value = text(node, name);
        check(!value.isEmpty(), "Empty operator identity");
        this.addIdentifierBytes(value);
        return value;
    }

    private void addIdentifierBytes(String value) throws IOException {
        this.identifierBytes += value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        check(this.identifierBytes <= MAX_IDENTIFIER_BYTES, "Identifiers exceed byte limit");
    }

    private static int unsigned(JsonNode node) throws IOException {
        check(node != null && node.isIntegralNumber() && node.canConvertToInt() && node.intValue() >= 0, "Invalid unsigned integer");
        return node.intValue();
    }

    private static String text(JsonNode node, String field) throws IOException {
        check(node.path(field).isTextual(), "Missing or invalid string: " + field);
        return node.path(field).textValue();
    }

    private static void array(JsonNode node) throws IOException { check(node.isArray(), "Missing or invalid array"); }

    private static void fields(JsonNode node, String... names) throws IOException {
        check(node != null && node.isObject(), "Missing or invalid object");
        Set<String> actual = new HashSet<>(); node.fieldNames().forEachRemaining(actual::add);
        check(actual.equals(Set.of(names)), "Unknown or missing fields");
    }

    private static void absent(JsonNode origin) throws IOException {
        fields(origin, "kind");
        check(text(origin, "kind").equals("absent"), "Source-bearing expression requires a source adapter");
    }

    private static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}
