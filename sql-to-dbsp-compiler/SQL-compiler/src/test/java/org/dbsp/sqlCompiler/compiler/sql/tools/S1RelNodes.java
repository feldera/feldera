package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.FelderaS1RelFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Construct relational nodes from the validated object and property arenas.
 * Table resolution is an explicit catalog dependency; this adapter does not
 * synthesize catalog declarations from display names. */
final class S1RelNodes {
    @FunctionalInterface interface Tables {
        RelOptTable resolve(int relation, List<String> name, RelDataType row) throws IOException;
    }
    record Decoded(List<RelNode> nodes, List<RelNode> roots, S1ExpressionObjectsTable.Decoded expressions, S1Catalog.Decoded catalog) { }

    private final JsonNode record;
    private final S1ExpressionObjectsTable.Decoded expressions;
    private final List<SqlOperator> aggregates;
    private final RelOptCluster cluster;
    private final Tables tables;
    private final S1Catalog.Decoded catalog;
    private final List<List<Integer>> edges = new ArrayList<>();
    private final List<List<RexNode>> local;

    private S1RelNodes(JsonNode record, S1BuiltinOperatorRegistry registry, Tables tables) throws IOException {
        this.record = record;
        S1InputProducers.validate(record.path("producers"), record.path("table"));
        this.expressions = S1ExpressionObjectsTable.decode(record.path("expressions"), record.path("table"), registry);
        this.catalog = tables == null ? S1Catalog.decode(record, this.expressions) : null;
        this.tables = this.catalog == null ? tables : this.catalog.resolver();
        var operators = record.at("/properties/operators");
        if (operators.path("kind").asText().equals("shared")) {
            S1MetadataWire.fields(operators, "kind");
            this.aggregates = this.expressions.operators();
        } else {
            S1MetadataWire.fields(operators, "kind", "objects");
            check(operators.path("kind").asText().equals("owned"), "Unknown operator arena");
            this.aggregates = S1OperatorObjects.decode(operators.path("objects"), this.expressions.types(),
                    S1ExpressionSources.of(null), registry).objects();
        }
        this.cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()),
                new RexBuilder(org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.S1RelTypeFactory.create()));
        int count = record.at("/producers/relations").size();
        check(record.at("/properties/relations").size() == count, "Relation property count differs");
        this.local = new ArrayList<>(Collections.nCopies(count, null));
        for (var use: record.at("/expressions/relation_uses")) {
            int relation = use.path("relation").intValue();
            var roots = new ArrayList<RexNode>();
            for (var root: use.path("roots")) roots.add(this.expressions.expressions().get(root.intValue()));
            if (this.local.get(relation) != null) {
                var previous = this.local.get(relation);
                check(previous.size() == roots.size(), "Shared relation root count differs");
                for (int index = 0; index < roots.size(); index++) check(previous.get(index) == roots.get(index), "Shared relation root identity differs");
            } else this.local.set(relation, List.copyOf(roots));
        }
        for (var producer: record.at("/producers/relations")) this.edges.add(integers(producer.path("inputs")));
        for (var roots: this.local) check(roots != null, "Relation is missing expression slots");
    }

    static Decoded decode(JsonNode record, S1BuiltinOperatorRegistry registry, Tables tables) throws IOException {
        var decoded = new S1RelNodes(record, registry, tables).construct();
        verify(record, decoded, registry);
        return decoded;
    }

    static Decoded decodeCatalog(JsonNode record, S1BuiltinOperatorRegistry registry) throws IOException {
        return decode(record, registry, null);
    }

    private static void verify(JsonNode record, Decoded decoded, S1BuiltinOperatorRegistry registry) throws IOException {
        var captured = S1ExpressionSharingProbe.collect(decoded.roots());
        check(captured.relationNodes.size() == decoded.nodes().size(), "Relation root reachability changed");
        var typeIds = new java.util.IdentityHashMap<RelDataType, Integer>();
        for (int id = 0; id < decoded.expressions().types().size(); id++) typeIds.put(decoded.expressions().types().get(id), id);
        var typeTable = new S1TypeTable.Encoded((com.fasterxml.jackson.databind.node.ObjectNode) record.at("/table/type_table"), typeIds);
        var inputs = new S1InputUsesTable.Encoded((com.fasterxml.jackson.databind.node.ObjectNode) record.path("table"), typeTable);
        com.fasterxml.jackson.databind.node.ObjectNode properties;
        if (record.at("/properties/operators/kind").asText().equals("shared")) {
            var operators = new java.util.IdentityHashMap<SqlOperator, Integer>();
            for (int id = 0; id < decoded.expressions().operators().size(); id++) operators.put(decoded.expressions().operators().get(id), id);
            properties = S1RelationProperties.encodeShared(captured, inputs, operators);
        } else properties = S1RelationProperties.encode(captured, inputs, registry);
        check(properties.equals(record.path("properties")), "Reconstructed relation properties differ");
        for (int id = 0; id < decoded.nodes().size(); id++) {
            var node = decoded.nodes().get(id);
            check(captured.relationNodes.get(id) == node, "Relation sharing or order changed");
            check(node.getClass().getSimpleName().equals(record.at("/producers/relations").get(id).path("form").asText()), "Relation class changed");
        }
        for (var use: record.at("/expressions/relation_uses")) {
            var roots = captured.relationExpressions.get(decoded.nodes().get(use.path("relation").intValue()));
            check(roots.size() == use.path("roots").size(), "Relation local field count changed");
            for (int index = 0; index < roots.size(); index++)
                check(roots.get(index) == decoded.expressions().expressions().get(use.path("roots").get(index).intValue()), "Relation expression object identity changed");
        }
    }

    private List<Integer> constructionOrder() throws IOException {
        var topological = S1ArenaGraph.order(this.edges);
        if (!this.record.has("table_scan_order")) return topological;
        var scans = integers(this.record.path("table_scan_order"));
        var seen = new boolean[this.edges.size()];
        var order = new ArrayList<Integer>();
        for (int id: scans) {
            check(id < seen.length && !seen[id], "Invalid or repeated table-scan identity");
            String form = this.record.at("/producers/relations").get(id).path("form").asText();
            check(form.equals("LogicalTableScan") || form.equals("JdbcTableScan"), "Scan order references a non-scan relation");
            check(this.edges.get(id).isEmpty(), "Table scan has inputs");
            seen[id] = true; order.add(id);
        }
        for (int id: topological) {
            String form = this.record.at("/producers/relations").get(id).path("form").asText();
            if (form.equals("LogicalTableScan") || form.equals("JdbcTableScan"))
                check(seen[id], "Table scan missing from order");
            else order.add(id);
        }
        // Calcite rewrites may replace inputs with newer nodes, so original
        // creation order is not generally topological. Only independent scans
        // participate in RelAnd ordering; build the rest by dependencies.
        return order;
    }

    private Decoded construct() throws IOException {
        var nodes = new ArrayList<RelNode>(Collections.nCopies(this.edges.size(), null));
        for (int id: this.constructionOrder()) {
            var producer = this.record.at("/producers/relations").get(id);
            var properties = this.record.at("/properties/relations").get(id);
            var fields = properties.path("fields");
            var row = this.expressions.types().get(producer.path("row").intValue());
            var metadata = S1RelationMetadata.decode(properties.path("metadata"), row.getFieldCount());
            var traits = metadata.traits(); var hints = metadata.hints();
            var inputs = this.edges.get(id).stream().map(nodes::get).toList();
            var roots = new Slots(this.local.get(id));
            RelNode node;
            try {
                switch (producer.path("form").asText()) {
                    case "LogicalTableScan" -> {
                        var table = this.tables.resolve(id, strings(fields.path("name")), row);
                        check(table.getRowType() == row && table.getQualifiedName().equals(strings(fields.path("name"))), "Catalog table identity or name differs");
                        node = new LogicalTableScan(this.cluster, traits, hints, table);
                    }
                    case "LogicalValues" -> {
                        var tuples = ImmutableList.<ImmutableList<RexLiteral>>builder();
                        for (int tuple = 0; tuple < fields.path("tuples").intValue(); tuple++) {
                            var values = ImmutableList.<RexLiteral>builder();
                            for (int field = 0; field < row.getFieldCount(); field++) values.add(roots.literal());
                            tuples.add(values.build());
                        }
                        node = new LogicalValues(this.cluster, traits, hints, row, tuples.build());
                    }
                    case "LogicalProject" -> node = new LogicalProject(this.cluster, traits, hints, inputs.get(0),
                            roots.take(fields.path("expressions").intValue()), row, variables(fields.path("variables")));
                    case "LogicalFilter" -> node = new LogicalFilter(this.cluster, traits, hints, inputs.get(0), roots.next(), variables(fields.path("variables")));
                    case "LogicalJoin", "LogicalAsofJoin" -> {
                        check(fields.path("system_fields").isEmpty(), "System fields require their names and indices in the relation contract");
                        var condition = roots.next(); var kind = JoinRelType.valueOf(fields.path("join_kind").asText());
                        if (producer.path("form").asText().equals("LogicalAsofJoin")) {
                            check(fields.path("variables").isEmpty() && !fields.path("semi_join_done").booleanValue(), "ASOF join has unsupported mutable flags");
                            node = new LogicalAsofJoin(this.cluster, traits, hints, inputs.get(0), inputs.get(1), condition, roots.next(), kind, ImmutableList.of());
                        } else node = new LogicalJoin(this.cluster, traits, hints, inputs.get(0), inputs.get(1), condition,
                                variables(fields.path("variables")), kind, fields.path("semi_join_done").booleanValue(), ImmutableList.of());
                    }
                    case "LogicalAggregate" -> node = new LogicalAggregate(this.cluster, traits, hints, inputs.get(0), bits(fields.path("group")),
                            bitSets(fields.path("groups")), this.aggregateCalls(fields.path("calls"), roots, inputs.get(0).getRowType().getFieldCount()));
                    case "LogicalWindow" -> {
                        var constants = new ArrayList<RexLiteral>();
                        for (var ignored: producer.path("constants")) constants.add(roots.literal());
                        node = new LogicalWindow(this.cluster, traits, hints, inputs.get(0), constants, row,
                                windowGroups(fields.path("groups"), roots, inputs.get(0).getRowType().getFieldCount() + constants.size()));
                    }
                    case "LogicalSort" -> {
                        var collation = S1RelationMetadata.collation(fields.path("collation"), row.getFieldCount());
                        var offset = fields.path("offset").booleanValue() ? roots.next() : null;
                        var fetch = fields.path("fetch").booleanValue() ? roots.next() : null;
                        node = LogicalSort.create(inputs.get(0), collation, offset, fetch).copy(traits, inputs.get(0), collation, offset, fetch).withHints(hints);
                    }
                    case "LogicalUnion" -> node = new LogicalUnion(this.cluster, traits, hints, inputs, fields.path("all").booleanValue());
                    case "LogicalIntersect" -> node = new LogicalIntersect(this.cluster, traits, hints, inputs, fields.path("all").booleanValue());
                    case "LogicalMinus" -> node = new LogicalMinus(this.cluster, traits, hints, inputs, fields.path("all").booleanValue());
                    case "LogicalCorrelate" -> node = new LogicalCorrelate(this.cluster, traits, hints, inputs.get(0), inputs.get(1),
                            new CorrelationId(fields.path("correlation").intValue()), bits(fields.path("required")), JoinRelType.valueOf(fields.path("join_kind").asText()));
                    case "Uncollect" -> {
                        check(hints.isEmpty(), "Uncollect cannot carry hints");
                        node = new Uncollect(this.cluster, traits, inputs.get(0), fields.path("ordinality").booleanValue(), strings(fields.path("aliases")),
                                bits(fields.path("passthrough")), bits(fields.path("collections")), fields.path("expand_struct_fields").booleanValue(), fields.path("outer").booleanValue());
                    }
                    case "Collect" -> {
                        check(hints.isEmpty(), "Collect cannot carry hints");
                        node = Collect.create(inputs.get(0), row).copy(traits, inputs.get(0));
                        check(((Collect) node).getFieldName().equals(fields.path("field").asText())
                                && ((Collect) node).getCollectionType().name().equals(fields.path("collection").asText()), "Collect row differs from its properties");
                    }
                    case "LogicalTableFunctionScan" -> {
                        java.lang.reflect.Type element = switch (fields.path("element").asText()) {
                            case "absent" -> null; case "object" -> Object.class; case "object_array" -> Object[].class;
                            default -> throw new IOException("Unknown table function element type");
                        };
                        node = new LogicalTableFunctionScan(this.cluster, traits, hints, inputs, roots.next(), element, row, mappings(fields.path("mappings")));
                    }
                    default -> throw new IOException("Relation requires an explicit constructor/catalog adapter: " + producer.path("form"));
                }
                roots.exhausted();
                FelderaS1RelFactory.retainRowType(node, row);
                check(S1RelationMetadata.encode(node).equals(properties.path("metadata")), "Relation metadata changed during construction");
            } catch (IllegalArgumentException | AssertionError error) {
                throw new IOException("Relation " + id + " (" + producer.path("form").asText() + ") construction failed", error);
            }
            nodes.set(id, node);
        }
        var roots = new ArrayList<RelNode>();
        for (var root: this.record.at("/producers/roots")) roots.add(nodes.get(root.intValue()));
        return new Decoded(List.copyOf(nodes), List.copyOf(roots), this.expressions, this.catalog);
    }

    private List<AggregateCall> aggregateCalls(JsonNode definitions, Slots roots, int inputWidth) throws IOException {
        var calls = new ArrayList<AggregateCall>();
        for (var call: definitions) {
            var operator = this.aggregates.get(call.path("operator").intValue());
            check(operator instanceof SqlAggFunction, "Aggregate call requires aggregate operator");
            var distinct = call.path("distinct_keys");
            var filter = call.path("filter"); var name = call.path("name");
            calls.add(AggregateCall.create(S1RelationMetadata.position(call.path("position")), (SqlAggFunction) operator,
                    call.path("distinct").booleanValue(), call.path("approximate").booleanValue(), call.path("ignore_nulls").booleanValue(),
                    roots.take(call.path("direct_arguments").intValue()), integers(call.path("arguments")),
                    filter.path("kind").asText().equals("absent") ? -1 : filter.path("field").intValue(),
                    distinct.path("kind").asText().equals("absent") ? null : bits(distinct.path("fields")),
                    S1RelationMetadata.collation(call.path("collation"), inputWidth), this.expressions.types().get(call.path("ty").intValue()),
                    name.path("kind").asText().equals("absent") ? null : name.path("name").textValue()));
        }
        return calls;
    }

    private static List<Window.Group> windowGroups(JsonNode definitions, Slots roots, int width) throws IOException {
        var groups = new ArrayList<Window.Group>();
        for (var group: definitions) {
            var lower = bound(group.path("lower").asText(), roots); var upper = bound(group.path("upper").asText(), roots);
            var calls = new ArrayList<Window.RexWinAggCall>();
            for (int index = 0; index < group.path("calls").intValue(); index++) {
                var call = roots.next(); check(call instanceof Window.RexWinAggCall, "Window slot requires extracted aggregate");
                calls.add((Window.RexWinAggCall) call);
            }
            groups.add(new Window.Group(bits(group.path("keys")), group.path("rows").booleanValue(), lower, upper,
                    RexWindowExclusion.valueOf(group.path("exclusion").asText()), S1RelationMetadata.collation(group.path("collation"), width), calls));
        }
        return groups;
    }
    private static RexWindowBound bound(String kind, Slots roots) throws IOException {
        return switch (kind) {
            case "unbounded_preceding" -> RexWindowBounds.UNBOUNDED_PRECEDING;
            case "unbounded_following" -> RexWindowBounds.UNBOUNDED_FOLLOWING;
            case "current_row" -> RexWindowBounds.CURRENT_ROW;
            case "preceding" -> RexWindowBounds.preceding(roots.next());
            case "following" -> RexWindowBounds.following(roots.next());
            default -> throw new IOException("Unknown window bound");
        };
    }
    private static Set<RelColumnMapping> mappings(JsonNode wire) throws IOException {
        if (wire.path("kind").asText().equals("absent")) return null;
        check(wire.path("kind").asText().equals("present"), "Unknown column mapping kind");
        var result = new HashSet<RelColumnMapping>();
        for (var mapping: wire.path("mappings")) result.add(new RelColumnMapping(mapping.path("output").intValue(),
                mapping.path("input").intValue(), mapping.path("column").intValue(), mapping.path("derived").booleanValue()));
        return result;
    }
    private static ImmutableSet<CorrelationId> variables(JsonNode values) { return integers(values).stream().map(CorrelationId::new).collect(ImmutableSet.toImmutableSet()); }
    private static ImmutableBitSet bits(JsonNode values) { return ImmutableBitSet.of(integers(values)); }
    private static List<ImmutableBitSet> bitSets(JsonNode values) { var result = new ArrayList<ImmutableBitSet>(); values.forEach(value -> result.add(bits(value))); return result; }
    private static List<Integer> integers(JsonNode values) { var result = new ArrayList<Integer>(); values.forEach(value -> result.add(value.intValue())); return result; }
    private static List<String> strings(JsonNode values) { var result = new ArrayList<String>(); values.forEach(value -> result.add(value.textValue())); return result; }

    private static final class Slots {
        private final List<RexNode> roots;
        private int index;
        Slots(List<RexNode> roots) { this.roots = roots; }
        RexNode next() throws IOException { check(this.index < this.roots.size(), "Missing relation expression slot"); return this.roots.get(this.index++); }
        List<RexNode> take(int count) throws IOException {
            check(count >= 0 && count <= this.roots.size() - this.index, "Invalid relation expression count");
            var result = this.roots.subList(this.index, this.index + count); this.index += count; return result;
        }
        RexLiteral literal() throws IOException { var node = this.next(); check(node instanceof RexLiteral, "Relation slot requires literal"); return (RexLiteral) node; }
        void exhausted() throws IOException { check(this.index == this.roots.size(), "Extra relation expression slots"); }
    }
    private static void check(boolean condition, String message) throws IOException { if (!condition) throw new IOException(message); }
}
