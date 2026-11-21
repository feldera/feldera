package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;

import static org.dbsp.sqlCompiler.compiler.sql.tools.S1MetadataWire.*;

/** Execute explicit Rust constructors against decoded FRel objects. This class
 * invokes no optimizer or decorrelator and never consults source SQL. */
final class CorrelationRecipe {
    private final S1RelNodes.Decoded original;
    private final List<RelNode> nodes = new ArrayList<>();
    private int expressionBudget = 100_000;
    private int fieldBudget = 100_000;
    private int nameBudget = 8 * 1024 * 1024;
    private final IdentityHashMap<Object, Boolean> inspected = new IdentityHashMap<>();

    private CorrelationRecipe(S1RelNodes.Decoded original) { this.original = original; }

    static List<RelNode> decode(JsonNode wire, JsonNode recipe, S1BuiltinOperatorRegistry registry) throws IOException {
        fields(recipe, "schema_version", "id", "nodes", "roots", "removed_correlates");
        check(text(recipe.path("schema_version")).equals("correlation-recipe-v1"), "Unknown correlation recipe");
        check(recipe.path("id").equals(wire.path("id")), "Recipe identity differs from input fragment");
        var decoded = S1RelNodes.decodeCatalog(wire, registry);
        check(unsigned(recipe.path("removed_correlates")) == decoded.nodes().stream()
                .filter(node -> node instanceof Correlate).count(), "Removed Correlate count differs");
        var builder = new CorrelationRecipe(decoded);
        array(recipe.path("nodes"));
        check(recipe.path("nodes").size() <= 100_000, "Recipe node count exceeds limit");
        for (var node: recipe.path("nodes")) builder.nodes.add(builder.node(node));
        array(recipe.path("roots"));
        check(recipe.path("roots").size() == decoded.roots().size(), "Recipe root count differs");
        var roots = new ArrayList<RelNode>();
        for (int index = 0; index < decoded.roots().size(); index++) {
            var root = builder.plan(recipe.path("roots").get(index));
            check(root.getRowType().equals(decoded.roots().get(index).getRowType()), "Recipe changed root row type");
            builder.checkIndependent(root);
            roots.add(root);
        }
        return List.copyOf(roots);
    }

    private void charge(int slots) throws IOException {
        check(slots >= 0 && slots <= this.fieldBudget, "Recipe field budget exceeded");
        this.fieldBudget -= slots;
    }

    private void chargeName(String name) throws IOException {
        check(name.length() <= this.nameBudget / 2, "Recipe column-name budget exceeded");
        this.nameBudget -= name.length() * 2;
    }

    private void chargeRow(RelDataType row) throws IOException {
        this.charge(row.getFieldCount());
        for (var field: row.getFieldList()) this.chargeName(field.getName());
    }

    /** Identity-based iteration bounds shared DAGs and avoids recursive stack
     * growth. No surviving correlation may depend on a removed ancestor. */
    private void checkIndependent(RelNode root) throws IOException {
        var pending = new ArrayDeque<Object>(); pending.add(root);
        while (!pending.isEmpty()) {
            var value = pending.removeFirst();
            if (this.inspected.put(value, true) != null) continue;
            check(this.inspected.size() <= 100_000, "Recipe traversal limit exceeded");
            if (value instanceof RelNode relation) {
                check(!(relation instanceof Correlate), "Recipe retains a Correlate");
                this.charge(relation.getRowType().getFieldCount() + relation.getInputs().size());
                pending.addAll(relation.getInputs());
                S1RelExpressions.forEachRoot(relation, pending::add, ignored -> { });
            } else if (value instanceof org.apache.calcite.rex.RexCorrelVariable) {
                throw new IOException("Recipe retains a free correlation variable");
            } else if (value instanceof org.apache.calcite.rex.RexSubQuery
                    || value instanceof org.apache.calcite.rex.RexOver) {
                throw new IOException("Recipe retains an unsupported nested expression");
            } else if (value instanceof RexCall call) {
                this.charge(call.operands.size()); pending.addAll(call.operands);
            } else if (value instanceof RexFieldAccess field) {
                pending.add(field.getReferenceExpr());
            } else if (!(value instanceof RexLiteral) && !(value instanceof RexInputRef)) {
                throw new IOException("Recipe retains an unsupported expression: " + value.getClass().getName());
            }
        }
    }

    private RelNode plan(JsonNode id) throws IOException { return this.nodes.get(reference(id, this.nodes.size())); }
    private RelNode original(JsonNode id) throws IOException {
        return this.original.nodes().get(reference(id, this.original.nodes().size()));
    }
    private RelDataType type(JsonNode id) throws IOException {
        var types = this.original.expressions().types(); return types.get(reference(id, types.size()));
    }
    private RexNode originalExpression(JsonNode id) throws IOException {
        var expressions = this.original.expressions().expressions();
        return expressions.get(reference(id, expressions.size()));
    }
    private List<RelHint> hints(JsonNode id, Class<? extends RelNode> expected) throws IOException {
        if (id.isNull()) return List.of();
        var relation = this.original(id);
        check(relation.getClass() == expected && relation instanceof Hintable, "Hint source relation differs");
        var hints = ((Hintable) relation).getHints();
        for (var hint: hints) check(hint.hintName.equals("alias"), "Unsupported optimizer hint");
        return hints;
    }
    private List<RelNode> inputs(JsonNode values) throws IOException {
        array(values); this.charge(values.size()); var result = new ArrayList<RelNode>();
        for (var id: values) result.add(this.plan(id));
        return result;
    }

    private RelNode node(JsonNode wire) throws IOException {
        String kind = text(wire.path("kind"));
        switch (kind) {
            case "original" -> {
                fields(wire, "kind", "relation");
                var result = this.original(wire.path("relation"));
                this.checkIndependent(result);
                return result;
            }
            case "rebuild" -> {
                fields(wire, "kind", "relation", "inputs");
                var original = this.original(wire.path("relation"));
                check(!(original instanceof Correlate), "Rebuild cannot hide a Correlate");
                var inputs = this.inputs(wire.path("inputs"));
                check(inputs.size() == original.getInputs().size(), "Rebuild input count differs");
                for (int index = 0; index < inputs.size(); index++)
                    check(inputs.get(index).getRowType().equals(original.getInput(index).getRowType()),
                            "Rebuild changed an input row type");
                this.chargeRow(original.getRowType());
                var result = original.copy(original.getTraitSet(), inputs);
                this.checkIndependent(result);
                return result;
            }
            case "project" -> {
                fields(wire, "kind", "input", "expressions", "columns", "relation");
                var input = this.plan(wire.path("input"));
                var expressions = this.expressions(wire.path("expressions"), input.getRowType(), input, 0);
                array(wire.path("columns"));
                this.charge(wire.path("columns").size());
                check(wire.path("columns").size() == expressions.size(), "Projection width differs");
                var names = new ArrayList<String>(); var types = new ArrayList<RelDataType>();
                for (int index = 0; index < expressions.size(); index++) {
                    var column = wire.path("columns").get(index); fields(column, "name", "ty");
                    var type = this.type(column.path("ty"));
                    check(expressions.get(index).getType().equals(type), "Projection expression type differs");
                    var name = text(column.path("name")); this.chargeName(name);
                    names.add(name); types.add(type);
                }
                check(new java.util.HashSet<>(names).size() == names.size(), "Duplicate projection field names");
                var row = input.getCluster().getTypeFactory().createStructType(types, names);
                return LogicalProject.create(input, this.hints(wire.path("relation"), LogicalProject.class), expressions, row);
            }
            case "filter" -> {
                fields(wire, "kind", "input", "condition", "relation");
                check(!wire.path("relation").isNull(), "Filter needs its original metadata source");
                var input = this.plan(wire.path("input"));
                var condition = this.expression(wire.path("condition"), input.getRowType(), input, 0);
                check(org.apache.calcite.sql.type.SqlTypeUtil.inBooleanFamily(condition.getType()), "Filter requires Boolean");
                return LogicalFilter.create(input, condition)
                        .withHints(this.hints(wire.path("relation"), LogicalFilter.class));
            }
            case "distinct" -> {
                fields(wire, "kind", "input");
                var input = this.plan(wire.path("input"));
                check(input.getRowType().getFieldCount() > 0, "Empty domain requires a cardinality-preserving distinct");
                this.chargeRow(input.getRowType());
                var keys = ImmutableBitSet.range(input.getRowType().getFieldCount());
                return LogicalAggregate.create(input, List.of(), keys, List.of(keys), List.of());
            }
            case "join" -> {
                fields(wire, "kind", "left", "right", "join_kind", "keys");
                var left = this.plan(wire.path("left")); var right = this.plan(wire.path("right"));
                this.chargeRow(left.getRowType()); this.chargeRow(right.getRowType());
                var builder = left.getCluster().getRexBuilder();
                String name = text(wire.path("join_kind"));
                check(name.equals("INNER") || name.equals("LEFT"), "Unsupported recipe join kind");
                array(wire.path("keys")); this.charge(wire.path("keys").size());
                var conditions = new ArrayList<RexNode>();
                for (var key: wire.path("keys")) {
                    array(key); check(key.size() == 2, "Join key needs two fields");
                    int l = reference(key.get(0), left.getRowType().getFieldCount());
                    int r = reference(key.get(1), right.getRowType().getFieldCount());
                    var leftType = left.getRowType().getFieldList().get(l).getType();
                    var rightType = right.getRowType().getFieldList().get(r).getType();
                    check(leftType.equals(rightType), "Domain key types differ");
                    conditions.add(builder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                            new RexInputRef(l, leftType),
                            new RexInputRef(left.getRowType().getFieldCount() + r, rightType)));
                }
                return LogicalJoin.create(left, right, List.of(), RexUtil.composeConjunction(builder, conditions),
                        Set.of(), JoinRelType.valueOf(name));
            }
            case "set" -> {
                fields(wire, "kind", "relation", "inputs");
                var original = this.original(wire.path("relation"));
                check(original instanceof SetOp, "Set recipe requires an original set operation");
                var inputs = this.inputs(wire.path("inputs"));
                check(inputs.size() == original.getInputs().size() && !inputs.isEmpty(), "Set input count differs");
                for (var input: inputs)
                    check(input.getRowType().equals(inputs.get(0).getRowType()), "Set branch row types differ");
                var set = (SetOp) original;
                this.chargeRow(inputs.get(0).getRowType());
                return set.copy(set.getTraitSet(), inputs, set.all);
            }
            case "partitioned_fetch" -> { return this.partitionedFetch(wire); }
            case "aggregate" -> {
                fields(wire, "kind", "relation", "input", "group", "arguments");
                var source = this.original(wire.path("relation"));
                check(source instanceof LogicalAggregate, "Aggregate source differs");
                var original = (LogicalAggregate) source;
                check(original.getGroupSets().size() == 1
                        && original.getGroupSets().get(0).equals(original.getGroupSet()),
                        "Aggregate grouping sets cannot be extended");
                var input = this.plan(wire.path("input"));
                array(wire.path("group")); array(wire.path("arguments"));
                this.charge(wire.path("group").size() + wire.path("arguments").size());
                var group = ImmutableBitSet.builder(); int previous = -1;
                for (var field: wire.path("group")) {
                    int index = reference(field, input.getRowType().getFieldCount());
                    check(index > previous, "Aggregate group positions must be sorted and unique");
                    group.set(index); previous = index;
                }
                var keys = group.build(); check(keys.cardinality() > original.getGroupCount(), "Missing aggregate domain");
                check(wire.path("arguments").size() == original.getAggCallList().size(), "Aggregate call count differs");
                var calls = new ArrayList<org.apache.calcite.rel.core.AggregateCall>();
                for (int index = 0; index < original.getAggCallList().size(); index++) {
                    var call = original.getAggCallList().get(index);
                    boolean count = call.getAggregation() == SqlStdOperatorTable.COUNT;
                    check((count || call.getAggregation() == SqlStdOperatorTable.MIN
                                    || call.getAggregation() == SqlStdOperatorTable.MAX
                                    || call.getAggregation() == SqlStdOperatorTable.SUM
                                    || call.getAggregation() == SqlStdOperatorTable.AVG
                                    || call.getAggregation() == SqlStdOperatorTable.SINGLE_VALUE
                                    || call.getAggregation() == org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlArrayAggFunction.INSTANCE)
                            && !call.isDistinct() && !call.isApproximate() && !call.ignoreNulls()
                            && call.rexList.isEmpty() && call.filterArg == -1 && call.distinctKeys == null
                            && call.getCollation().getFieldCollations().isEmpty()
                            && (count ? call.getArgList().size() <= 1 : call.getArgList().size() == 1),
                            "Unsupported aggregate metadata");
                    var arguments = wire.path("arguments").get(index); array(arguments);
                    this.charge(arguments.size());
                    check(arguments.size() == call.getArgList().size(), "Aggregate arity differs");
                    var mapped = new ArrayList<Integer>();
                    for (int argument = 0; argument < arguments.size(); argument++) {
                        int position = reference(arguments.get(argument), input.getRowType().getFieldCount());
                        check(input.getRowType().getFieldList().get(position).getType().equals(
                                original.getInput().getRowType().getFieldList().get(call.getArgList().get(argument)).getType()),
                                "Aggregate argument type differs");
                        mapped.add(position);
                    }
                    var adapted = call.adaptTo(input, mapped, -1, original.getGroupCount(), keys.cardinality());
                    if (call.getName() != null) this.chargeName(call.getName());
                    var factory = input.getCluster().getTypeFactory();
                    check(factory.createTypeWithNullability(adapted.getType(), true).equals(
                            factory.createTypeWithNullability(call.getType(), true)), "Aggregate changed its SQL result type");
                    calls.add(adapted);
                }
                this.charge(keys.cardinality() + calls.size());
                for (int key: keys) this.chargeName(input.getRowType().getFieldList().get(key).getName());
                return LogicalAggregate.create(input, this.hints(wire.path("relation"), LogicalAggregate.class),
                        keys, List.of(keys), calls);
            }
            default -> throw new IOException("Unknown correlation recipe node: " + kind);
        }
    }

    private RelNode partitionedFetch(JsonNode wire) throws IOException {
        fields(wire, "kind", "relation", "input", "partition", "order", "limit");
        var source = this.original(wire.path("relation"));
        check(source instanceof Sort, "Partitioned FETCH requires an original sort");
        var sort = (Sort) source;
        long limit = unsigned(wire.path("limit"));
        check(limit > 0 && limit <= Integer.MAX_VALUE && sort.offset == null
                        && sort.fetch instanceof RexLiteral literal
                        && BigDecimal.valueOf(limit).equals(literal.getValueAs(BigDecimal.class)),
                "Partitioned FETCH limit differs");
        var input = this.plan(wire.path("input"));
        var row = input.getRowType();
        int width = row.getFieldCount();
        var partition = wire.path("partition");
        var order = wire.path("order");
        array(partition); array(order);
        this.charge(partition.size() + order.size() + width + 1);
        var keys = ImmutableBitSet.builder();
        for (var field: partition)
            keys.set(reference(field, width));
        var keyFields = keys.build();
        check(!keyFields.isEmpty() && keyFields.cardinality() == partition.size(),
                "Partitioned FETCH keys must be nonempty and distinct");
        var originalOrder = sort.getCollation().getFieldCollations();
        check(originalOrder.size() == order.size(), "Partitioned FETCH order arity differs");
        check(!originalOrder.isEmpty() || limit == 1, "Unordered FETCH requires LIMIT 1");
        var ordering = new ArrayList<RelFieldCollation>();
        for (int index = 0; index < order.size(); index++) {
            var old = originalOrder.get(index);
            int field = reference(order.get(index), width);
            check(row.getFieldList().get(field).getType().equals(
                            sort.getRowType().getFieldList().get(old.getFieldIndex()).getType()),
                    "Partitioned FETCH sort field type differs");
            ordering.add(old.withFieldIndex(field));
        }
        var cluster = input.getCluster();
        var rankType = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        var rank = new Window.RexWinAggCall(SqlParserPos.ZERO, SqlStdOperatorTable.ROW_NUMBER,
                rankType, List.of(), 0, false, false);
        var group = new Window.Group(keyFields, true, RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW, RexWindowExclusion.EXCLUDE_NO_OTHER,
                RelCollations.of(ordering), List.of(rank));
        String name = "$correlation_rank";
        var names = Set.copyOf(row.getFieldNames());
        for (int suffix = 0; names.contains(name); suffix++)
            name = "$correlation_rank" + suffix;
        var rankedRow = cluster.getTypeFactory().builder().addAll(row.getFieldList()).add(name, rankType).build();
        this.chargeRow(rankedRow);
        var window = new LogicalWindow(cluster, cluster.traitSet(), input, List.of(), rankedRow, List.of(group));
        var rex = cluster.getRexBuilder();
        var predicate = rex.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                rex.makeInputRef(rankType, width), rex.makeExactLiteral(BigDecimal.valueOf(limit), rankType));
        var filter = LogicalFilter.create(window, predicate);
        var payload = new ArrayList<RexNode>();
        for (var field: row.getFieldList())
            payload.add(rex.makeInputRef(field.getType(), field.getIndex()));
        return LogicalProject.create(filter, this.hints(wire.path("relation"), LogicalSort.class), payload, row);
    }

    private List<RexNode> expressions(JsonNode values, RelDataType row, RelNode input, int depth) throws IOException {
        array(values); var result = new ArrayList<RexNode>();
        for (var value: values) result.add(this.expression(value, row, input, depth));
        return result;
    }

    private RexNode expression(JsonNode wire, RelDataType row, RelNode input, int depth) throws IOException {
        check(depth <= 128 && --this.expressionBudget >= 0, "Recipe expression limit exceeded");
        switch (text(wire.path("kind"))) {
            case "existing" -> {
                fields(wire, "kind", "expression");
                var expression = this.originalExpression(wire.path("expression"));
                check(expression instanceof RexLiteral, "Only literals may bypass expression remapping");
                return expression;
            }
            case "input" -> {
                fields(wire, "kind", "field", "ty");
                int field = reference(wire.path("field"), row.getFieldCount());
                var type = this.type(wire.path("ty"));
                check(row.getFieldList().get(field).getType().equals(type), "Recipe input field " + field
                        + " type differs: expected " + type.getFullTypeString() + ", actual "
                        + row.getFieldList().get(field).getType().getFullTypeString());
                return new RexInputRef(field, type);
            }
            case "widen_nullability" -> {
                fields(wire, "kind", "input", "ty");
                var expression = this.expression(wire.path("input"), row, input, depth + 1);
                var type = this.type(wire.path("ty"));
                var factory = input.getCluster().getTypeFactory();
                check(!expression.getType().isNullable() && type.isNullable()
                        && factory.createTypeWithNullability(type, false).equals(expression.getType()),
                        "Recipe coercion must only widen top-level nullability");
                return input.getCluster().getRexBuilder().makeCast(type, expression);
            }
            case "outer_input" -> {
                fields(wire, "kind", "field", "ty");
                int field = reference(wire.path("field"), row.getFieldCount());
                var type = input.getCluster().getTypeFactory().createTypeWithNullability(this.type(wire.path("ty")), true);
                check(row.getFieldList().get(field).getType().equals(type), "Nullable outer field type differs");
                return new RexInputRef(field, type);
            }
            case "zero_if_null" -> {
                fields(wire, "kind", "input", "ty");
                var expression = this.expression(wire.path("input"), row, input, depth + 1);
                var type = this.type(wire.path("ty"));
                check(type.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.BIGINT && !type.isNullable()
                        && input.getCluster().getTypeFactory().createTypeWithNullability(type, true)
                                .equals(expression.getType()), "COUNT default requires nullable BIGINT to nonnullable BIGINT");
                var builder = input.getCluster().getRexBuilder();
                return builder.makeCall(type, SqlStdOperatorTable.COALESCE,
                        List.of(expression, builder.makeExactLiteral(java.math.BigDecimal.ZERO, type)));
            }
            case "call" -> {
                fields(wire, "kind", "expression", "arguments");
                var expression = this.originalExpression(wire.path("expression"));
                check(expression.getClass() == RexCall.class, "Call recipe requires an ordinary RexCall");
                var call = (RexCall) expression;
                var arguments = this.expressions(wire.path("arguments"), row, input, depth + 1);
                check(arguments.size() == call.operands.size(), "Call arity differs");
                for (int index = 0; index < arguments.size(); index++)
                    check(arguments.get(index).getType().equals(call.operands.get(index).getType()),
                            "Call argument type differs");
                return call.clone(call.getType(), arguments);
            }
            case "field" -> {
                fields(wire, "kind", "expression", "input");
                var original = this.originalExpression(wire.path("expression"));
                check(original instanceof RexFieldAccess, "Field recipe requires a field access");
                var field = (RexFieldAccess) original;
                var reference = this.expression(wire.path("input"), row, input, depth + 1);
                check(reference.getType().equals(field.getReferenceExpr().getType()), "Field base type differs");
                var result = input.getCluster().getRexBuilder().makeFieldAccess(reference, field.getField().getIndex());
                check(result.getType().equals(field.getType()), "Field result type differs");
                return result;
            }
            default -> throw new IOException("Unknown correlation recipe expression");
        }
    }
}
