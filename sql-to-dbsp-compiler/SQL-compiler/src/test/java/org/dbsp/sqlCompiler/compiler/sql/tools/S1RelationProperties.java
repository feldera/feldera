package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.util.ImmutableBitSet;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Explicit operator properties, independent of expression payloads. The Rust
 * reader checks field ranges against the producer rows. This is not a catalog,
 * trait or hint adapter, nor does it reconstruct complete relational nodes. */
final class S1RelationProperties {
    private S1RelationProperties() { }
    @FunctionalInterface private interface OperatorId {
        int id(org.apache.calcite.sql.SqlOperator operator) throws IOException;
    }

    static List<RelDataType> additionalTypes(List<RelNode> relations) {
        var types = new ArrayList<RelDataType>();
        for (var relation: relations) {
            if (relation instanceof Aggregate aggregate)
                aggregate.getAggCallList().forEach(call -> types.add(call.getType()));
            if (relation instanceof Join join)
                join.getSystemFieldList().forEach(field -> types.add(field.getType()));
        }
        return types;
    }

    static ObjectNode encode(S1ExpressionSharingProbe.References captured, S1InputUsesTable.Encoded encoded,
                             S1BuiltinOperatorRegistry registry) throws IOException {
        var types = encoded.types().ids();
        var result = object().put("schema_version", "relation-properties-v1");
        var relations = result.putArray("relations");
        var operators = new S1OperatorObjects(registry, S1ExpressionSources.of(null));
        for (var relation: captured.relationNodes) {
            var entry = relations.addObject();
            entry.set("fields", encode(relation, types, operators::add));
            entry.set("metadata", S1RelationMetadata.encode(relation));
        }
        result.putObject("operators").put("kind", "owned").set("objects", operators.encode(types, java.util.Map.of()));
        var decodedTypes = S1TypeTable.decodeWithSources(encoded.types().table()).types();
        var decoded = S1OperatorObjects.decode(result.at("/operators/objects"), decodedTypes, S1ExpressionSources.of(null), registry);
        operators.verify(decoded.objects(), types, decodedTypes, java.util.Map.of(), List.of());
        return result;
    }

    static ObjectNode encodeShared(S1ExpressionSharingProbe.References captured, S1InputUsesTable.Encoded encoded,
                                   java.util.Map<org.apache.calcite.sql.SqlOperator, Integer> operators) throws IOException {
        var result = object().put("schema_version", "relation-properties-v1");
        result.putObject("operators").put("kind", "shared");
        var relations = result.putArray("relations");
        for (var relation: captured.relationNodes) {
            var entry = relations.addObject();
            entry.set("fields", encode(relation, encoded.types().ids(), operator -> {
                Integer id = operators.get(operator);
                if (id == null) throw new IOException("Aggregate operator is outside the shared arena");
                return id;
            }));
            entry.set("metadata", S1RelationMetadata.encode(relation));
        }
        return result;
    }

    static void validateOperators(com.fasterxml.jackson.databind.JsonNode properties,
                                  com.fasterxml.jackson.databind.JsonNode table,
                                  S1BuiltinOperatorRegistry registry) throws IOException {
        var types = S1TypeTable.decodeWithSources(table.path("type_table")).types();
        var decoded = S1OperatorObjects.decode(properties.at("/operators/objects"), types,
                S1ExpressionSources.of(null), registry);
        for (var operator: decoded.objects())
            if (!(operator instanceof org.apache.calcite.sql.SqlAggFunction))
                throw new IOException("Relational aggregate requires an aggregate operator");
    }

    private static ObjectNode encode(RelNode relation, java.util.Map<RelDataType, Integer> types,
                                     OperatorId operators) throws IOException {
        var result = object();
        // The expression field walker has already checked the exact class.
        if (relation instanceof TableScan scan) {
            result.put("kind", "scan"); strings(result.putArray("name"), scan.getTable().getQualifiedName());
        } else if (relation instanceof Project project) {
            result.put("kind", "project").put("expressions", project.getProjects().size());
            variables(result.putArray("variables"), project.getVariablesSet());
        } else if (relation instanceof Filter filter) {
            result.put("kind", "filter"); variables(result.putArray("variables"), filter.getVariablesSet());
        } else if (relation instanceof Values values) {
            result.put("kind", "values").put("tuples", values.getTuples().size());
        } else if (relation instanceof SetOp set) {
            result.put("kind", "set").put("all", set.all);
        } else if (relation instanceof Join join) {
            result.put("kind", "join").put("join_kind", join.getJoinType().name())
                    .put("semi_join_done", join.isSemiJoinDone());
            variables(result.putArray("variables"), join.getVariablesSet());
            var fields = result.putArray("system_fields");
            join.getSystemFieldList().forEach(field -> fields.add(types.get(field.getType())));
        } else if (relation instanceof Aggregate aggregate) {
            result.put("kind", "aggregate"); bits(result.putArray("group"), aggregate.getGroupSet());
            var groups = result.putArray("groups");
            for (var group: aggregate.getGroupSets()) bits(groups.addArray(), group);
            var calls = result.putArray("calls");
            for (var call: aggregate.getAggCallList()) calls.add(aggregate(call, types, operators));
        } else if (relation instanceof Window window) {
            result.put("kind", "window"); var groups = result.putArray("groups");
            for (var group: window.groups) {
                var entry = groups.addObject().put("rows", group.isRows).put("lower", bound(group.lowerBound))
                        .put("upper", bound(group.upperBound)).put("exclusion", group.exclude.name())
                        .put("calls", group.aggCalls.size());
                bits(entry.putArray("keys"), group.keys); entry.set("collation", S1RelationMetadata.collation(group.orderKeys));
            }
        } else if (relation instanceof Sort sort) {
            result.put("kind", "sort").put("offset", sort.offset != null).put("fetch", sort.fetch != null);
            result.set("collation", S1RelationMetadata.collation(sort.collation));
        } else if (relation instanceof Uncollect uncollect) {
            result.put("kind", "uncollect").put("ordinality", uncollect.withOrdinality)
                    .put("outer", uncollect.isOuter).put("expand_struct_fields", uncollect.expandStructFields);
            strings(result.putArray("aliases"), uncollect.getItemAliases());
            bits(result.putArray("passthrough"), uncollect.getPassthroughFieldIndices());
            bits(result.putArray("collections"), uncollect.getCollectionFieldIndices());
        } else if (relation instanceof Collect collect) {
            result.put("kind", "collect").put("field", collect.getFieldName())
                    .put("collection", collect.getCollectionType().name());
        } else if (relation instanceof Correlate correlate) {
            result.put("kind", "correlate").put("correlation", correlate.getCorrelationId().getId())
                    .put("join_kind", correlate.getJoinType().name());
            bits(result.putArray("required"), correlate.getRequiredColumns());
        } else if (relation instanceof TableFunctionScan function) {
            result.put("kind", "table_function");
            var type = function.getElementType();
            if (type == null) result.put("element", "absent");
            else if (type == Object[].class) result.put("element", "object_array");
            else if (type == Object.class) result.put("element", "object");
            else throw new IOException("Unmodeled table-function element type: " + type);
            var mappings = result.putObject("mappings");
            if (function.getColumnMappings() == null) mappings.put("kind", "absent");
            else {
                mappings.put("kind", "present"); var entries = mappings.putArray("mappings");
                // Column mappings are a set. A deterministic order retains all
                // four fields; it does not sort any ordered relational inputs.
                var ordered = new ArrayList<>(function.getColumnMappings());
                ordered.sort(java.util.Comparator.comparingInt((org.apache.calcite.rel.metadata.RelColumnMapping m) -> m.iOutputColumn)
                        .thenComparingInt(m -> m.iInputRel).thenComparingInt(m -> m.iInputColumn).thenComparing(m -> m.derived));
                for (var mapping: ordered) entries.addObject().put("output", mapping.iOutputColumn)
                        .put("input", mapping.iInputRel).put("column", mapping.iInputColumn).put("derived", mapping.derived);
            }
        } else throw new IOException("Unmodeled relation properties: " + relation.getClass().getName());
        return result;
    }

    private static ObjectNode aggregate(AggregateCall call, java.util.Map<RelDataType, Integer> types,
                                        OperatorId operators) throws IOException {
        var result = object().put("operator", operators.id(call.getAggregation())).put("ty", types.get(call.getType()))
                .put("distinct", call.isDistinct()).put("approximate", call.isApproximate())
                .put("ignore_nulls", call.ignoreNulls()).put("direct_arguments", call.rexList.size());
        var name = result.putObject("name");
        if (call.getName() == null) name.put("kind", "absent");
        else name.put("kind", "present").put("name", call.getName());
        var arguments = result.putArray("arguments"); call.getArgList().forEach(arguments::add);
        var filter = result.putObject("filter");
        if (call.filterArg == -1) filter.put("kind", "absent");
        else filter.put("kind", "present").put("field", call.filterArg);
        var keys = result.putObject("distinct_keys");
        if (call.distinctKeys == null) keys.put("kind", "absent");
        else { keys.put("kind", "present"); bits(keys.putArray("fields"), call.distinctKeys); }
        result.set("collation", S1RelationMetadata.collation(call.collation)); result.set("position", S1RelationMetadata.position(call.getParserPosition()));
        return result;
    }

    private static String bound(RexWindowBound bound) throws IOException {
        if (bound.isCurrentRow()) return "current_row";
        if (bound.isPreceding()) return bound.isUnbounded() ? "unbounded_preceding" : "preceding";
        if (bound.isFollowing()) return bound.isUnbounded() ? "unbounded_following" : "following";
        throw new IOException("Unknown Window bound");
    }
    private static void variables(ArrayNode output, Set<CorrelationId> variables) {
        variables.stream().map(CorrelationId::getId).sorted().forEach(output::add);
    }
    private static void bits(ArrayNode output, ImmutableBitSet bits) { bits.forEach(output::add); }
    private static void strings(ArrayNode output, List<String> strings) { strings.forEach(output::add); }
    private static ObjectNode object() { return Utilities.deterministicObjectMapper().createObjectNode(); }
}
