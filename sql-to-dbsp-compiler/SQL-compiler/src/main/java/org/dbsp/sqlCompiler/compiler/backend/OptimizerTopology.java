package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeltaOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPBinaryDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateLinearPostprocessRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPrimitiveAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.circuit.operator.IInputOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIncrementalConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.visitors.outer.PropagateEmptySources;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.JsonStream;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Set;

/** Circuit projection for passes that read only operator kinds, edges and annotations. */
final class OptimizerTopology {
    enum Details { NONE, TABLE_NAMES, VIEW_NAMES, ERROR_CONSUMERS, EMPTY_LITERALS, FILTER_PREDICATES, COLUMN_INITIALIZERS, PRIMARY_KEYS, SINK_KIND, RECURSIVE_VIEWS, TRACE_RETENTION, TRACE_INTEGRATORS, GC_GROUPS, SHARED_INDEXES, PERSISTENT_IDS, CONSUMERS, TABLE_CHANGES, STREAMING, STATE_BOUNDS, AGGREGATE_RESULTS }

    private OptimizerTopology() {}

    /** Each body contains its ID and its ordered operators. An operator carries its ID,
     * ordered producer IDs, and an optional role for recursive feedback. */
    static byte[] consumerInputs(DBSPCircuit circuit) {
        var json = JsonStream.compactCircuit();
        json.beginArray();
        writeConsumerBody(json, circuit);
        json.endArray();
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void writeConsumerBody(JsonStream json, ICircuit body) {
        json.beginArray();
        json.append(body.getId()).beginArray();
        for (var op : body.getAllOperators()) {
            json.beginArray();
            json.append(op.id).beginArray();
            for (var input : op.inputs)
                json.append(input.operator.id);
            json.endArray();
            if (op instanceof DBSPDeltaOperator)
                json.append("delta");
            else if (op instanceof DBSPViewDeclarationOperator decl)
                json.beginObject().label("declaration").append(decl.tableName.name()).endObject();
            else if (op instanceof DBSPViewOperator view)
                json.beginObject().label("view").append(view.viewName.name()).endObject();
            json.endArray();
        }
        json.endArray().endArray();
        for (var op : body.getAllOperators())
            if (op instanceof DBSPNestedOperator nested)
                writeConsumerBody(json, nested);
    }

    static byte[] encode(DBSPCircuit circuit) {
        return encode(circuit, Details.NONE);
    }

    static byte[] encode(DBSPCircuit circuit, Details details) {
        return encode(circuit, details, null);
    }

    static byte[] stateBounds(DBSPCompiler compiler, DBSPCircuit circuit) {
        return encode(circuit, Details.STATE_BOUNDS, compiler);
    }

    static byte[] statistics(DBSPCompiler compiler, DBSPCircuit circuit) {
        return encode(circuit, Details.NONE, compiler);
    }

    private static byte[] encode(DBSPCircuit circuit, Details details, @Nullable DBSPCompiler typeContext) {
        var json = JsonStream.compactCircuit();
        json.beginObject().appendClass(circuit).label("id").append(circuit.id);
        if (details == Details.STATE_BOUNDS)
            json.label("incremental").append(circuit.incremental);
        json.label("declarations").beginArray();
        json.endArray();
        json.label("allOperators");
        var types = typeContext == null ? null : new ToJsonInnerVisitor(typeContext, json, 1);
        writeBody(json, circuit.getAllOperators(), details, types, null);
        json.endObject();
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Include expression payloads only for operators Rust will compare.
     * Inputs remain references into the enclosing topology. */
    private record SelectedPayloads(Set<Long> selected, ToJsonOuterVisitor writer) {
        boolean write(DBSPOperator op) {
            if (!this.selected.contains(op.id))
                return false;
            for (var input : op.inputs)
                this.writer.serialized.add(input.operator.id);
            this.writer.serialized.remove(op.id);
            op.accept(this.writer);
            return true;
        }
    }

    /** Analyses return facts about existing IDs. They need expression definitions and
     * flatmap shuffle identities. Source positions and replay metadata belong to the
     * full snapshot used when a pass returns new IR. */
    static ToJsonOuterVisitor selectedExpressions(DBSPCompiler compiler, DBSPCircuit circuit,
                                                  Set<Long> selected, Details details) {
        return selectedExpressions(compiler, circuit, selected, details,
                new ToJsonInnerVisitor(compiler, JsonStream.compactCircuit(), 0));
    }

    /** Keep source ranges and object identities when a pass returns rewritten closures. */
    static ToJsonOuterVisitor selectedForRewrite(DBSPCompiler compiler, DBSPCircuit circuit,
                                                 Set<Long> selected, Details details) {
        var inner = new ToJsonInnerVisitor(compiler, JsonStream.compactCircuit(), 1, true);
        inner.liveNodes = new HashMap<>();
        return selectedExpressions(compiler, circuit, selected, details, inner);
    }

    private static ToJsonOuterVisitor selectedExpressions(DBSPCompiler compiler, DBSPCircuit circuit,
                                                          Set<Long> selected, Details details, ToJsonInnerVisitor inner) {
        var json = inner.stream;
        inner.liveShuffles = new HashMap<>();
        var writer = new ToJsonOuterVisitor(compiler, 0, inner);
        json.beginObject().appendClass(circuit).label("id").append(circuit.id);
        json.label("declarations").beginArray();
        json.endArray().label("allOperators");
        writeBody(json, circuit.getAllOperators(), details, null, new SelectedPayloads(selected, writer));
        json.endObject();
        return writer;
    }

    private static void writeBody(JsonStream json, Iterable<DBSPOperator> operators, Details details,
                                  @Nullable ToJsonInnerVisitor types, @Nullable SelectedPayloads payloads) {
        json.beginArray();
        for (var op : operators) {
            if (payloads != null && payloads.write(op))
                continue;
            json.beginObject().appendClass(op).label("id").append(op.id);
            if (details == Details.TABLE_NAMES && op instanceof DBSPSourceMultisetOperator table) {
                json.label("tableName").beginObject().label("name").append(table.tableName.name()).endObject();
            }
            if (details == Details.ERROR_CONSUMERS) {
                if (op instanceof IInputOperator input)
                    json.label("tableName").beginObject().label("name").append(input.getTableName().name()).endObject();
                if (op instanceof DBSPViewOperator view)
                    json.label("viewName").beginObject().label("name").append(view.viewName.name()).endObject();
            }
            if (details == Details.VIEW_NAMES && op instanceof DBSPSinkOperator sink)
                json.label("viewName").beginObject().label("name").append(sink.viewName.name()).endObject();
            if (details == Details.RECURSIVE_VIEWS || details == Details.CONSUMERS || details == Details.GC_GROUPS || details == Details.SHARED_INDEXES) {
                if (op instanceof DBSPViewDeclarationOperator decl)
                    json.label("tableName").beginObject().label("name").append(decl.tableName.name()).endObject();
                if (op instanceof DBSPViewOperator view) {
                    json.label("viewName").beginObject().label("name").append(view.viewName.name()).endObject();
                    if (details == Details.RECURSIVE_VIEWS)
                        json.label("metadata").beginObject().label("recursive").append(view.metadata.recursive)
                            .label("viewKind").append(view.metadata.viewKind.name()).endObject();
                }
            }
            if (details == Details.FILTER_PREDICATES && op instanceof DBSPFilterOperator filter)
                json.label("literalPredicate").append(filter.getClosureFunction().body instanceof DBSPBoolLiteral);
            if (details == Details.AGGREGATE_RESULTS && op instanceof DBSPAggregateOperatorBase aggregate)
                json.label("hasArrayResult").append(aggregate.aggregateList != null &&
                        aggregate.aggregateList.aggregates.stream().anyMatch(a -> a.getType() instanceof DBSPTypeArray));
            if (details == Details.TRACE_INTEGRATORS) {
                if (op instanceof DBSPSourceTableOperator table)
                    json.label("metadata").beginObject().label("materialized").append(table.metadata.materialized).endObject();
            }
            if (details == Details.STREAMING) {
                if (op instanceof DBSPSourceTableOperator table) {
                    json.label("metadata").beginObject().label("changes")
                            .append(table.metadata.isAppendOnly() ? "AppendOnly" : "Unrestricted");
                    json.label("columnMetadata").beginArray();
                    for (var column : table.metadata.getColumns())
                        if (column.lateness != null)
                            json.beginObject().label("lateness").append(true).endObject();
                    json.endArray().endObject();
                } else if (op instanceof DBSPViewBaseOperator view) {
                    json.label("metadata").beginObject().label("columns").beginArray();
                    if (view.metadata.hasLateness())
                        json.beginObject().label("lateness").append(true).endObject();
                    json.endArray().endObject();
                }
            }
            if (details == Details.TABLE_CHANGES && op instanceof DBSPSourceTableOperator table)
                json.label("metadata").beginObject().label("changes")
                        .append(table.metadata.isAppendOnly() ? "AppendOnly" : "Unrestricted").endObject();
            if ((details == Details.TRACE_INTEGRATORS || details == Details.SINK_KIND || details == Details.GC_GROUPS || details == Details.SHARED_INDEXES) && op instanceof DBSPSinkOperator sink)
                json.label("metadata").beginObject().label("viewKind").append(sink.metadata.viewKind.name()).endObject();
            if ((details == Details.TRACE_RETENTION || details == Details.GC_GROUPS) && op instanceof DBSPIntegrateTraceRetainKeysOperator retainer)
                json.label("accumulate").append(retainer.accumulate);
            if (details == Details.PERSISTENT_IDS && op instanceof DBSPSinkOperator sink)
                json.label("metadata").beginObject().label("system").append(sink.metadata.system).endObject();
            if (details == Details.GC_GROUPS && op instanceof DBSPWindowOperator window)
                json.label("lowerUnbounded").append(window.lowerUnbounded);
            if (details == Details.COLUMN_INITIALIZERS) {
                TableMetadata metadata = op instanceof DBSPSourceTableOperator table ? table.metadata
                        : op instanceof DBSPInputMapWithWaterlineOperator input ? input.metadata : null;
                if (metadata != null)
                    json.label("hasInitializers").append(metadata.getColumns().stream()
                            .anyMatch(column -> column.defaultValue != null || column.lateness != null));
            }
            if (details == Details.PRIMARY_KEYS && op instanceof DBSPSourceMultisetOperator source)
                json.label("hasPrimaryKey").append(source.metadata.getColumns().stream().anyMatch(column -> column.isPrimaryKey));
            if (details == Details.EMPTY_LITERALS) {
                if (op instanceof DBSPConstantOperator constant)
                    json.label("emptyLiteral").append(PropagateEmptySources.isEmpty(constant.getFunction()));
                else if (op instanceof DBSPIncrementalConstantOperator constant)
                    json.label("emptyLiteral").append(PropagateEmptySources.isEmpty(constant.getFunction()));
            }
            if (details == Details.STATE_BOUNDS) {
                if (op instanceof DBSPSourceTableOperator table) {
                    json.label("tableName").beginObject().label("name").append(table.tableName.name()).endObject();
                    json.label("metadata").beginObject();
                    if (table.metadata.expectedSize != null)
                        json.label("expected_size").append(table.metadata.expectedSize.longValue());
                    json.endObject();
                }
                if (op instanceof DBSPViewOperator view)
                    json.label("viewName").beginObject().label("name").append(view.viewName.name()).endObject();
                if (op instanceof DBSPNestedOperator nested) {
                    json.label("outputViews").beginArray();
                    for (var name : nested.outputViews)
                        json.beginObject().label("name").append(name.name()).endObject();
                    json.endArray();
                }
                if (op instanceof DBSPIndexedTopKOperator top)
                    json.label("literalLimit").append(top.limit instanceof DBSPUSizeLiteral);
                if (op instanceof DBSPWindowOperator window)
                    json.label("lowerUnbounded").append(window.lowerUnbounded);
                if (op instanceof DBSPDistinctOperator || op instanceof DBSPBinaryDistinctOperator
                        || op instanceof DBSPAggregateLinearPostprocessOperator || op instanceof DBSPChainAggregateOperator
                        || op instanceof DBSPAggregateLinearPostprocessRetainKeysOperator || op instanceof DBSPPrimitiveAggregateOperator
                        || op instanceof DBSPAggregateOperatorBase || op instanceof DBSPIndexedTopKOperator) {
                    json.label("domainInputType");
                    op.inputs.get(0).outputType().accept(java.util.Objects.requireNonNull(types));
                }
            } else if (types != null && !(op instanceof DBSPNestedOperator)) {
                json.label("statisticsTypes").beginArray();
                for (int i = 0; i < op.outputCount(); i++)
                    op.outputType(i).accept(types);
                json.endArray();
            }
            json.label("annotations");
            op.annotations.asJson(json, true);
            json.label("inputs").beginArray();
            for (var port : op.inputs)
                writePort(json, port);
            json.endArray();
            if (op instanceof DBSPNestedOperator nested) {
                json.label("allOperators");
                writeBody(json, nested.getAllOperators(), details, types, payloads);
                json.label("internalOutputs").beginArray();
                for (var port : nested.internalOutputs)
                    writePort(json, port);
                json.endArray();
            }
            json.endObject();
        }
        json.endArray();
    }

    private static void writePort(JsonStream json, @Nullable OutputPort port) {
        if (port == null) {
            json.appendNull();
            return;
        }
        json.beginObject().label("outputNumber").append(port.outputNumber);
        json.label("operator").beginObject().label("node").append(port.operator.id);
        json.endObject();
        json.endObject();
    }
}
