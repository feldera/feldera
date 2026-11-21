package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MergeGC;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.CheckPersistentIds;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.HashPrefixPass;
import org.dbsp.sqlCompiler.compiler.visitors.outer.merkle.SetOperatorHashes;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.FindMapChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing.CollapseSharedChains;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.FindWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.ImplementWindowGroups;
import org.dbsp.sqlCompiler.compiler.visitors.outer.windowSharing.WindowGroupOracle;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ConstantViews;
import org.dbsp.sqlCompiler.compiler.visitors.unusedFields.InputFieldOracle;
import org.dbsp.sqlCompiler.compiler.visitors.outer.AppendOnly;
import org.dbsp.sqlCompiler.compiler.visitors.outer.LowerAsof;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FindUnboundedState;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import java.util.Comparator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitStatistics;
import org.dbsp.sqlCompiler.compiler.visitors.outer.TailStatisticsOracle;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ExpandAggregateZero;
import org.dbsp.sqlCompiler.compiler.visitors.outer.ExpandAggregates;
import org.dbsp.sqlCompiler.compiler.visitors.outer.OptimizeWithGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.OrderErrorConsumers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveTable;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveUselessLeftJoinsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveViewOperators;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Shuffle;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.Objects;

/** Record every invocation of the selected pass, including repeat iterations. */
public final class PassCorpusOracle extends FrozenCircuitObserver {
    private static final boolean STREAM = Boolean.getBoolean("feldera.optimizer.streamInvocations");
    private int streamed;
    private int messagesAtEntry;
    @Nullable private ArrayNode diagnostics;
    private final String passClass;
    private final ArrayNode invocations;
    private final DBSPCompiler compiler;
    private final IdentityHashMap<CircuitTransform, Integer> instances = new IdentityHashMap<>();
    @Nullable private ObjectNode active;

    private PassCorpusOracle(DBSPCompiler compiler, ObjectNode record, String passClass) {
        super(compiler, record);
        this.passClass = passClass;
        this.compiler = compiler;
        record.put("pass", passClass);
        this.invocations = record.putArray("invocations");
    }

    private static void writePorts(ObjectNode row, String name, java.util.Set<OutputPort> ports) {
        var array = row.putArray(name);
        ports.stream().sorted(Comparator.comparingLong((OutputPort p) -> p.operator.id)
                .thenComparingInt(p -> p.outputNumber))
                .forEach(p -> array.addArray().add(p.operator.id).add(p.outputNumber));
    }

    private boolean selected(CircuitTransform pass) {
        if (this.passClass.equals("ExpandSafeCasts") || this.passClass.equals("ExpandUnsafeCasts")
                || this.passClass.equals("EnsureExpressionTrees") || this.passClass.equals("UDFInliner")
                || this.passClass.equals("SimplifyConditionals")
                || this.passClass.equals("Simplify") || this.passClass.equals("SimplifyWaterline"))
            return pass instanceof org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter
                    && pass.getName().equals(this.passClass);
        if (this.passClass.equals("NoConnectorMetadata") || this.passClass.equals("AdjustSqlIndex")
                || this.passClass.equals("CreateRuntimeErrorWrappers"))
            return pass.getName().equals(this.passClass);
        if (pass instanceof OptimizeWithGraph)
            return this.passClass.equals("OptimizeWithGraph(" + pass.getName() + ")");
        return pass.getClass().getSimpleName().equals(this.passClass);
    }

    @Override
    void diagnostic(org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange range, boolean warning,
                    boolean continuation, String type, String message) {
        if (this.diagnostics != null) {
            var emitted = this.diagnostics.addObject().put("error_type", type).put("message", message)
                    .put("warning", warning).put("continuation", continuation);
            emitted.set("range", range.asJson());
        }
    }

    @Override
    public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
        if (!this.selected(pass))
            return;
        if (this.active != null)
            throw new IllegalStateException("Overlapping invocations of " + this.passClass);
        this.active = STREAM ? this.observation.objectNode() : this.invocations.addObject();
        this.active.put("container", container).put("index", index).put("iteration", iteration);
        this.active.put("instance", this.instances.computeIfAbsent(pass, ignored -> this.instances.size()));
        this.active.put("names_before", DBSPVariablePath.nextGeneratedName());
        var writer = this.snapshotWriter(circuit);
        this.active.set("before", this.snapshot(writer));
        if (pass instanceof LowerAsof.OrderGcDependencies order)
            this.active.set("extra_edges", org.dbsp.util.Utilities.deterministicObjectMapper()
                    .valueToTree(order.edgeIds()));
        if (pass instanceof LowerAsof.MoveGC move)
            this.active.set("asof_joins", org.dbsp.util.Utilities.deterministicObjectMapper()
                    .valueToTree(move.retentionJoins()));
        if (pass instanceof org.dbsp.sqlCompiler.compiler.visitors.unusedFields.ReplaceCommonProjections replace) {
            var state = org.dbsp.sqlCompiler.compiler.backend.SharedProjectionState.encode(writer, replace.analysis());
            try {
                this.active.set("projection_closures", org.dbsp.util.Utilities.deterministicObjectMapper().readTree(state));
            } catch (java.io.IOException error) {
                throw new java.io.UncheckedIOException(error);
            }
        }
        if (pass instanceof org.dbsp.sqlCompiler.compiler.visitors.unusedFields.UnusedFields.TrimInputs trim)
            this.active.set("input_layouts", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(trim.layouts()));
        if (this.passClass.equals("FindUnusedInputFields"))
            InputFieldOracle.capture(this.compiler, circuit, this.active.putArray("input_fields"));
        if (this.passClass.equals("IndexedInputs")) {
            this.active.put("dedup_keys", this.compiler.options.ioOptions.gen2);
            this.active.put("lazy_upsert", this.compiler.metadata.lazyUpsert());
        }
        if (this.passClass.equals("ExpandStreamAggregates") || this.passClass.equals("Simplify") || this.passClass.equals("PropagateConstants")
                || this.passClass.equals("PullFilterVisitor") || this.passClass.equals("OptimizeProjectionVisitor"))
            this.diagnostics = this.active.putArray("diagnostics");
        if (pass instanceof ExpandAggregates.ExpandStreamAggregates expand) {
            this.active.set("append_only_inputs", org.dbsp.util.Utilities.deterministicObjectMapper()
                    .valueToTree(expand.appendOnlyInputs()));
            this.active.put("weight_id", this.compiler.weightVar.getId());
            this.active.put("weight_name", this.compiler.weightVar.variable);
        }
        if (pass instanceof MergeGC.FindMultipleRetains groups)
            this.active.set("previously_grouped", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(groups.groupedIds()));
        if (pass instanceof HashPrefixPass prefix)
            this.active.set("prefix_inputs", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(prefix.inputs()));
        if (pass instanceof CheckPersistentIds)
            this.messagesAtEntry = this.compiler.messages.messages.size();
        if (pass instanceof SetOperatorHashes set)
            this.active.set("computed_hashes", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(set.computedHashes()));
        if (pass instanceof MergeGC.MergeRetain merge)
            this.active.set("retention_groups", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(merge.memberIds()));
        if (pass instanceof MergeGC.MergeNoops merge)
            this.active.set("shared_traces", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(merge.sharedInputs()));
        if (pass instanceof FindUnboundedState.FindBounded bounded) {
            var context = this.active.putObject("bound_inputs");
            writePorts(context, "known", bounded.boundedPorts());
            writePorts(context, "keys", bounded.indexedKeys(circuit));
            context.put("now_table", DBSPCompiler.NOW_TABLE_NAME.name());
        }
        if (pass instanceof FindUnboundedState.CollectUnbounded collect)
            writePorts(this.active, "bounded", collect.boundedPorts());
        if (pass instanceof CollapseSharedChains chains) {
            var ordered = this.active.putArray("map_chains");
            for (var ids : chains.chainIds()) {
                var list = ordered.addArray();
                ids.forEach(list::add);
            }
        }
        if (pass instanceof ImplementWindowGroups implementation) {
            var groups = this.active.putArray("groups");
            for (var members : implementation.projections()) {
                var group = groups.addArray();
                for (var member : members) {
                    var fields = group.addObject().put("window", member.window()).putArray("fields");
                    for (var field : member.fields())
                        fields.add(field);
                }
            }
        }
        if (pass instanceof org.dbsp.sqlCompiler.compiler.visitors.outer.EnsureDistinctOutputs distinct) {
            var sinks = this.active.putArray("multiset_sinks");
            distinct.multisetSinks(circuit).forEach(sinks::add);
        }
        if (pass instanceof RemoveUselessLeftJoinsVisitor joins) {
            var keyed = this.active.putArray("keyed_left_joins");
            joins.keyedLeftJoins(circuit).forEach(keyed::add);
        }
        if (pass instanceof ConstantViews) {
            var views = this.active.putArray("constant_views");
            this.active.put("error_view", DBSPCompiler.ERROR_VIEW_NAME.name());
            new ConstantViews(this.compiler) {
                @Override
                protected void constantView(DBSPSinkOperator sink) {
                    views.add(sink.id);
                }
            }.apply(circuit);
        }
        if (pass instanceof ExpandAggregateZero) {
            var order = this.active.putArray("nested_hoist_order");
            ExpandAggregateZero.nestedHoistOrder(this.compiler, circuit).forEach(order::add);
        }
        if (pass instanceof OrderErrorConsumers)
            this.active.put("error_table", DBSPCompiler.ERROR_TABLE_NAME.name());
        if (pass instanceof RemoveTable table)
            this.active.put("table_name", table.tableName().name());
        if (pass instanceof DeadCode dead)
            this.active.put("keep_sources", dead.keepAllSources);
        if (pass instanceof RemoveViewOperators views) {
            this.active.put("remove_all", views.removesAllViews());
            this.active.put("error_view", DBSPCompiler.ERROR_VIEW_NAME.name());
        }
        if (this.passClass.equals("CSE")) {
            var shuffles = this.active.putObject("flatmap_shuffles");
            var identities = new IdentityHashMap<Shuffle, Integer>();
            new InnerVisitor(this.compiler) {
                @Override
                public void postorder(DBSPFlatmap flatmap) {
                    shuffles.put(Long.toString(flatmap.id),
                            identities.computeIfAbsent(flatmap.shuffle, ignored -> identities.size()));
                }
            }.getCircuitVisitor(true).apply(circuit);
        }
    }

    @Override
    public void after(String container, int index, int iteration, CircuitTransform pass,
                      DBSPCircuit circuit, boolean completed) {
        if (!this.selected(pass))
            return;
        if (!completed || circuit == null)
            throw new IllegalStateException("Incomplete " + this.passClass);
        var current = Objects.requireNonNull(this.active);
        this.diagnostics = null;
        if (pass instanceof HashPrefixPass prefix)
            current.set("prefixes", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(prefix.prefixesById()));
        if (pass instanceof org.dbsp.sqlCompiler.compiler.visitors.unusedFields.FindCommonProjections common)
            InputFieldOracle.common(this.compiler, circuit, common, current.putArray("shared_projections"));
        if (pass instanceof CheckPersistentIds) {
            var warnings = current.putArray("identical_view_warnings");
            var messages = this.compiler.messages.messages;
            for (var message : messages.subList(this.messagesAtEntry, messages.size())) {
                if (!message.warning || !message.errorType.equals("Identical views"))
                    throw new IllegalStateException("Unexpected persistent-ID diagnostic " + message);
                warnings.add(message.message);
            }
        }
        if (pass instanceof MergeGC.FindEquivalentNoops noops)
            current.set("shared_traces", org.dbsp.util.Utilities.deterministicObjectMapper().valueToTree(noops.sharedInputs()));
        if (pass instanceof MergeGC.FindMultipleRetains groups) {
            var mapper = org.dbsp.util.Utilities.deterministicObjectMapper();
            current.set("grouped", mapper.valueToTree(groups.groupedIds()));
            current.set("retention_groups", mapper.valueToTree(groups.memberIds()));
        }
        if (pass instanceof org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.CircuitContainsNow clock)
            current.put("uses_clock", clock.found());
        if (pass instanceof FindUnboundedState.CollectUnbounded collect) {
            var findings = current.putArray("unbounded");
            for (var finding : collect.findings()) {
                var inputs = findings.addObject().put("operator", finding.operator().id).putArray("inputs");
                finding.unboundedInputs().forEach(inputs::add);
            }
        }
        if (pass instanceof FindUnboundedState.DetectStreamingOperations streaming)
            current.put("streaming", streaming.isStreaming());
        if (pass instanceof FindUnboundedState.FindGCedStreams gc)
            writePorts(current, "pruned", gc.prunedStreams());
        if (pass instanceof FindUnboundedState.FindBounded bounded)
            writePorts(current, "bounded", bounded.boundedPorts());
        if (pass instanceof AppendOnly analysis) {
            var streams = current.putArray("append_only");
            analysis.appendOnly.stream().mapToLong(port -> {
                if (port.outputNumber != 0)
                    throw new IllegalStateException("Unexpected secondary append-only output");
                return port.operator.id;
            }).sorted().forEach(streams::add);
        }
        if (pass instanceof FindWindowGroups find)
            WindowGroupOracle.capture(this.compiler, circuit, find, current);
        if (pass instanceof FindMapChains find) {
            var chains = current.putArray("map_chains");
            for (var ids : find.chainIds()) {
                var chain = chains.addArray();
                ids.forEach(chain::add);
            }
        }
        if (pass instanceof CircuitStatistics stats)
            current.set("statistics", TailStatisticsOracle.counters(stats));
        if (pass instanceof OrderErrorConsumers order) {
            var reached = current.putArray("error_consumers");
            circuit.allOperators.stream().filter(order.reachable()::contains).forEach(op -> reached.add(op.id));
        }
        current.put("names_after", DBSPVariablePath.nextGeneratedName());
        current.set("after", this.snapshot(circuit));
        if (STREAM) {
            ObjectNode chunk = this.observation.objectNode();
            chunk.set("id", this.observation.required("id"));
            chunk.set("input_sha256", this.observation.required("input_sha256"));
            chunk.put("pass", this.passClass).put("index", this.streamed++);
            chunk.set("invocation", current);
            System.out.println(chunk);
        }
        this.active = null;
    }

    @Override
    public void inPlace(String container, int index, String operation, DBSPCircuit circuit) {
        if (this.passClass.equals("LinearPostprocessRetainKeys") && container.equals(this.passClass)
                && operation.equals("OrderWaterlineDependencies")) {
            // The in-place operation changes only the body's order. Keep the original
            // operator identities so a replacement or an omission is also observable.
            var order = Objects.requireNonNull(this.active).putArray("waterline_order");
            circuit.allOperators.forEach(operator -> order.add(operator.id));
        }
    }

    @Override
    void finish() {
        if (STREAM)
            this.observation.put("streamed_invocations", this.streamed);
        if (this.active != null)
            throw new IllegalStateException("Missing pass output");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("Expected POPULATION.jsonl and a pass class");
        run(Path.of(args[0]), (compiler, record) -> new PassCorpusOracle(compiler, record, args[1]));
    }
}
