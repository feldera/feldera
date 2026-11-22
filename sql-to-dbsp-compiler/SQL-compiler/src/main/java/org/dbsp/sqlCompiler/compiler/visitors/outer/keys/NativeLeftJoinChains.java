package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/** Reconnect Rust's chain schedule to the live row functions. Only operators
 * crossed by a chain need expression analysis. */
public final class NativeLeftJoinChains {
    public enum Kind { Unary, Differentiate, Integrate, LeftJoin }
    public record Operator(long operator, @Nullable Long input, Kind kind, boolean collectionInput) {}
    public record Step(long end, @Nullable Long previous, List<Long> via, boolean candidate, boolean terminal) {}
    public record LinkColumns(long operator, boolean join, @Nullable List<Column> seed,
                              @Nullable Set<Column> reads, List<DeferCarriedColumns.ColumnCopy> copies) {}
    public record Inputs(List<Step> steps, List<LinkColumns> links, boolean logging) {}
    public record Carried(long seed, long end, int length, SortedSet<Column> read,
                          List<DeferCarriedColumns.ColumnCopy> copies) {}

    private final LeftJoinChainsVisitor visitor;
    private final List<Operator> graph = new ArrayList<>();
    private final Map<Long, DBSPSimpleOperator> live = new HashMap<>();

    NativeLeftJoinChains(LeftJoinChainsVisitor visitor) {
        this.visitor = visitor;
    }

    private void collect(ICircuit circuit) {
        for (var op : circuit.getAllOperators()) {
            if (op instanceof DBSPNestedOperator nested) {
                this.collect(nested);
                continue;
            }
            if (!(op instanceof DBSPSimpleOperator simple))
                continue;
            Kind kind;
            OutputPort input;
            if (LeftJoinChainsVisitor.isLeftJoin(op)) {
                var closure = KeyAnalysis.closureOf(simple);
                if (simple.outputPort().getShape() == null || closure == null || closure.parameters.length != 3)
                    continue;
                kind = Kind.LeftJoin;
                input = simple.inputs.get(0);
            } else if (op instanceof DBSPUnaryOperator) {
                var source = this.visitor.sourceTransform(simple.outputPort());
                if (source == null)
                    continue;
                input = source.port();
                if (op instanceof DBSPDifferentiateOperator)
                    kind = Kind.Differentiate;
                else if (op instanceof DBSPIntegrateOperator)
                    kind = Kind.Integrate;
                else
                    kind = Kind.Unary;
            } else {
                continue;
            }
            this.live.put(op.id, simple);
            this.graph.add(new Operator(op.id, input.outputNumber == 0 ? input.operator.id : null,
                    kind, kind != Kind.LeftJoin && input.kind() == StreamKind.COLLECTION));
        }
    }

    boolean analyze(DBSPCircuit circuit) {
        this.collect(circuit);
        var steps = RustCircuitPass.chainSteps(this.visitor.compiler(), this.graph);
        if (steps == null)
            return false;
        if (steps.isEmpty())
            return true;
        var selected = new LinkedHashSet<Long>();
        var seeds = new HashSet<Long>();
        var terminals = new HashSet<Long>();
        for (var step : steps) {
            if (step.previous() == null)
                seeds.add(step.via().get(0));
            selected.addAll(step.via());
            selected.add(step.end());
            if (step.terminal())
                terminals.add(step.end());
        }
        var links = new ArrayList<LinkColumns>(selected.size());
        for (var id : selected) {
            var columns = this.visitor.linkColumns(this.live.get(id), seeds.contains(id));
            if (columns == null) {
                var observer = this.visitor.compiler().stateObserver;
                if (observer != null)
                    observer.nativeDecision("LeftJoinChains/Columns", false);
                return false;
            }
            links.add(columns);
        }
        boolean logging = Logger.INSTANCE.getLoggingLevel(LeftJoinChainsVisitor.class) >= 1;
        for (var chain : RustCircuitPass.carriedColumns(this.visitor.compiler(), new Inputs(steps, links, logging))) {
            var start = this.live.get(chain.seed()).inputs.get(0);
            var end = this.live.get(chain.end()).outputPort();
            this.visitor.install(start, end, chain, terminals.contains(chain.end()));
        }
        return true;
    }
}
