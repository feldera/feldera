package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CanonicalForm;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Graph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;
import org.dbsp.util.Maybe;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/** Reorders the conjuncts within a filter.
 * The goal is to enable many temporal filters that share an input
 * and compare the same timestamp to be built later as Window
 * operators that share their left integral.  This pass does two things:
 * - analyzes temporal filters that may generate shareable integrals later
 * - when detecting large groups of such filters, it reorders them such that
 *   the shared temporal filter is implemented first, the non-temporal
 *   components next, and the remaining temporal filters last.
 * If a filter does not contain temporal filter conjuncts or does not
 * share a source with other similar filters, it is left alone. */
public class ReorderTemporalFilters extends Passes {
    /** Where the temporal conjuncts of a filter should be moved. */
    public enum FilterOrderPolicy {
        FIRST,
        LAST,
        KEEP
    }

    public ReorderTemporalFilters(DBSPCompiler compiler) {
        super("ReorderTemporalFilters", compiler);
        Graph graph = new Graph(compiler);
        this.add(graph);
        Scan scan = new Scan(compiler, graph.getGraphs());
        this.add(new Conditional(compiler, scan, () -> !compiler.metadata.windowSharingDisabled()));
        this.add(new Conditional(compiler, new Reorder(compiler, scan.chosenConjunct),
                () -> !compiler.metadata.windowSharingDisabled()));
    }

    /** Splits a tree of AND expressions into its operands. */
    static void conjuncts(DBSPExpression expression, List<DBSPExpression> out) {
        if (expression.is(DBSPBinaryExpression.class)) {
            DBSPBinaryExpression binary = expression.to(DBSPBinaryExpression.class);
            if (binary.opcode == DBSPOpcode.AND) {
                conjuncts(binary.left, out);
                conjuncts(binary.right, out);
                return;
            }
        }
        out.add(expression);
    }

    /** The WRAP_BOOL around `expression`, or null if there is none. */
    @Nullable
    static DBSPUnaryExpression wrapper(DBSPExpression expression) {
        if (expression.is(DBSPUnaryExpression.class)) {
            DBSPUnaryExpression unary = expression.to(DBSPUnaryExpression.class);
            if (unary.opcode == DBSPOpcode.WRAP_BOOL)
                return unary;
        }
        return null;
    }

    /** The top-level conjuncts of `condition`, with the WRAP_BOOL around it removed. */
    static List<DBSPExpression> topLevelConjuncts(DBSPClosureExpression condition) {
        DBSPUnaryExpression wrap = wrapper(condition.body);
        List<DBSPExpression> all = new ArrayList<>();
        conjuncts(wrap == null ? condition.body : wrap.source, all);
        return all;
    }

    /** AND a list of Boolean expressions */
    static DBSPExpression conjunction(List<DBSPExpression> operands) {
        if (operands.size() == 1)
            return operands.get(0);
        boolean nullable = false;
        for (DBSPExpression operand : operands)
            nullable |= operand.getType().mayBeNull;
        return ExpressionCompiler.makeBinaryExpressions(
                operands.get(0).getNode(), DBSPTypeBool.create(nullable), DBSPOpcode.AND, operands);
    }

    /**
     * The expression computing a temporal filter by which a window implementing `conjunct` would index its input,
     * or null if no window can implement `conjunct`.  This returns a result only
     * if the conjunct contains exactly 1 temporal filter.
     *
     * @param filter    Parent operator
     * @param parameter Parameter of the closure containing the conjunct
     * @param conjunct  A single conjunct of the filter's condition.
     */
    @Nullable
    public static DBSPExpression timestampExpression(DBSPCompiler compiler, DBSPFilterOperator filter,
                                                     DBSPParameter parameter, DBSPExpression conjunct) {
        DBSPClosureExpression single = conjunct.deepCopy().wrapBoolIfNeeded().closure(parameter)
                .ensureTree(compiler).to(DBSPClosureExpression.class);
        List<BooleanExpression> classified =
                FindComparisons.decomposeIntoTemporalFilters(compiler, filter, single);
        if (classified.size() != 1)
            return null;
        BooleanExpression only = classified.get(0);
        if (!only.is(TemporalFilter.class))
            return null;
        return only.to(TemporalFilter.class).noNow();
    }

    /**
     * Moves the conjuncts of `body` accepted by `movable` to the end named by `policy`, keeping their
     * relative order.  Returns `body` when nothing moves.
     *
     * @param body    Condition whose top-level conjuncts are reordered.
     * @param movable Selects the conjuncts to move.
     */
    public static DBSPExpression reorder(
            DBSPExpression body, FilterOrderPolicy policy, Predicate<DBSPExpression> movable) {
        if (policy == FilterOrderPolicy.KEEP)
            return body;
        DBSPUnaryExpression wrap = wrapper(body);
        DBSPExpression inner = wrap == null ? body : wrap.source;

        List<DBSPExpression> all = new ArrayList<>();
        conjuncts(inner, all);
        if (all.size() < 2)
            return body;

        List<DBSPExpression> moved = new ArrayList<>();
        List<DBSPExpression> rest = new ArrayList<>();
        for (DBSPExpression conjunct : all) {
            if (movable.test(conjunct))
                moved.add(conjunct);
            else
                rest.add(conjunct);
        }
        if (moved.isEmpty() || rest.isEmpty())
            return body;

        List<DBSPExpression> reordered = new ArrayList<>();
        if (policy == FilterOrderPolicy.FIRST) {
            reordered.addAll(moved);
            reordered.addAll(rest);
        } else {
            reordered.addAll(rest);
            reordered.addAll(moved);
        }
        if (reordered.equals(all))
            return body;

        DBSPExpression result = conjunction(reordered);
        if (wrap != null)
            result = new DBSPUnaryExpression(
                    wrap.getNode(), wrap.getType(), DBSPOpcode.WRAP_BOOL, result);
        return result;
    }

    /**
     * Orders the conjuncts of `body` for a filter whose window will share the input computed by
     * `first`: the conjuncts on `first`'s timestamp go first, the temporal conjuncts on other
     * timestamps go last, and the remaining conjuncts stay in between.
     *
     * <p>Only `first`'s timestamp is hoisted.  A shared window is evaluated before anything
     * view-specific, so hoisting the other temporal conjuncts too would make their windows
     * integrate the whole unfiltered stream; left last, they integrate what the non-temporal
     * conjuncts pass.  Conjuncts on the same timestamp stay adjacent because a single window
     * carries both of their bounds, as a BETWEEN's do.
     *
     * @param body        Condition whose top-level conjuncts are reordered.
     * @param timestampOf Names the timestamp a window would index a conjunct by, null if none.
     * @param first       Conjunct whose input is shared.
     */
    public static DBSPExpression reorderAroundSharedTimestamp(
            DBSPExpression body, Function<DBSPExpression, String> timestampOf,
            DBSPExpression first) {
        String shared = timestampOf.apply(first);
        Predicate<DBSPExpression> onSharedTimestamp = conjunct -> {
            String timestamp = timestampOf.apply(conjunct);
            return timestamp != null && timestamp.equals(shared);
        };
        DBSPExpression pushed = reorder(body, FilterOrderPolicy.LAST, conjunct -> {
            String timestamp = timestampOf.apply(conjunct);
            return timestamp != null && !timestamp.equals(shared);
        });
        return reorder(pushed, FilterOrderPolicy.FIRST, onSharedTimestamp);
    }

    /**
     * Names the timestamp that a window implementing `conjunct` would index its input by,
     * or null if no window can implement `conjunct`.  Conjuncts with equal names are implemented
     * by a single window carrying both bounds, as a BETWEEN's are.
     *
     * @param filter    Parent operator
     * @param parameter Parameter of the closure containing the conjunct
     * @param conjunct  A single conjunct of the filter's condition.
     */
    @Nullable
    static String timestampFingerprint(DBSPCompiler compiler, DBSPFilterOperator filter,
                                       DBSPParameter parameter, DBSPExpression conjunct) {
        DBSPClosureExpression timestamp = timestampClosure(compiler, filter, parameter, conjunct);
        if (timestamp == null)
            return null;
        return CanonicalForm.asString(compiler, timestamp);
    }

    /** The closure computing the timestamp a window implementing `conjunct` would index its input
     * by, or null if no window can implement `conjunct`.
     *
     * @param filter    Parent operator
     * @param parameter Parameter of the closure containing the conjunct
     * @param conjunct  A single conjunct of the filter's condition. */
    @Nullable
    static DBSPClosureExpression timestampClosure(
            DBSPCompiler compiler, DBSPFilterOperator filter, DBSPParameter parameter,
            DBSPExpression conjunct) {
        DBSPExpression timestamp = timestampExpression(compiler, filter, parameter, conjunct);
        if (timestamp == null)
            return null;
        return timestamp.deepCopy()
                .closure(parameter)
                .ensureTree(compiler).to(DBSPClosureExpression.class);
    }

    public interface PortAndClosureFingerprint {
        OutputPort port();

        DBSPClosureExpression closure();

        /** Given a port and a closure, produce a string that uniquely identifies the pair */
        default String inputFingerprint(DBSPCompiler compiler) {
            OutputPort port = this.port();
            return port.node().id + ":" + port.port()
                    + "|" + CanonicalForm.asString(compiler, this.closure());
        }
    }

    record PortAndClosure(OutputPort port, DBSPClosureExpression closure)
            implements PortAndClosureFingerprint {
    }

    /** An output port and a chain of functions applied to that port. */
    record SourceAndMaps(OutputPort port, List<DBSPClosureExpression> maps) {}

    /** Given a conjunct of a temporal filter and a source that should produce it,
     * compute the transformation producing the timestamp needed by the conjunct
     * from the source.  This transformation is the composition of all closures in
     * source.maps followed by the closure computing the timestamp needed by the conjunct.
     *
     * @param source    Source of the filter the conjunct belongs to.
     * @param filter    Filter the conjunct belongs to.
     * @param parameter The parameter of the filter's expression
     * @param conjunct  A single conjunct of the condition. */
    @Nullable
    static PortAndClosure temporalFilterSource(
            DBSPCompiler compiler, SourceAndMaps source, DBSPFilterOperator filter, DBSPParameter parameter,
            DBSPExpression conjunct) {
        DBSPClosureExpression timestamp = timestampClosure(compiler, filter, parameter, conjunct);
        if (timestamp == null)
            return null;
        for (DBSPClosureExpression map : source.maps())
            timestamp = timestamp.applyAfter(compiler, map, Maybe.MAYBE);
        return new PortAndClosure(source.port(), timestamp);
    }

    /**
     * The stream that will (likely) feed the first window synthesized from `filter`, with the functions to
     * compose a timestamp through.  May return null if the circuit has an unexpected shape.
     *
     * @param filter    Filter to inspect.
     * @param parameter The parameter of the filter's expression
     */
    @Nullable
    static SourceAndMaps filterSourcePath(CircuitGraph graph, DBSPFilterOperator filter, DBSPParameter parameter) {
        DBSPType parameterType = parameter.getType();
        List<DBSPClosureExpression> maps = new ArrayList<>();
        OutputPort currentSource = filter.input();
        // This while loop assumes that the chain of Map/MapIndex sources discovered here
        // will be collapsed by subsequent optimizations
        while (true) {
            DBSPSimpleOperator sourceOperator = currentSource.node().as(DBSPSimpleOperator.class);
            if (sourceOperator == null)
                break;
            if (!sourceOperator.is(DBSPMapOperator.class) && !sourceOperator.is(DBSPMapIndexOperator.class))
                break;
            if (graph.getFanout(sourceOperator) != 1)
                break;
            if (sourceOperator.function == null || !sourceOperator.function.is(DBSPClosureExpression.class))
                return null;
            DBSPClosureExpression map = sourceOperator.getClosureFunction();
            if (map.parameters.length != 1 || !parameterType.sameType(map.getResultType().ref()))
                return null;
            maps.add(map);
            parameterType = map.parameters[0].getType();
            currentSource = sourceOperator.inputs.get(0);
        }
        return new SourceAndMaps(currentSource, maps);
    }

    /** The condition of `filter` as an expression tree.
     * null if the filter does not mention now(). */
    @Nullable
    static DBSPClosureExpression condition(DBSPCompiler compiler, DBSPFilterOperator filter) {
        if (filter.function == null || !filter.function.is(DBSPClosureExpression.class))
            return null;
        DBSPClosureExpression condition = filter.getClosureFunction();
        if (condition.parameters.length != 1)
            return null;
        if (!ContainsNow.find(compiler, condition))
            return null;
        // The canonical form requires a tree, not a DAG.
        return condition.ensureTree(compiler).to(DBSPClosureExpression.class);
    }

    /**
     * Fingerprints of the sources that may produce temporal filter keys for 'condition',
     * each with the position of the conjunct producing it.
     *
     * @param filter    Filter to inspect.
     * @param condition The filter's condition, as an expression tree.
     */
    static Map<String, Integer> sourceFingerprints(
            DBSPCompiler compiler, CircuitGraph graph, DBSPFilterOperator filter, DBSPClosureExpression condition) {
        SourceAndMaps source = filterSourcePath(graph, filter, condition.parameters[0]);
        if (source == null)
            return Map.of();
        Map<String, Integer> keys = new LinkedHashMap<>();
        List<DBSPExpression> conjuncts = topLevelConjuncts(condition);
        for (int i = 0; i < conjuncts.size(); i++) {
            PortAndClosure pc = temporalFilterSource(
                    compiler, source, filter, condition.parameters[0], conjuncts.get(i));
            if (pc != null)
                keys.putIfAbsent(pc.inputFingerprint(compiler), i);
        }
        return keys;
    }

    /** Scan all filter operators that contain temporal filters and analyze how they may share inputs. */
    public static class Scan extends CircuitWithGraphsVisitor {
        /** For each filter we choose at most one input to share with other filters.
         * This holds the position of the conjunct */
        public final Map<DBSPFilterOperator, Integer> chosenConjunct = new LinkedHashMap<>();
        /** For each filter a set of fingerprints: each fingerprint corresponds to
         * a temporal filter expression and describes the input feeding the expression and
         * the closure computing the expression.  Each maps to the position of its conjunct. */
        private final Map<DBSPFilterOperator, Map<String, Integer>> candidates = new LinkedHashMap<>();

        public Scan(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
        }


        @Override
        public void postorder(DBSPFilterOperator filter) {
            DBSPClosureExpression condition = condition(this.compiler(), filter);
            if (condition == null)
                return;
            Map<String, Integer> keys = sourceFingerprints(
                    this.compiler(), this.getGraph(), filter, condition);
            if (!keys.isEmpty())
                this.candidates.put(filter, keys);
        }

        @Override
        public Token startVisit(IDBSPOuterNode node) {
            this.candidates.clear();
            return super.startVisit(node);
        }

        /** Compute for each filter the conjunct to place first (chosenConjunct) */
        @Override
        public void endVisit() {
            int threshold = this.compiler().metadata.windowSharingThreshold();
            this.chosenConjunct.clear();
            // Group filters by fingerprint
            Map<String, Set<DBSPFilterOperator>> byFingerprint = new LinkedHashMap<>();
            for (Map.Entry<DBSPFilterOperator, Map<String, Integer>> filter : this.candidates.entrySet())
                for (String key : filter.getValue().keySet())
                    byFingerprint.computeIfAbsent(key, k -> new LinkedHashSet<>()).add(filter.getKey());

            // Greedily choose conjuncts starting with the most shared
            int sharedInputs = 0;
            while (true) {
                // Find largest group
                String mostUsedFingerprint = null;
                int largestCount = 0;
                // Insertion order breaks ties, so the fingerprint of the earliest filter wins.
                // This makes the iteration deterministic.
                for (Map.Entry<String, Set<DBSPFilterOperator>> group : byFingerprint.entrySet()) {
                    if (group.getValue().size() > largestCount) {
                        largestCount = group.getValue().size();
                        mostUsedFingerprint = group.getKey();
                    }
                }
                if (mostUsedFingerprint == null || largestCount < threshold)
                    // Largest group is not large enough
                    break;
                // Emit all members of the larest group
                for (DBSPFilterOperator filter : Utilities.getExists(byFingerprint, mostUsedFingerprint)) {
                    Map<String, Integer> keys = this.candidates.get(filter);
                    this.chosenConjunct.put(filter, keys.get(mostUsedFingerprint));
                    // Each filter can only be used once, so remove it from byFingerprint for the other inputs
                    for (String key : keys.keySet())
                        if (!key.equals(mostUsedFingerprint))
                            Utilities.getExists(byFingerprint, key).remove(filter);
                }
                byFingerprint.remove(mostUsedFingerprint);
                sharedInputs++;
            }
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Shared window inputs: ")
                    .append(sharedInputs)
                    .append(", filters sharing them: ")
                    .append(this.chosenConjunct.size())
                    .newline();
            super.endVisit();
        }
    }

    /** Reorders each filter's conjuncts according to the policy the scan picked. */
    public static class Reorder extends CircuitCloneVisitor {
        /** For each filter that shares an input, the position of the conjunct computing the
         * timestamp of that input. */
        public final Map<DBSPFilterOperator, Integer> chosenConjunct;

        public Reorder(DBSPCompiler compiler, Map<DBSPFilterOperator, Integer> chosenConjunct) {
            super(compiler, false);
            this.chosenConjunct = chosenConjunct;
        }

        @Override
        public void postorder(DBSPFilterOperator filter) {
            DBSPClosureExpression condition = condition(this.compiler(), filter);
            if (condition == null) {
                super.postorder(filter);
                return;
            }

            DBSPParameter parameter = condition.parameters[0];
            Predicate<DBSPExpression> temporal = conjunct ->
                    timestampExpression(this.compiler(), filter, parameter, conjunct) != null;
            Integer chosen = this.chosenConjunct.get(filter);

            // If the input will NOT be shared, move all temporal filters last.
            // If the input will be shared, hoist only the conjuncts on the shared timestamp.
            DBSPExpression result;
            if (chosen == null) {
                result = reorder(condition.body, FilterOrderPolicy.LAST, temporal);
            } else {
                result = reorderAroundSharedTimestamp(
                        condition.body,
                        conjunct -> timestampFingerprint(
                                this.compiler(), filter, parameter, conjunct),
                        topLevelConjuncts(condition).get(chosen));
            }
            if (result == condition.body) {
                super.postorder(filter);
                return;
            }
            DBSPFilterOperator replacement = new DBSPFilterOperator(
                    filter.getRelNode(), result.closure(condition.parameters[0]),
                    this.mapped(filter.input()));
            this.map(filter, replacement);
        }
    }
}
