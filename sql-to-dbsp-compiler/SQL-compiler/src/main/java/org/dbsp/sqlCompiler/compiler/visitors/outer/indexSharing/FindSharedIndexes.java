package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIndexedTopKOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPPartitionedRollingAggregateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraph;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitGraphs;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitWithGraphsVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Finds multiple {@link DBSPMapIndexOperator}s that can be combined into a single wide one */
class FindSharedIndexes extends CircuitWithGraphsVisitor {
    final List<Candidates> candidates;
    final Set<DBSPMapIndexOperator> visited;

    boolean sameKey(DBSPClosureExpression first, DBSPClosureExpression second) {
        DBSPVariablePath varLeft = first.getResultType().ref().var();
        DBSPClosureExpression projectLeft = varLeft.deref().field(0).closure(varLeft);

        DBSPVariablePath varRight = second.getResultType().ref().var();
        DBSPClosureExpression projectRight = varRight.deref().field(0).closure(varRight);

        DBSPClosureExpression firstKey = projectLeft.applyAfter(this.compiler, first, Maybe.YES);
        DBSPClosureExpression secondKey = projectRight.applyAfter(this.compiler, second, Maybe.YES);
        return firstKey.equivalent(secondKey);
    }

    public FindSharedIndexes(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs);
        this.candidates = new ArrayList<>();
        this.visited = new HashSet<>();
    }

    /** Find if the node has a single successor which requires an integrator on this input */
    @Nullable
    static MapIndexAndConsumer followedByIntegrator(CircuitGraph graph, DBSPMapIndexOperator operator) {
        List<Port<DBSPOperator>> successors = graph.getSuccessors(operator);
        if (successors.size() > 1)
            return null;
        Port<DBSPOperator> port = successors.get(0);
        DBSPOperator successor = port.node();
        if (!canShareInputIntegral(successor, port.port()))
            return null;
        return new MapIndexAndConsumer(operator, successor.to(DBSPSimpleOperator.class), port.port());
    }

    /** True if the consumer keeps an integral of the collection it reads on input
     * {@code inputIndex}, and can therefore share that integral with the other consumers
     * of the same index.
     * @param consumer    Operator that reads an index.
     * @param inputIndex  Input of the consumer that reads it. */
    static boolean canShareInputIntegral(DBSPOperator consumer, int inputIndex) {
        if (consumer.is(DBSPJoinBaseOperator.class))
            return !consumer.is(DBSPAsofJoinOperator.class) &&
                   !consumer.is(DBSPConcreteAsofJoinOperator.class);
        if (consumer.is(DBSPStarJoinBaseOperator.class))
            return true;
        // An antijoin shares on both of its inputs; requiresFixedIndex keeps the value it
        // reads on input 0, and only that one, from widening
        if (consumer.is(DBSPAntiJoinOperator.class))
            return true;
        if (requiresFixedIndex(consumer, inputIndex))
            return true;
        if (consumer.is(DBSPPartitionedRollingAggregateOperator.class))
            return true;
        if (consumer.is(DBSPAggregateOperatorBase.class))
            // If the operator has a list of aggregates it implies that the value computed by
            // the map-index can be reorganized
            return consumer.to(DBSPAggregateOperatorBase.class).aggregateList != null;
        return false;
    }

    /** Returns an index and the consumer that reads it, or null.
     * The result is non-null when the index can be combined with other indexes.
     * @param graph     Graph of the circuit that contains the index.
     * @param operator  Index to examine.
     * @return          The index and its consumer, or null when it cannot be combined. */
    @Nullable
    MapIndexAndConsumer mayBeCombined(CircuitGraph graph, DBSPMapIndexOperator operator) {
        // No candidate set holds the index yet
        if (this.visited.contains(operator))
            return null;
        // Exactly one consumer reads the index, and that consumer can share an integral
        MapIndexAndConsumer pair = followedByIntegrator(graph, operator);
        if (pair == null)
            return pair;
        // The value of the index can be taken apart field by field
        if (!this.valueFieldIsDecomposable(operator.getClosureFunction()))
            return null;
        return pair;
    }

    /** True if closure body has a shape which is easy to "decompose" into individual fields:
     * The function is either a projection, or the value part of the function is
     * a tuple constructor. */
    boolean valueFieldIsDecomposable(DBSPClosureExpression function) {
        Projection projection = new Projection(this.compiler, true, false);
        projection.apply(function);
        if (projection.isProjection)
            return true;
        if (!function.body.is(DBSPRawTupleExpression.class))
            return false;
        DBSPRawTupleExpression body = function.body.to(DBSPRawTupleExpression.class);
        return body.fields != null && body.size() == 2 && body.fields[1].is(DBSPTupleExpression.class);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        CircuitGraph graph = this.getGraph();
        OutputPort input = operator.input();
        List<Port<DBSPOperator>> siblings = graph.getSuccessors(input.node());
        if (siblings.size() < 2) return;

        MapIndexAndConsumer pair = this.mayBeCombined(graph, operator);
        if (pair == null) return;
        boolean nullableValue = operator.getOutputIndexedZSetType().elementType.mayBeNull;

        List<MapIndexAndConsumer> matching = new ArrayList<>();
        matching.add(pair);
        this.visited.add(operator);

        // Scan all siblings to see which ones match
        for (Port<DBSPOperator> port : siblings) {
            DBSPOperator sibling = port.node();
            if (!sibling.is(DBSPMapIndexOperator.class))
                continue;
            if (sibling == operator)
                continue;
            DBSPMapIndexOperator smi = sibling.to(DBSPMapIndexOperator.class);
            MapIndexAndConsumer nextPair = this.mayBeCombined(graph, smi);
            if (nextPair == null)
                continue;

            boolean nullableSmiValue = smi.getOutputIndexedZSetType().elementType.mayBeNull;
            if (nullableSmiValue != nullableValue)
                continue;

            if (!this.sameKey(operator.getClosureFunction(), smi.getClosureFunction()))
                continue;
            this.visited.add(nextPair.index());
            matching.add(nextPair);
        }

        if (matching.size() > 1)
            this.candidates.add(new Candidates(matching));
    }

    /** True if the indexed input of a consumer operator cannot be changed
     * without altering the consumer's semantics.
     * @param consumer    Operator that reads an index.
     * @param inputIndex  Input of the consumer that reads it. */
    static boolean requiresFixedIndex(DBSPOperator consumer, int inputIndex) {
        if (consumer.is(DBSPAntiJoinOperator.class))
            // The operator copies to its output the value it reads on input 0; input 1
            // supplies keys, and the value read there is discarded
            return inputIndex == 0;
        if (consumer.is(DBSPWindowOperator.class))
            // Input 1 carries the window bounds
            return inputIndex == 0;
        if (consumer.is(DBSPSinkOperator.class))
            return consumer.to(DBSPSinkOperator.class).metadata.viewKind == 
                               SqlCreateView.ViewKind.MATERIALIZED;
        if (consumer.is(DBSPAggregateOperatorBase.class) &&
                !consumer.is(DBSPPartitionedRollingAggregateOperator.class))
            // If there is no aggregate list we cannot modify the index
            return consumer.to(DBSPAggregateOperatorBase.class).aggregateList == null;
        // A distinct is never currently applied to an indexed-zset.
        // This code will fire in the future if it does.
        return consumer.is(DBSPDistinctOperator.class) ||
                consumer.is(DBSPIndexedTopKOperator.class) ||
                consumer.is(DBSPLagOperator.class);
    }

    /** A set of candidate operators to share a single index, each paired with the index it reads and
     * the input reading it.  Within one set
     * - all indexes read the same source,
     * - all indexes compute the same key,
     * - all indexes produce a value with the same nullability,
     * - all indexes have one consumer, and
     * - all consumers may share the integral they keep of the collection their index produces. */
    record Candidates(List<MapIndexAndConsumer> members) {
        int size() {
            return this.members.size();
        }

        /** Split the members into the lists that can each be implemented as one shared index. */
        List<List<MapIndexAndConsumer>> split(DBSPCompiler compiler) {
            // An index whose consumer cannot take a wider value seeds a list, the widest of
            // those first: the value of the first member becomes the start of the shared
            // value, and the widest one covers the fields of the most other members.
            List<MapIndexAndConsumer> remaining = new ArrayList<>(
                    Linq.where(this.members, MapIndexAndConsumer::requiresFixedIndex));
            remaining.sort(Comparator.comparingInt(MapIndexAndConsumer::valueWidth).reversed());
            remaining.addAll(Linq.where(this.members, m -> !m.requiresFixedIndex()));
            List<List<MapIndexAndConsumer>> result = new ArrayList<>();
            // Greedy algorithm: pick the widest fixed-inputs operator and add
            // all compatible operators to the list.
            while (!remaining.isEmpty()) {
                List<MapIndexAndConsumer> group = new ArrayList<>();
                group.add(remaining.remove(0));
                for (int i = 0; i < remaining.size(); i++) {
                    List<MapIndexAndConsumer> trial = Linq.append(group, remaining.get(i));
                    if (WideMapIndexBuilder.create(compiler, trial).reproducesFixedValues(trial)) {
                        group.add(remaining.remove(i));
                        i--;
                    }
                }
                if (group.size() > 1)
                    result.add(group);
            }
            return result;
        }

        @Override
        public String toString() {
            return this.members.toString();
        }
    }

    /** A MapIndex operator and the consumer that follows it.
     * @param index       Operator computing the index.
     * @param consumer    Operator reading the index.
     * @param inputIndex  Input of the consumer that reads the index. */
    record MapIndexAndConsumer(DBSPMapIndexOperator index, DBSPSimpleOperator consumer, int inputIndex) {
        boolean requiresFixedIndex() {
            return FindSharedIndexes.requiresFixedIndex(this.consumer, this.inputIndex);
        }

        /** Number of fields in the value that the index computes */
        int valueWidth() {
            DBSPType value = this.index.getOutputIndexedZSetType().elementType;
            return value.is(DBSPTypeTupleBase.class) ? value.to(DBSPTypeTupleBase.class).size() : 1;
        }

        @Override
        public String toString() {
            return "Ix[" + this.index + ", " + this.consumer + ":" + this.inputIndex + "]";
        }
    }
}
