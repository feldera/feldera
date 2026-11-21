package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.util.Pair;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.IGCOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCustomOrdField;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPNoComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnwrapCustomOrdExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Lower {@link org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator} into
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator}.
 * Also moves their corresponding {@link DBSPIntegrateTraceRetainValuesOperator}
 * and {@link DBSPIntegrateTraceRetainNValuesOperator}, if they exist. */
public class LowerAsof implements CircuitTransform {
    final DBSPCompiler compiler;
    private final Function<CircuitTransform, CircuitTransform> select;

    LowerAsof(DBSPCompiler compiler, Function<CircuitTransform, CircuitTransform> select) {
        this.compiler = compiler;
        this.select = select;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        // Maps an integrate trace node in the original graph to the asof join operator that it should be moved to
        Map<IGCOperator, DBSPConcreteAsofJoinOperator> leftGces = new HashMap<>();
        Map<IGCOperator, DBSPConcreteAsofJoinOperator> rightGces = new HashMap<>();
        // Replay uses insertion order to reproduce the order of GC edges.
        Map<IGCOperator, IGCOperator> original = this.compiler.usesCircuitSeam()
                ? new LinkedHashMap<>() : new HashMap<>();

        Graph graph = new Graph(this.compiler);
        this.compiler.applyObservedPass(this.getName(), 0, 0, this.select.apply(graph), circuit);

        // First scan the circuit, populate the maps, and replace asof operators
        var lower = new LowerAsofInner(compiler, graph.graphs, leftGces, rightGces, original);
        circuit = this.compiler.applyObservedPass(this.getName(), 1, 0, lower, circuit);
        if (leftGces.isEmpty()) {
            return circuit;
        }

        graph = new Graph(this.compiler);
        this.compiler.applyObservedPass(this.getName(), 2, 0, this.select.apply(graph), circuit);
        // Add dependency edges to the circuit to enforce the processing of integrate trace operators
        // after the corresponding joins
        var ordering = new OrderGcDependencies(graph.graphs.getGraph(circuit));
        for (var entry: original.entrySet()) {
            var newIntegrate = entry.getKey();
            var originalIntegrate = entry.getValue();
            var newJoin = leftGces.get(originalIntegrate);
            if (newJoin != null)
                ordering.add(newJoin, newIntegrate.asOperator());
            newJoin = rightGces.get(originalIntegrate);
            if (newJoin != null)
                ordering.add(newJoin, newIntegrate.asOperator());
        }
        circuit = this.compiler.applyObservedPass(this.getName(), 3, 0, this.select.apply(ordering), circuit);

        // Scan the circuit a second time and fix the integrate nodes.
        var moveGc = new MoveGC(compiler, leftGces, rightGces, original);
        circuit = this.compiler.applyObservedPass(this.getName(), 4, 0, this.select.apply(moveGc), circuit);
        return circuit;
    }

    @Override
    public String getName() {
        return "LowerAsof";
    }

    @Override
    public String toString() {
        return this.getName();
    }

    /** The insertion order of additional edges breaks ties between ready operators. */
    public static final class OrderGcDependencies implements CircuitTransform {
        private record Edge(DBSPOperator join, DBSPOperator retainer) {}
        private final CircuitGraph graph;
        private final List<Edge> edges = new ArrayList<>();

        OrderGcDependencies(CircuitGraph graph) {
            this.graph = graph;
        }

        private void add(DBSPOperator join, DBSPOperator retainer) {
            this.edges.add(new Edge(join, retainer));
        }

        public List<long[]> edgeIds() {
            return this.edges.stream().map(e -> new long[]{e.join.id, e.retainer.id}).toList();
        }

        @Override
        public String getName() {
            return "OrderGcDependencies";
        }

        @Override
        public DBSPCircuit apply(DBSPCircuit circuit) {
            for (var edge : this.edges)
                this.graph.addEdge(edge.join, edge.retainer, 0);
            circuit.resort(this.graph);
            return circuit;
        }
    }

    /** Move the integrate trace operator to the new join */
    public static class MoveGC extends CircuitCloneVisitor {
        final Map<IGCOperator, DBSPConcreteAsofJoinOperator> leftGces;
        final Map<IGCOperator, DBSPConcreteAsofJoinOperator> rightGces;
        final Map<IGCOperator, IGCOperator> original;

        public MoveGC(
                DBSPCompiler compiler,
                Map<IGCOperator, DBSPConcreteAsofJoinOperator> leftGces,
                Map<IGCOperator, DBSPConcreteAsofJoinOperator> rightGces,
                Map<IGCOperator, IGCOperator> original) {
            super(compiler, false);
            this.leftGces = leftGces;
            this.rightGces = rightGces;
            this.original = original;
            this.preservesTypes = false;
        }

        /** Bind retention operators in the lowered circuit to their concrete joins. */
        public Map<Long, Long> retentionJoins() {
            Map<Long, Long> joins = new HashMap<>();
            for (var operator : this.original.values()) {
                var original = this.original.get(operator);
                var side = operator instanceof DBSPIntegrateTraceRetainNValuesOperator
                        ? this.rightGces : this.leftGces;
                var join = side.get(original);
                if (join != null)
                    joins.put(operator.asOperator().id, join.id);
            }
            return joins;
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
            // Because we only replace such operators in this visitor, and such
            // operators have no successors, we know that the graph will not change
            // anywhere else, including the ConcreteAsofJoinOperator
            IGCOperator original = this.original.get(operator);
            if (original == null || !this.leftGces.containsKey(original)) {
                super.postorder(operator);
                return;
            }
            DBSPConcreteAsofJoinOperator join = Utilities.getExists(this.leftGces, original);
            // This will be the same in the original and new circuit
            // Have to rewrite the function of the operator; the original one had signature
            // (key, value), the new one has signature (key, WithCustomOrd(value))
            // If the original function of operator was |x, y| f(x, y),
            // the new one is |z, y| f( (unwrapCustomOrd(z), y)
            DBSPClosureExpression originalRetain = operator.getClosureFunction();
            Utilities.enforce(originalRetain.parameters.length == 2);

            OutputPort left = join.left();
            DBSPTypeIndexedZSet ix = left.getOutputIndexedZSetType();
            DBSPVariablePath z = ix.elementType.ref().var();
            DBSPVariablePath y = originalRetain.parameters[1].getType().var();
            DBSPExpression arg = new DBSPUnwrapCustomOrdExpression(z.deref()).borrow();
            DBSPClosureExpression newRetain = originalRetain.call(arg, y).reduce(this.compiler).closure(z, y);
            DBSPSimpleOperator replacement = operator.with(
                    newRetain, left.outputType(),
                    Linq.list(left, operator.right()), true);
            this.map(operator, replacement);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
            // Because we only replace such operators in this visitor, and such
            // operators have no successors, we know that the graph will not change
            // anywhere else, including the ConcreteAsofJoinOperator
            IGCOperator original = this.original.get(operator);
            if (original == null || !this.rightGces.containsKey(original)) {
                super.postorder(operator);
                return;
            }
            DBSPConcreteAsofJoinOperator join = Utilities.getExists(this.rightGces, original);
            // This will be the same in the original and new circuit
            // Have to rewrite the function of the operator; the original one had signature
            // (key, value), the new one has signature (key, WithCustomOrd(value))
            // If the original function of operator was |x, y| f(x, y),
            // the new one is |z, y| f( (unwrapCustomOrd(z), y)
            DBSPClosureExpression originalRetain = operator.getClosureFunction();
            Utilities.enforce(originalRetain.parameters.length == 2);

            OutputPort right = join.right();
            DBSPTypeIndexedZSet ix = right.getOutputIndexedZSetType();
            DBSPVariablePath z = ix.elementType.ref().var();
            DBSPVariablePath y = originalRetain.parameters[1].getType().var();
            DBSPExpression arg = new DBSPUnwrapCustomOrdExpression(z.deref()).borrow();
            DBSPClosureExpression newRetain = originalRetain.call(arg, y).reduce(this.compiler).closure(z, y);
            DBSPSimpleOperator replacement = operator.with(
                    newRetain, right.outputType(),
                    // Yes, we replace the left input of this operator with the right join input source
                    Linq.list(right, operator.right()), true);
            this.map(operator, replacement);
        }
    }

    static class LowerAsofInner extends CircuitCloneWithGraphsVisitor {
        final Map<IGCOperator, DBSPConcreteAsofJoinOperator> leftGces;
        final Map<IGCOperator, DBSPConcreteAsofJoinOperator> rightGces;
        final Map<IGCOperator, IGCOperator> original;
        // One LowerAsofInner instance visits one circuit. Most circuits contain no ASOF joins.
        @javax.annotation.Nullable private Map<DBSPAsofJoinOperator, List<IGCOperator>> nativeRetainers;

        public LowerAsofInner(
                DBSPCompiler compiler, CircuitGraphs graphs,
                Map<IGCOperator, DBSPConcreteAsofJoinOperator> leftGces,
                Map<IGCOperator, DBSPConcreteAsofJoinOperator> rightGces,
                Map<IGCOperator, IGCOperator> original) {
            super(compiler, graphs);
            this.leftGces = leftGces;
            this.rightGces = rightGces;
            this.original = original;
        }

        private DBSPMapIndexOperator indexInput(CalciteRelNode node, OutputPort input, DBSPType keyType,
                DBSPVariablePath pair, DBSPComparatorExpression comparator, boolean left) {
            DBSPClosureExpression function;
            if (this.compiler().options.ioOptions.rustOptimizerPasses) {
                function = RustCircuitPass.asofIndex(this.compiler(), pair, comparator, left);
            } else {
                var value = DBSPTupleExpression.flatten(pair.field(1).deref());
                var wrapped = new DBSPCustomOrdExpression(node, value, comparator);
                var key = pair.field(0).deref();
                function = new DBSPRawTupleExpression(left ? DBSPTupleExpression.flatten(key) : key, wrapped)
                        .closure(pair);
            }
            var valueType = function.getResultType().to(DBSPTypeRawTuple.class).getFieldType(1);
            var index = new DBSPMapIndexOperator(node, function, TypeCompiler.makeIndexedZSet(keyType, valueType),
                    this.mapped(input));
            this.addOperator(index);
            return index;
        }

        private Pair<DBSPClosureExpression, DBSPClosureExpression> timestamps(DBSPAsofJoinOperator join,
                DBSPTypeWithCustomOrd wrappedLeftType, DBSPTypeWithCustomOrd wrappedRightType) {
            if (this.compiler().options.ioOptions.rustOptimizerPasses)
                return RustCircuitPass.asofTimestamps(this.compiler(), wrappedLeftType, join.leftTimestampIndex,
                        wrappedRightType, join.rightTimestampIndex);

            DBSPVariablePath leftVar = wrappedLeftType.ref().var();
            DBSPVariablePath rightVar = wrappedRightType.ref().var();

            DBSPExpression leftTS = new DBSPUnwrapCustomOrdExpression(leftVar.deref())
                    .field(join.leftTimestampIndex);
            DBSPExpression rightTS = new DBSPUnwrapCustomOrdExpression(rightVar.deref())
                    .field(join.rightTimestampIndex);

            DBSPType leftTSType = leftTS.getType();
            DBSPType rightTSType = rightTS.getType();
            // Rust expects both timestamps to have the same type
            boolean nullable = leftTSType.mayBeNull || rightTSType.mayBeNull;
            DBSPType commonTSType = leftTSType.withMayBeNull(nullable);
            Utilities.enforce(commonTSType.sameType(rightTSType.withMayBeNull(nullable)));

            DBSPClosureExpression leftTimestamp = leftTS.cast(
                    leftTS.getNode(), commonTSType, DBSPCastExpression.CastType.SqlUnsafe)
                    .applyCloneIfNeeded()
                    .closure(leftVar);
            DBSPClosureExpression rightTimestamp = rightTS.cast(
                    rightTS.getNode(), commonTSType, DBSPCastExpression.CastType.SqlUnsafe)
                    .applyCloneIfNeeded()
                    .closure(rightVar);
            return Pair.of(leftTimestamp, rightTimestamp);
        }

        private DBSPClosureExpression projection(DBSPAsofJoinOperator join,
                DBSPTypeWithCustomOrd wrappedLeftType, DBSPTypeWithCustomOrd wrappedRightType) {
            if (this.compiler().options.ioOptions.rustOptimizerPasses)
                return RustCircuitPass.asofProjection(this.compiler(), join.getClosureFunction(), join.getKeyType(),
                        wrappedLeftType, wrappedRightType);

            DBSPVariablePath k = join.getKeyType().ref().var();
            DBSPVariablePath l0 = wrappedLeftType.ref().var();
            // Signature of the function is (k: &K, l: &L, r: Option<&R>)
            DBSPVariablePath r0 = wrappedRightType.ref().withMayBeNull(true).var();
            List<DBSPExpression> lFields = new ArrayList<>();
            for (int i = 0; i < wrappedLeftType.size(); i++)
                lFields.add(new DBSPUnwrapCustomOrdExpression(l0.deref()).field(i));
            List<DBSPExpression> rFields = new ArrayList<>();
            for (int i = 0; i < wrappedRightType.size(); i++)
                rFields.add(new DBSPCustomOrdField(r0, i));
            DBSPTupleExpression lTuple = new DBSPTupleExpression(lFields, false);
            DBSPTupleExpression rTuple = new DBSPTupleExpression(rFields, false);
            DBSPExpression call = join.getClosureFunction().call(k, lTuple.borrow(), rTuple.borrow())
                    .reduce(this.compiler);
            return call.closure(k, l0, r0);
        }

        @Override
        public void postorder(DBSPAsofJoinOperator join) {
            // Convert AsofJoinOperator to ConcreteAsofJoinOperator
            // The new operator has two MapIndex operators in front of its
            // inputs, producing DBSPCustomOrdExpressions.
            // Internally the new operator also has a function which converts
            // back to normal types.
            CalciteRelNode node = join.getRelNode();
            DBSPTypeTuple leftElementType = join.getLeftInputValueType()
                    .to(DBSPTypeTuple.class);
            DBSPTypeTuple rightElementType = join.getRightInputValueType()
                    .to(DBSPTypeTuple.class);
            DBSPType keyType = join.getKeyType();

            DBSPVariablePath l = join.left().getOutputIndexedZSetType().getKVRefType().var();
            DBSPVariablePath r = join.right().getOutputIndexedZSetType().getKVRefType().var();

            var leftComparator = new DBSPNoComparatorExpression(node, leftElementType)
                    .field(join.leftTimestampIndex, true);
            var leftIndex = this.indexInput(node, join.left(), keyType, l, leftComparator, true);
            var rightComparator = new DBSPNoComparatorExpression(node, rightElementType)
                    .field(join.rightTimestampIndex, true);
            var rightIndex = this.indexInput(node, join.right(), keyType, r, rightComparator, false);

            var wrappedLeftType = new DBSPTypeWithCustomOrd(node, leftElementType, leftComparator.getComparatorType());
            var wrappedRightType = new DBSPTypeWithCustomOrd(node, rightElementType, rightComparator.getComparatorType());

            var timestamps = this.timestamps(join, wrappedLeftType, wrappedRightType);
            var function = this.projection(join, wrappedLeftType, wrappedRightType);

            DBSPConcreteAsofJoinOperator result = new DBSPConcreteAsofJoinOperator(node, join.getOutputZSetType(),
                    function, timestamps.left, timestamps.right, join.comparator,
                    join.isLeft,
                    leftIndex.outputPort(), rightIndex.outputPort())
                    .copyAnnotations(join)
                    .to(DBSPConcreteAsofJoinOperator.class);
            this.map(join, result);

            if (this.compiler().options.ioOptions.rustOptimizerPasses) {
                if (this.nativeRetainers == null)
                    this.nativeRetainers = RustCircuitPass.asofRetainers(this.getCircuit());
                for (var retainer : this.nativeRetainers.getOrDefault(join, List.of())) {
                    if (retainer instanceof DBSPIntegrateTraceRetainValuesOperator)
                        Utilities.putNew(this.leftGces, retainer, result);
                    else if (retainer instanceof DBSPIntegrateTraceRetainNValuesOperator)
                        Utilities.putNew(this.rightGces, retainer, result);
                    else
                        throw new IllegalStateException("Unexpected ASOF retention operator " + retainer);
                }
                return;
            }

            // If there is an IntegrateTraceRetainValues operator on the left input, it needs to be moved.
            // The graph has edges per operator, so a successor can read another output of the input.
            CircuitGraph graph = this.getGraph();
            for (Port<DBSPOperator> port : graph.getSuccessors(join.left().operator)) {
                IGCOperator op = port.node().as(DBSPIntegrateTraceRetainValuesOperator.class);
                if (op != null && op.garbageCollects(join.left()))
                    Utilities.putNew(this.leftGces, op, result);
            }

            // Similar procedure for the right input
            for (Port<DBSPOperator> port : graph.getSuccessors(join.right().operator)) {
                IGCOperator op = port.node().as(DBSPIntegrateTraceRetainNValuesOperator.class);
                if (op != null && op.garbageCollects(join.right()))
                    Utilities.putNew(this.rightGces, op, result);
            }
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
            super.postorder(operator);
            DBSPIntegrateTraceRetainValuesOperator repl = this.mapped(operator.getOutput(0))
                    .operator.to(DBSPIntegrateTraceRetainValuesOperator.class);
            Utilities.putNew(this.original, operator, repl);
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
            super.postorder(operator);
            DBSPIntegrateTraceRetainNValuesOperator repl = this.mapped(operator.getOutput(0))
                    .operator.to(DBSPIntegrateTraceRetainNValuesOperator.class);
            Utilities.putNew(this.original, operator, repl);
        }
    }
}
