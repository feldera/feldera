package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
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
import org.dbsp.sqlCompiler.ir.type.user.DBSPComparatorType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWithCustomOrd;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Lower {@link org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator} into
 * {@link org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator}.
 * Also moves their corresponding {@link DBSPIntegrateTraceRetainValuesOperator}
 * if they exist. */
public class LowerAsof implements CircuitTransform {
    final DBSPCompiler compiler;

    LowerAsof(DBSPCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        // Maps an integrate trace node in the original graph to the asof join operator that it should be moved to
        Map<DBSPIntegrateTraceRetainValuesOperator, DBSPConcreteAsofJoinOperator> gces = new HashMap<>();
        // Maps an integrate trace operator in the new graph to the original one
        Map<DBSPIntegrateTraceRetainValuesOperator, DBSPIntegrateTraceRetainValuesOperator> original = new HashMap<>();

        Graph graph = new Graph(this.compiler);
        graph.apply(circuit);

        // First scan the circuit, populate the maps, and replace asof operators
        var lower = new LowerAsofInner(compiler, graph.graphs, gces, original);
        circuit = lower.apply(circuit);
        if (gces.isEmpty()) {
            return circuit;
        }

        graph = new Graph(this.compiler);
        graph.apply(circuit);
        // Add dependency edges to the circuit to enforce the processing of integrate trace operators
        // after the corresponding joins
        var cg = graph.graphs.getGraph(circuit);
        for (var entry: original.entrySet()) {
            var newIntegrate = entry.getKey();
            var originalIntegrate = entry.getValue();
            var newJoin = gces.get(originalIntegrate);
            if (newJoin != null)
                cg.addEdge(newJoin, newIntegrate, 0);
        }
        circuit.resort(cg);

        // Scan the circuit a second time and fix the integrate nodes.
        var moveGc = new MoveGC(compiler, gces, original);
        circuit = moveGc.apply(circuit);
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

    /** Move the integrate trace operator to the new join */
    static class MoveGC extends CircuitCloneVisitor {
        final Map<DBSPIntegrateTraceRetainValuesOperator, DBSPConcreteAsofJoinOperator> gces;
        final Map<DBSPIntegrateTraceRetainValuesOperator, DBSPIntegrateTraceRetainValuesOperator> original;

        public MoveGC(
                DBSPCompiler compiler,
                Map<DBSPIntegrateTraceRetainValuesOperator, DBSPConcreteAsofJoinOperator> gces,
                Map<DBSPIntegrateTraceRetainValuesOperator, DBSPIntegrateTraceRetainValuesOperator> original) {
            super(compiler, false);
            this.gces = gces;
            this.original = original;
            this.preservesTypes = false;
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
            // Because we only replace such operators in this visitor, and such
            // operators have no successors, we know that the graph will not change
            // anywhere else, including the ConcreteAsofJoinOperator
            DBSPIntegrateTraceRetainValuesOperator original = this.original.get(operator);
            if (original == null) {
                super.postorder(operator);
                return;
            }
            DBSPConcreteAsofJoinOperator join = Utilities.getExists(this.gces, original);
            // This will be the same in the original and new circuit
            // Have to rewrite the function of the operator; the original one had signature
            // (key, value), the new one has signature (key, WithCustomOrd(value))
            // If the original function of operator was |x, y| f(x, y),
            // the new one is |z, y| f( (unwrap(z), y)
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
    }

    static class LowerAsofInner extends CircuitCloneWithGraphsVisitor {
        final Map<DBSPIntegrateTraceRetainValuesOperator, DBSPConcreteAsofJoinOperator> gces;
        final Map<DBSPIntegrateTraceRetainValuesOperator, DBSPIntegrateTraceRetainValuesOperator> original;

        public LowerAsofInner(
                DBSPCompiler compiler, CircuitGraphs graphs,
                Map<DBSPIntegrateTraceRetainValuesOperator, DBSPConcreteAsofJoinOperator> gces,
                Map<DBSPIntegrateTraceRetainValuesOperator, DBSPIntegrateTraceRetainValuesOperator> original) {
            super(compiler, graphs, false);
            this.gces = gces;
            this.original = original;
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

            DBSPTupleExpression tuple = DBSPTupleExpression.flatten(l.field(1).deref());
            DBSPComparatorExpression leftComparator =
                    new DBSPNoComparatorExpression(node, leftElementType)
                            .field(join.leftTimestampIndex, true);
            DBSPComparatorType leftComparatorType = leftComparator.getComparatorType();
            DBSPExpression wrapper = new DBSPCustomOrdExpression(node, tuple, leftComparator);
            DBSPClosureExpression toLeftKey =
                    new DBSPRawTupleExpression(DBSPTupleExpression.flatten(l.field(0).deref()), wrapper)
                            .closure(l);
            DBSPMapIndexOperator leftIndex = new DBSPMapIndexOperator(
                    node, toLeftKey,
                    TypeCompiler.makeIndexedZSet(keyType, wrapper.getType()),
                    false, this.mapped(join.left()));
            this.addOperator(leftIndex);

            DBSPComparatorExpression rightComparator =
                    new DBSPNoComparatorExpression(node, rightElementType)
                            .field(join.rightTimestampIndex, true);
            DBSPType rightComparatorType = rightComparator.getComparatorType();
            wrapper = new DBSPCustomOrdExpression(node,
                    DBSPTupleExpression.flatten(r.field(1).deref()), rightComparator);
            DBSPClosureExpression toRightKey =
                    new DBSPRawTupleExpression(r.field(0).deref(), wrapper)
                            .closure(r);
            DBSPMapIndexOperator rightIndex = new DBSPMapIndexOperator(
                    node, toRightKey,
                    TypeCompiler.makeIndexedZSet(keyType, wrapper.getType()),
                    false, this.mapped(join.right()));
            this.addOperator(rightIndex);

            DBSPType wrappedLeftType = new DBSPTypeWithCustomOrd(node, leftElementType, leftComparatorType);
            DBSPType wrappedRightType = new DBSPTypeWithCustomOrd(node, rightElementType, rightComparatorType);

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

            DBSPClosureExpression leftTimestamp = leftTS.cast(leftTS.getNode(), commonTSType, false).closure(leftVar);
            DBSPClosureExpression rightTimestamp = rightTS.cast(rightTS.getNode(), commonTSType, false).closure(rightVar);

            DBSPVariablePath k = keyType.ref().var();
            DBSPVariablePath l0 = wrappedLeftType.ref().var();
            // Signature of the function is (k: &K, l: &L, r: Option<&R>)
            DBSPVariablePath r0 = wrappedRightType.ref().withMayBeNull(true).var();
            List<DBSPExpression> lFields = new ArrayList<>();
            for (int i = 0; i < leftElementType.size(); i++)
                lFields.add(new DBSPUnwrapCustomOrdExpression(l0.deref()).field(i));
            List<DBSPExpression> rFields = new ArrayList<>();
            for (int i = 0; i < rightElementType.size(); i++)
                rFields.add(new DBSPCustomOrdField(r0, i));
            DBSPTupleExpression lTuple = new DBSPTupleExpression(lFields, false);
            DBSPTupleExpression rTuple = new DBSPTupleExpression(rFields, false);
            DBSPExpression call = join.getClosureFunction().call(k, lTuple.borrow(), rTuple.borrow())
                    .reduce(this.compiler);
            DBSPExpression function = call.closure(k, l0, r0);

            DBSPConcreteAsofJoinOperator result = new DBSPConcreteAsofJoinOperator(node, join.getOutputZSetType(),
                    function, leftTimestamp, rightTimestamp, join.comparator,
                    join.isMultiset, join.isLeft,
                    leftIndex.outputPort(), rightIndex.outputPort());
            this.map(join, result);

            // If there is an IntegrateTraceRetainValues operator on the left input, it needs to be moved.
            CircuitGraph graph = this.getGraph();
            for (Port<DBSPOperator> port : graph.getSuccessors(join.left().operator)) {
                DBSPIntegrateTraceRetainValuesOperator op =
                        port.node().as(DBSPIntegrateTraceRetainValuesOperator.class);
                if (op != null)
                    Utilities.putNew(this.gces, op, result);
            }
        }

        @Override
        public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
            super.postorder(operator);
            DBSPIntegrateTraceRetainValuesOperator repl = this.mapped(operator.getOutput(0))
                    .operator.to(DBSPIntegrateTraceRetainValuesOperator.class);
            Utilities.putNew(this.original, operator, repl);
        }
    }
}
