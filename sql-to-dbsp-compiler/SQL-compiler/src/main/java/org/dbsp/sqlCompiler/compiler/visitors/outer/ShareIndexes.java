package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Find patterns where the same collection is indexed twice on the same key with different values
 * (followed by an integral) and try to share the indexing by expanding the values. */
public class ShareIndexes extends Passes {
    public ShareIndexes(DBSPCompiler compiler) {
        super("ShareIndexes", compiler);
        Graph graph = new Graph(compiler);
        this.add(graph);
        FindSharedIndexes shared = new FindSharedIndexes(compiler, graph.getGraphs());
        this.add(shared);
        this.add(new ReplaceSharedIndexes(compiler, shared));
    }

    record MapIndexAndConsumer(DBSPMapIndexOperator index, DBSPJoinBaseOperator consumer, boolean leftInput) {
        @Override
        public String toString() {
            return "IxJ[" + this.index + ", " + this.consumer + "]";
        }
    }

    /** Helper class which combines functions from multiple {@link DBSPMapIndexOperator} to produce a single
     * {@link DBSPMapIndexOperator} operator */
    static class WideMapIndexBuilder {
        final CalciteRelNode node;
        public final DBSPVariablePath var;
        final DBSPExpression keyExpression;
        final EquivalenceContext eqContext;
        final List<DBSPExpression> outputFields;
        /** For each function the list of outputs it emits as its value */
        final List<List<Integer>> outputIndexes;
        final boolean valueNullable;
        @Nullable
        DBSPMapIndexOperator result = null;

        private WideMapIndexBuilder(CalciteRelNode node, DBSPVariablePath var, DBSPExpression
                keyExpression, boolean valueNullable) {
            this.node = node;
            this.var = var;
            this.outputFields = new ArrayList<>();
            this.outputIndexes = new ArrayList<>();
            this.valueNullable = valueNullable;
            this.keyExpression = keyExpression;
            this.eqContext = new EquivalenceContext();
        }

        @Override
        public String toString() {
            return "WideMapIndexBuilder(" + this.outputIndexes.size() + ")";
        }

        void addFunction(DBSPCompiler compiler, DBSPClosureExpression function) {
            Utilities.enforce(function.parameters.length == 1);
            Utilities.enforce(function.parameters[0].getType().sameType(this.var.type));
            DBSPTypeRawTuple resultType = function.getResultType().to(DBSPTypeRawTuple.class);
            Utilities.enforce(resultType.size() == 2);
            DBSPTypeTuple valueType = resultType.tupFields[1].to(DBSPTypeTuple.class);
            Utilities.enforce(valueType.mayBeNull == this.valueNullable);
            List<Integer> currentOutputs = new ArrayList<>(valueType.size());
            for (int i = 0; i < valueType.size(); i++) {
                DBSPExpression outputI = function.call(this.var).field(1).field(i).reduce(compiler);
                boolean found = false;
                List<DBSPExpression> fields = this.outputFields;
                for (int j = 0; j < fields.size(); j++) {
                    DBSPExpression outputJ = fields.get(j);
                    if (this.eqContext.equivalent(outputI, outputJ)) {
                        currentOutputs.add(j);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    currentOutputs.add(this.outputFields.size());
                    this.outputFields.add(outputI);
                }
            }
            this.outputIndexes.add(currentOutputs);
        }

        public static WideMapIndexBuilder create(
                CalciteRelNode node, DBSPCompiler compiler, List<DBSPClosureExpression> closures) {
            Utilities.enforce(closures.size() > 1);
            DBSPClosureExpression first = closures.get(0);
            Utilities.enforce(first.parameters.length == 1);
            DBSPVariablePath var = first.parameters[0].type.var();
            boolean valueNullable = first.getResultType().to(DBSPTypeRawTuple.class).tupFields[1].mayBeNull;
            DBSPExpression keyExpression = first.call(var).field(0).reduce(compiler);
            WideMapIndexBuilder result = new WideMapIndexBuilder(node, var, keyExpression, valueNullable);
            for (var clo: closures)
                result.addFunction(compiler, clo);
            return result;
        }

        /** Create the MapIndex operator represented by this builder if it does not exist. */
        DBSPMapIndexOperator build(OutputPort input) {
            if (this.result == null) {
                DBSPClosureExpression closure = new DBSPRawTupleExpression(
                        this.keyExpression,
                        new DBSPTupleExpression(this.outputFields, this.valueNullable)).closure(this.var);
                this.result = new DBSPMapIndexOperator(this.node, closure, input);
            }
            return this.result;
        }

        DBSPMapIndexOperator get() {
            Utilities.enforce(this.result != null);
            return this.result;
        }
    }

    /** Finds patterns that can be combined */
    static class FindSharedIndexes extends CircuitWithGraphsVisitor {
        /** Map from source to a set of destinations which can be combined */
        final Map<OutputPort, List<MapIndexAndConsumer>> clusters;
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

        boolean sameValueNullability(DBSPClosureExpression first, DBSPClosureExpression second) {
            return first.getResultType().to(DBSPTypeRawTuple.class).tupFields[1].mayBeNull
                    == second.getResultType().to(DBSPTypeRawTuple.class).tupFields[1].mayBeNull;
        }

        public FindSharedIndexes(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.clusters = new HashMap<>();
            this.visited = new HashSet<>();
        }

        /** True if the node has a single successor which requires an integrator on this input */
        @Nullable
        static MapIndexAndConsumer followedByIntegrator(CircuitGraph graph, DBSPMapIndexOperator operator) {
            List<Port<DBSPOperator>> successors = graph.getSuccessors(operator);
            if (successors.size() > 1)
                return null;
            // Currently only handle joins
            DBSPOperator successor = successors.get(0).node();
            if (successor.is(DBSPJoinBaseOperator.class)) {
                var join = successor.to(DBSPJoinBaseOperator.class);
                boolean isLeftInput = join.left().operator == operator;
                return new MapIndexAndConsumer(operator, join, isLeftInput);
            }
            return null;
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            boolean process = !this.visited.contains(operator);
            OutputPort input = operator.input();

            CircuitGraph graph = this.getGraph();
            List<Port<DBSPOperator>> siblings = graph.getSuccessors(input.node());
            if (siblings.size() < 2)
                process = false;
            MapIndexAndConsumer pair = followedByIntegrator(graph, operator);
            Projection projection = new Projection(this.compiler, true, false);
            projection.apply(operator.getClosureFunction());
            if (!projection.isProjection)
                process = false;
            if (process && pair != null) {
                for (Port<DBSPOperator> port : siblings) {
                    DBSPOperator sibling = port.node();
                    if (!sibling.is(DBSPMapIndexOperator.class))
                        continue;
                    if (sibling == operator)
                        continue;
                    DBSPMapIndexOperator smi = sibling.to(DBSPMapIndexOperator.class);
                    projection.apply(smi.getClosureFunction());
                    if (!projection.isProjection)
                        continue;
                    if (!this.sameKey(operator.getClosureFunction(), smi.getClosureFunction()))
                        continue;
                    if (!this.sameValueNullability(operator.getClosureFunction(), smi.getClosureFunction()))
                        continue;
                    MapIndexAndConsumer nextPair = followedByIntegrator(graph, smi);
                    if (nextPair != null) {
                        this.visited.add(operator);
                        this.visited.add(nextPair.index);
                        if (!this.clusters.containsKey(input)) {
                            Utilities.putNew(this.clusters, input, new ArrayList<>());
                            this.clusters.get(input).add(pair);
                        }
                        this.clusters.get(input).add(nextPair);
                    }
                }
                var list = this.clusters.get(input);
                if (list != null)
                    System.out.println(list);
            }
            super.postorder(operator);
        }
    }

    record VarAndExpression(DBSPVariablePath var, DBSPExpression expression) {}

    record JoinSource(WideMapIndexBuilder builder, int consumerIndex) {
        /** Create an expression that represents the value part of the field of the new join input */
        public VarAndExpression createValue() {
            DBSPMapIndexOperator source = this.builder.get();
            // This is the new input for this join input
            var newVar = source.getOutputIndexedZSetType().elementType.ref().var();
            List<DBSPExpression> valueFields = new ArrayList<>();
            // This is the list of fields from the value produced by the MapIndex that this join consumes
            List<Integer> outputIndexes = builder.outputIndexes.get(consumerIndex);
            for (int index: outputIndexes) {
                valueFields.add(newVar.deref().field(index).applyCloneIfNeeded());
            }
            return new VarAndExpression(newVar,
                    new DBSPTupleExpression(valueFields, builder.valueNullable).borrow());
        }

        @Override
        public String toString() {
            return this.builder + "[" + this.consumerIndex + "]";
        }
    }

    static class JoinInputs {
        @Nullable JoinSource left = null;
        @Nullable JoinSource right = null;

        void setLeft(JoinSource left) {
            Utilities.enforce(this.left == null);
            this.left = left;
        }

        void setRight(JoinSource right) {
            Utilities.enforce(this.right == null);
            this.right = right;
        }

        @Override
        public String toString() {
            return "L=" + (this.left != null ? this.left.toString() : "-") + " R=" +
                    (this.right != null ? this.right.toString() : "-");
        }
    }

    static class ReplaceSharedIndexes extends CircuitCloneVisitor {
        final FindSharedIndexes finder;
        /** Maps the operators with integrals to the information needed to synthesize their inputs */
        final Map<DBSPJoinBaseOperator, JoinInputs> combinations;

        public ReplaceSharedIndexes(DBSPCompiler compiler, FindSharedIndexes finder) {
            super(compiler, false);
            this.finder = finder;
            this.combinations = new HashMap<>();
        }

        @Override
        public Token startVisit(IDBSPOuterNode circuit) {
            for (var pairs: finder.clusters.values()) {
                List<DBSPClosureExpression> functions = Linq.map(pairs, s -> s.index().getClosureFunction());
                WideMapIndexBuilder builder = WideMapIndexBuilder.create(pairs.get(0).index.getRelNode(), this.compiler, functions);
                for (int i = 0; i < pairs.size(); i++) {
                    MapIndexAndConsumer mi = pairs.get(i);
                    DBSPJoinBaseOperator join = mi.consumer;
                    JoinSource cvi = new JoinSource(builder, i);
                    if (!this.combinations.containsKey(join))
                        Utilities.putNew(this.combinations, join, new JoinInputs());
                    if (mi.leftInput)
                        this.combinations.get(join).setLeft(cvi);
                    else
                        this.combinations.get(join).setRight(cvi);
                }
            }
            return super.startVisit(circuit);
        }

        DBSPClosureExpression rewriteJoinClosure(
                DBSPClosureExpression closure,
                JoinInputs inputs) {

            DBSPVariablePath keyVar = closure.parameters[0].type.var();
            DBSPVariablePath leftVar = closure.parameters[1].type.var();
            DBSPVariablePath rightVar = closure.parameters[2].type.var();
            DBSPExpression leftValue = leftVar;
            if (inputs.left != null) {
                var pair = inputs.left.createValue();
                leftValue = pair.expression;
                leftVar = pair.var;
            }
            DBSPExpression rightValue = rightVar;
            if (inputs.right != null) {
                var pair = inputs.right.createValue();
                rightValue = pair.expression;
                rightVar = pair.var;
            }
            DBSPExpression call = closure.call(keyVar, leftValue, rightValue);
            DBSPExpression reduced = call.reduce(this.compiler);
            return reduced.closure(keyVar, leftVar, rightVar);
        }

        @Override
        public void postorder(DBSPLeftJoinOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPStreamJoinOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPJoinOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPJoinIndexOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPLeftJoinIndexOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        public boolean processJoin(DBSPJoinBaseOperator operator) {
            JoinInputs joinInputs = this.combinations.get(operator);
            if (joinInputs == null)
                return false;

            Utilities.enforce(joinInputs.left != null || joinInputs.right != null);
            OutputPort left = this.mapped(operator.left());
            OutputPort right = this.mapped(operator.right());
            if (joinInputs.left != null) {
                var builder = joinInputs.left.builder;
                OutputPort leftParent = this.mapped(left.node().inputs.get(0));
                var mapIndex = builder.build(leftParent);
                if (!this.getUnderConstruction().contains(mapIndex))
                    this.addOperator(mapIndex);
                left = mapIndex.outputPort();
            }
            if (joinInputs.right != null) {
                var builder = joinInputs.right.builder;
                OutputPort rightParent = this.mapped(right.node().inputs.get(0));
                var mapIndex = builder.build(rightParent);
                if (!this.getUnderConstruction().contains(mapIndex))
                    this.addOperator(mapIndex);
                right = mapIndex.outputPort();
            }

            // Must be done after the inputs are created
            DBSPClosureExpression joinClosure = this.rewriteJoinClosure(operator.getClosureFunction(), joinInputs);
            var newJoin = operator.withFunctionAndInputs(joinClosure, left, right);
            this.map(operator, newJoin);
            return true;
        }
    }
}
