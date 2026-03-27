package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPChainOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConcreteAsofJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ResolveReferences;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPDerefExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFieldExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;
import org.dbsp.util.graph.Port;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Find patterns where the same collection is indexed twice on the same key with different values
 * (followed by an integral) and try to share the indexing by expanding the values.
 *
 * <pre>
 *        source
 *        /    \
 *    index   index
 *     /         \
 *  join        join
 * </pre>
 * <p>when the two index nodes have the same key, is rewritten as
 * <pre>
 *     source
 *        |
 *      index
 *     /    \
 *  join   join
 * </pre>
 * <p>where the common index produces the union of the fields of the two indexes.
 * The two joins need to have their functions adjusted to read the appropriate fields.
 * */
public class ShareIndexes extends Passes {
    public ShareIndexes(DBSPCompiler compiler) {
        super("ShareIndexes", compiler);
        // Detect patterns of the form
        //    source
        //     /  \
        //   map  mapIndex
        //   ...     ...
        // mapIndex  mapIndex
        //   |       |
        //  join    join
        // And collapse them to
        //    source
        //     /   \
        // mapIndex mapIndex
        //   |       |
        //  join    join
        var mapChains = new FindMapChains(compiler);
        this.add(mapChains);
        this.add(new CollapseSharedChains(compiler, mapChains.chains));
        this.add(new DeadCode(compiler, true));

        // Give each join its own copy of a MapIndex
        Graph graph1 = new Graph(compiler);
        this.add(graph1);
        this.add(new DuplicateSharedIndexes(compiler, graph1.getGraphs()));
        this.add(new DeadCode(compiler, true));

        Graph graph2 = new Graph(compiler);
        this.add(graph2);
        FindSharedIndexes shared = new FindSharedIndexes(compiler, graph2.getGraphs());
        this.add(shared);
        this.add(new ReplaceSharedIndexes(compiler, shared));
        this.add(new DeadCode(compiler, true));
    }

    record MapIndexAndConsumer(DBSPMapIndexOperator index, DBSPJoinBaseOperator consumer, boolean leftInput) {
        @Override
        public String toString() {
            return "IxJ[" + this.index + ", " + this.consumer + (this.leftInput ? "L" : "R") + "]";
        }
    }

    /** For each join make sure that the MapIndex preceding it (if it exists) is not shared */
    static class DuplicateSharedIndexes extends CircuitCloneWithGraphsVisitor {
        public DuplicateSharedIndexes(DBSPCompiler compiler, CircuitGraphs graph) {
            super(compiler, graph, false);
        }

        /** If a MapIndex is shared, make a copy */
        @Nullable
        DBSPMapIndexOperator unshareIfNeeded(OutputPort input) {
            DBSPOperator node = input.node();
            if (!node.is(DBSPMapIndexOperator.class))
                return null;
            if (this.getGraph().getFanout(node) == 1)
                return null;
            DBSPMapIndexOperator index = node.to(DBSPMapIndexOperator.class);
            var copy = new DBSPMapIndexOperator(
                    index.getRelNode(), index.getClosureFunction(), index.getOutputIndexedZSetType(),
                    index.isMultiset, this.mapped(index.input()));
            this.addOperator(copy);
            return copy;
        }

        boolean processJoin(DBSPJoinBaseOperator join) {
            OutputPort left;
            OutputPort right;
            var lmi = this.unshareIfNeeded(join.left());
            boolean modified = false;
            if (lmi != null) {
                left = lmi.outputPort();
                modified = true;
            } else {
                left = this.mapped(join.left());
            }
            var rmi = this.unshareIfNeeded(join.right());
            if (rmi != null) {
                right = rmi.outputPort();
                modified = true;
            } else {
                right = this.mapped(join.right());
            }
            if (!modified) return false;
            DBSPSimpleOperator result = join.withInputs(Linq.list(left, right), true)
                    .to(DBSPSimpleOperator.class);
            this.map(join, result);
            return true;
        }

        @Override
        public void postorder(DBSPLeftJoinOperator operator) {
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

        @Override
        public void postorder(DBSPStreamJoinIndexOperator operator) {
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
        public void postorder(DBSPJoinFilterMapOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPLeftJoinFilterMapOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

    }

    /** Find if the node has a single successor which requires an integrator on this input */
    @Nullable
    static MapIndexAndConsumer followedByIntegrator(CircuitGraph graph, DBSPMapIndexOperator operator) {
        List<Port<DBSPOperator>> successors = graph.getSuccessors(operator);
        if (successors.size() > 1)
            return null;
        // Currently only handle joins
        DBSPOperator successor = successors.get(0).node();
        if (successor.is(DBSPJoinBaseOperator.class) &&
                !successor.is(DBSPAsofJoinOperator.class) &&
                !successor.is(DBSPConcreteAsofJoinOperator.class)) {
            var join = successor.to(DBSPJoinBaseOperator.class);
            boolean isLeftInput = join.left().operator == operator;
            return new MapIndexAndConsumer(operator, join, isLeftInput);
        }
        return null;
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
                // For a closure of the form clo = (TupX::new(...), Some(TupY::new(a, b, c))
                // we will need to synthesize in the combined MapIndex a new closure of the
                // form (TupX::new(...), Some(TupZ::new(a, b, c, ...)).
                DBSPExpression outputI = function.call(this.var).field(1).field(i);
                if (!valueType.getFieldType(i).mayBeNull && outputI.getType().mayBeNull)
                    outputI = outputI.neverFailsUnwrap();
                outputI = outputI.reduce(compiler);
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

    record MapChain(List<DBSPUnaryOperator> operators) {
        DBSPUnaryOperator head() {
            return Utilities.last(this.operators);
        }

        public DBSPUnaryOperator tail() {
            return this.operators.get(0);
        }
    }

    /** Finds chains of operators in the graph that end in an operator with an integral
     * and are formed only of Map and MapIndex. */
    static class FindMapChains extends CircuitVisitor {
        final List<MapChain> chains;

        protected FindMapChains(DBSPCompiler compiler) {
            super(compiler);
            this.chains = new ArrayList<>();
        }

        List<DBSPUnaryOperator> findMapChain(OutputPort input) {
            List<DBSPUnaryOperator> list = new ArrayList<>();
            while (input.node().is(DBSPMapIndexOperator.class) ||
                input.node().is(DBSPMapOperator.class)) {
                list.add(input.node().to(DBSPUnaryOperator.class));
                input = input.simpleNode().inputs.get(0);
            }
            return list;
        }

        @Override
        public void postorder(DBSPJoinBaseOperator join) {
            var left = this.findMapChain(join.left());
            if (!left.isEmpty())
                // This can happen when compiling without -i and the input is
                // an integrator, not a MapIndex
                this.chains.add(new MapChain(left));
            var right = this.findMapChain(join.right());
            if (!right.isEmpty())
                this.chains.add(new MapChain(right));
        }
    }

    static class CollapseSharedChains extends CircuitCloneVisitor {
        final List<MapChain> chains;
        /** Key is last operation in each chain */
        final Map<DBSPUnaryOperator, MapChain> tail;

        CollapseSharedChains(DBSPCompiler compiler, List<MapChain> chains) {
            super(compiler, false);
            this.chains = chains;
            this.tail = new HashMap<>();
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            if (!this.tail.containsKey(operator)) {
                super.postorder(operator);
                return;
            }

            MapChain chain = this.tail.get(operator);
            if (chain.operators().size() == 1) {
                // Trivial chain, nothing to do.
                super.postorder(operator);
                return;
            }
            OutputPort input = chain.head().input();
            DBSPType inputType = input.outputType();
            Collections.reverse(chain.operators);
            List<DBSPChainOperator.Computation> list = Linq.map(chain.operators, ChainVisitor::getComputation);
            DBSPChainOperator.ComputationChain cc = new DBSPChainOperator.ComputationChain(inputType, list);
            cc = cc.shrinkMaps(this.compiler);
            if (cc.size() > 1) {
                // If this could not reduce it, the closure will be too complicated to analyze later; give up
                super.postorder(operator);
                return;
            }
            DBSPClosureExpression function = cc.collapse(this.compiler);
            var result = new DBSPMapIndexOperator(
                    operator.getRelNode(), function, operator.getOutputIndexedZSetType(),
                    operator.isMultiset, this.mapped(input));
            this.map(operator, result);
        }

        @Override
        public Token startVisit(IDBSPOuterNode circuit) {
            // Maps the head of a chain to the list of chains descending from it
            final Map<OutputPort, List<MapChain>> head = new HashMap<>();
            for (MapChain map: this.chains) {
                OutputPort chainHead = map.head().input();
                if (head.containsKey(chainHead)) {
                    head.get(chainHead).add(map);
                } else {
                    List<MapChain> chains = new ArrayList<>();
                    chains.add(map);
                    head.put(chainHead, chains);
                }
            }

            // Remove all chains that do not share a head
            head.entrySet().removeIf(e -> e.getValue().size() < 2);
            for (List<MapChain> l: head.values()) {
                for (var c : l) {
                    this.tail.put(c.tail(), c);
                }
            }
            return super.startVisit(circuit);
        }
    }

    /** Finds patterns that can be combined */
    static class FindSharedIndexes extends CircuitWithGraphsVisitor {
        // List of operators that can be combined.
        // Two MapIndex operators can be combined if they
        // - have the same source,
        // - have the same key
        // - have the same value nullability
        // - are followed by a stateful operator
        final List<List<MapIndexAndConsumer>> clusters;
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
            this.clusters = new ArrayList<>();
            this.visited = new HashSet<>();
        }

        // Return non-null if the operator has the requisite shape
        @Nullable
        MapIndexAndConsumer mayBeCombined(CircuitGraph graph, DBSPMapIndexOperator operator) {
            if (this.visited.contains(operator))
                return null;
            MapIndexAndConsumer pair = followedByIntegrator(graph, operator);
            if (pair == null)
                return pair;
            Projection projection = new Projection(this.compiler, true, false);
            projection.apply(operator.getClosureFunction());
            if (!projection.isProjection) return null;
            return pair;
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
                this.visited.add(nextPair.index);
                matching.add(nextPair);
            }

            if (matching.size() > 1)
                this.clusters.add(matching);
        }
    }

    record ParameterIndexMap(DBSPVariablePath var, Map<Integer, Integer> indexRemap) {}

    record ParameterIndexMapSet(Map<DBSPParameter, ParameterIndexMap> map) {
        public ParameterIndexMapSet() {
            this(new HashMap<>());
        }

        void add(DBSPParameter param, ParameterIndexMap map) {
            Utilities.putNew(this.map, param, map);
        }

        @Nullable
        ParameterIndexMap get(DBSPParameter param) {
            return this.map.get(param);
        }
    }

    /** For a parameter param this is given a map from integer to integer and a variable.
     * If the map[a] = b, this rewrites (*param).a to (*var).b */
    static class ParameterIndexRewriter extends ExpressionTranslator {
        final ResolveReferences resolver;
        final ParameterIndexMapSet rewriteMap;

        public ParameterIndexRewriter(DBSPCompiler compiler, ParameterIndexMapSet rewriteMap) {
            super(compiler);
            this.resolver = new ResolveReferences(compiler, false);
            this.rewriteMap = rewriteMap;
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            this.resolver.apply(node);
        }

        @Override
        public void postorder(DBSPVariablePath var) {
            if (this.maybeGet(var) != null) {
                // Already translated
                return;
            }
            var decl = this.resolver.reference.getDeclaration(var);
            if (decl.is(DBSPParameter.class)) {
                var map = this.rewriteMap.get(decl.to(DBSPParameter.class));
                if (map != null) {
                    this.map(var, map.var.deepCopy());
                    return;
                }
            }
            super.postorder(var);
        }

        @Override
        public void postorder(DBSPFieldExpression field) {
            if (this.maybeGet(field) != null) {
                // Already translated
                return;
            }
            if (field.expression.is(DBSPDerefExpression.class)) {
                var deref = field.expression.to(DBSPDerefExpression.class);
                if (deref.expression.is(DBSPVariablePath.class)) {
                    var var = deref.expression.to(DBSPVariablePath.class);
                    var decl = this.resolver.reference.getDeclaration(var);
                    if (decl.is(DBSPParameter.class)) {
                        var map = this.rewriteMap.get(decl.to(DBSPParameter.class));
                        if (map != null) {
                            Integer newField = map.indexRemap.get(field.fieldNo);
                            if (newField == null)
                                newField = field.fieldNo;
                            this.map(field, map.var.deepCopy().deref().field(newField));
                            return;
                        }
                    }
                }
            }
            super.postorder(field);
        }
    }

    record JoinSource(WideMapIndexBuilder builder, int consumerIndex) {
        public ParameterIndexMap getParameterRemap() {
            DBSPMapIndexOperator source = this.builder.get();
            // This is the new input for this join input
            var newVar = source.getOutputIndexedZSetType().elementType.ref().var();

            // This is the list of fields from the value produced by the MapIndex that this join consumes
            List<Integer> outputIndexes = builder.outputIndexes.get(consumerIndex);
            Map<Integer, Integer> remap = new HashMap<>();
            for (int i = 0; i < outputIndexes.size(); i++) {
                int index = outputIndexes.get(i);
                remap.put(i, index);
            }

            return new ParameterIndexMap(newVar, remap);
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
            int count = 1;
            if (!this.finder.clusters.isEmpty()) {
                Logger.INSTANCE.belowLevel(ShareIndexes.class, 1)
                        .append("Shared indexes found:").newline();
            }
            for (var pairs: this.finder.clusters) {
                Logger.INSTANCE.belowLevel(ShareIndexes.class, 1)
                        .append(count)
                        .append(". ")
                        .append(pairs.size() + " indexes")
                        .newline();
                // System.out.println(pairs);
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
                count++;
            }
            return super.startVisit(circuit);
        }

        DBSPClosureExpression rewriteJoinClosure(
                DBSPClosureExpression closure,
                JoinInputs inputs) {

            DBSPVariablePath keyVar = closure.parameters[0].asVariable();
            DBSPVariablePath leftVar = closure.parameters[1].asVariable();
            DBSPVariablePath rightVar = closure.parameters[2].asVariable();
            ParameterIndexMapSet set = new ParameterIndexMapSet();

            if (inputs.left != null) {
                var remap = inputs.left.getParameterRemap();
                leftVar = remap.var;
                set.add(closure.parameters[1], remap);
            }
            if (inputs.right != null) {
                var remap = inputs.right.getParameterRemap();
                rightVar = remap.var;
                set.add(closure.parameters[2], remap);
            }

            ParameterIndexRewriter rewriter = new ParameterIndexRewriter(this.compiler, set);
            DBSPClosureExpression result = rewriter.apply(closure).to(DBSPClosureExpression.class);
            return result.body.closure(keyVar, leftVar, rightVar);
        }

        @Override
        public void postorder(DBSPLeftJoinOperator operator) {
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

        @Override
        public void postorder(DBSPStreamJoinIndexOperator operator) {
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
        public void postorder(DBSPJoinFilterMapOperator operator) {
            if (!this.processJoin(operator)) {
                super.postorder(operator);
            }
        }

        @Override
        public void postorder(DBSPLeftJoinFilterMapOperator operator) {
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
                OutputPort leftParent = left.node().inputs.get(0);
                var mapIndex = builder.build(leftParent);
                if (!this.getUnderConstruction().contains(mapIndex))
                    this.addOperator(mapIndex);
                left = mapIndex.outputPort();
            }
            if (joinInputs.right != null) {
                var builder = joinInputs.right.builder;
                OutputPort rightParent = right.node().inputs.get(0);
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
