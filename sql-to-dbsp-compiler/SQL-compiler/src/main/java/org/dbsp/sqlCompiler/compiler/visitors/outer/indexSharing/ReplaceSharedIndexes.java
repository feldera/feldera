package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Works in tandem with FindSharedIndexes; it replaces multiple
 {@link org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator} with a single wide one
 by adjusting the consumers */
class ReplaceSharedIndexes extends CircuitCloneVisitor {
    final FindSharedIndexes finder;
    /**
     * Maps the operators with integrals to the information needed to synthesize their inputs
     */
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
        for (var pairs : this.finder.clusters) {
            Logger.INSTANCE.belowLevel(ShareIndexes.class, 1)
                    .append(count)
                    .append(". ")
                    .append(pairs.size() + " indexes")
                    .newline();
            // System.out.println(pairs);
            List<DBSPClosureExpression> functions = Linq.map(pairs, s -> s.index().getClosureFunction());
            WideMapIndexBuilder builder = WideMapIndexBuilder.create(pairs.get(0).index().getRelNode(), this.compiler, functions);
            for (int i = 0; i < pairs.size(); i++) {
                FindSharedIndexes.MapIndexAndConsumer mi = pairs.get(i);
                DBSPJoinBaseOperator join = mi.consumer();
                JoinSource cvi = new JoinSource(builder, i);
                if (!this.combinations.containsKey(join))
                    Utilities.putNew(this.combinations, join, new JoinInputs());
                if (mi.leftInput())
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
            var builder = joinInputs.left.builder();
            OutputPort leftParent = left.node().inputs.get(0);
            var mapIndex = builder.build(leftParent);
            if (!this.getUnderConstruction().contains(mapIndex))
                this.addOperator(mapIndex);
            left = mapIndex.outputPort();
        }
        if (joinInputs.right != null) {
            var builder = joinInputs.right.builder();
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

    static class JoinInputs {
        @Nullable
        JoinSource left = null;
        @Nullable
        JoinSource right = null;

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
}
