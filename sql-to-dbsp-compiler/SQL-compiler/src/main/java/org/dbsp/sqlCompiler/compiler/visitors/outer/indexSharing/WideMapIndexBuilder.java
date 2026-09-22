package org.dbsp.sqlCompiler.compiler.visitors.outer.indexSharing;

import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.util.ExplicitShuffle;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;

/** Helper class which combines functions from multiple {@link DBSPMapIndexOperator} to produce a single
 * {@link DBSPMapIndexOperator} operator */
class WideMapIndexBuilder {
    final CalciteRelNode node;
    public final DBSPVariablePath var;
    final DBSPExpression keyExpression;
    final EquivalenceContext eqContext;
    final List<DBSPExpression> outputFields;
    /** For each function the list of outputs it emits as its value */
    final List<List<Integer>> outputIndexes;
    final boolean valueNullable;
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
                outputI = outputI.neverFailsUnwrap(outputI.getNode());
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

    /** True if the function merged at position {@code function} emits the shared value
     * unchanged. */
    boolean emitsIdentityPermutation(int functionIndex) {
        List<Integer> fields = this.outputIndexes.get(functionIndex);
        if (fields.size() != this.outputFields.size())
            return false;
        return new ExplicitShuffle(this.outputFields.size(), fields).isIdentityPermutation();
    }

    /** Merge the index functions of the members, in list order, into one wide index.
     * @param members  Members of one candidate set; each computes an index over the same
     *                 source, with the same key and the same value nullability, and the
     *                 result describes {@code members.get(i)} at position {@code i}. */
    static WideMapIndexBuilder create(
            DBSPCompiler compiler, List<FindSharedIndexes.MapIndexAndConsumer> members) {
        List<DBSPClosureExpression> functions = Linq.map(members, m -> m.index().getClosureFunction());
        return create(members.get(0).index().getRelNode(), compiler, functions);
    }

    /** True if every member that needs a fixed index reads the entire shared value, in the
     * order this builder produces it.
     * @param members  The members merged into this builder, in the order it merged them. */
    boolean reproducesFixedValues(List<FindSharedIndexes.MapIndexAndConsumer> members) {
        for (int i = 0; i < members.size(); i++)
            if (members.get(i).requiresFixedIndex() && !this.emitsIdentityPermutation(i))
                return false;
        return true;
    }

    /** The function of the wide MapIndex: it computes the shared key and the shared value */
    DBSPClosureExpression closure() {
        return new DBSPRawTupleExpression(
                this.keyExpression,
                new DBSPTupleExpression(this.outputFields, this.valueNullable)).closure(this.var);
    }

    /** Type of the value that the wide MapIndex computes */
    DBSPType valueType() {
        return new DBSPTupleExpression(this.outputFields, this.valueNullable).getType();
    }
}
