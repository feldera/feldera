package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.List;

/** Expands joins that do not have DBSP implementations, such as
 * {@link DBSPStreamAntiJoinOperator} and {@link DBSPStreamJoinIndexOperator}.
 * These are implemented using differentiators and integrators around a non-stream operator.
 * These operators should not appear in incremental circuits. */
public class ImplementJoins extends CircuitCloneVisitor {
    /** Set to 'true' to implement star_join using standard join;
     * set to 'false' to implement it using the native DBSP operator. */
    static final boolean EXPAND_STAR = false;

    public ImplementJoins(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public void postorder(DBSPStreamAntiJoinOperator operator) {
        var inputs = Linq.map(operator.inputs, this::mapped);
        List<OutputPort> diffs = new ArrayList<>(operator.inputs.size());
        for (OutputPort in: inputs) {
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(
                    operator.getRelNode().intermediate(), in);
            this.addOperator(diff);
            diffs.add(diff.outputPort());
        }
        DBSPAntiJoinOperator join = new DBSPAntiJoinOperator(
                operator.getRelNode().intermediate(), diffs.get(0), diffs.get(1));
        this.addOperator(join);
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getRelNode(), join.outputPort());
        this.map(operator, integrator);
    }

    @Override
    public void postorder(DBSPStreamJoinIndexOperator operator) {
        var inputs = Linq.map(operator.inputs, this::mapped);
        List<OutputPort> diffs = new ArrayList<>(operator.inputs.size());
        for (OutputPort in: inputs) {
            DBSPDifferentiateOperator diff = new DBSPDifferentiateOperator(
                    operator.getRelNode().intermediate(), in);
            this.addOperator(diff);
            diffs.add(diff.outputPort());
        }
        DBSPJoinIndexOperator join = new DBSPJoinIndexOperator(
                operator.getRelNode().intermediate(), operator.getOutputIndexedZSetType(), operator.getFunction(),
                operator.isMultiset, diffs.get(0), diffs.get(1), operator.balanced);
        this.addOperator(join);
        DBSPIntegrateOperator integrator = new DBSPIntegrateOperator(operator.getRelNode(), join.outputPort());
        this.map(operator, integrator);
    }

    /** A class representing an expansion of a StarJoinBase operator into a tree of standard joins */
    public static class StarJoinImplementation {
        final DBSPCompiler compiler;
        public final List<DBSPJoinBaseOperator> joins;
        public final DBSPSimpleOperator result;
        final boolean delta;

        /** Expand a {@link DBSPStarJoinOperator} or {@link DBSPStarJoinIndexOperator} operator
         * into a tree of joins.
         * @param compiler   Compiler.
         * @param operator   Operator to expand.
         * @param inputs     Inputs to use for expansion.
         * @param delta      If true use {@link DBSPStreamJoinIndexOperator}s operators in the expansion,
         *                   else use {@link DBSPJoinIndexOperator}s.  This is set to 'true' during
         *                   {@link org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.DeltaExpandOperators}.
         */
        public StarJoinImplementation(DBSPCompiler compiler, DBSPStarJoinBaseOperator operator,
                                      List<OutputPort> inputs, boolean delta) {
            this.compiler = compiler;
            this.joins = new ArrayList<>();
            this.delta = delta;

            DBSPType keyType = operator.inputs.get(0).getOutputIndexedZSetType().keyType;
            var concat = this.concatenateFields(operator.getRelNode(), keyType, inputs);

            DBSPClosureExpression reduced = this.rewriteStarClosure(
                    concat.getOutputIndexedZSetType(), operator.getClosureFunction());
            if (operator.is(DBSPStarJoinOperator.class))
                this.result = new DBSPMapOperator(operator.getRelNode(), reduced, concat);
            else if (operator.is(DBSPStarJoinIndexOperator.class))
                this.result = new DBSPMapIndexOperator(operator.getRelNode(), reduced, concat);
            else
                throw new InternalCompilerError("Unexpected operator " + operator);
        }

        void addOperator(DBSPJoinBaseOperator operator) {
            this.joins.add(operator);
        }

        /** Given two indexed results, join them on the common keys and concatenate the fields */
        private DBSPSimpleOperator combineTwoInputs(
                CalciteRelNode node, DBSPType groupKeyType,
                OutputPort left, OutputPort right) {
            DBSPTypeTupleBase leftType = left.getOutputIndexedZSetType().getElementTypeTuple();
            DBSPTypeTupleBase rightType = right.getOutputIndexedZSetType().getElementTypeTuple();

            DBSPVariablePath leftVar = leftType.ref().var();
            DBSPVariablePath rightVar = rightType.ref().var();

            leftType = leftType.concat(rightType);
            DBSPTypeIndexedZSet joinOutputType = TypeCompiler.makeIndexedZSet(groupKeyType, leftType);

            DBSPVariablePath key = groupKeyType.ref().var();
            DBSPExpression body = new DBSPRawTupleExpression(
                    DBSPTupleExpression.flatten(key.deref()),
                    DBSPTupleExpression.flatten(leftVar.deref(), rightVar.deref()));
            DBSPClosureExpression appendFields = body.closure(key, leftVar, rightVar);

            final DBSPJoinBaseOperator result;
            if (!this.delta) {
                result = new DBSPJoinIndexOperator(
                        node, joinOutputType, appendFields, false, left, right, false);
            } else {
                result = new DBSPStreamJoinIndexOperator(
                        node, joinOutputType, appendFields, false, left, right, false);
            }
            this.addOperator(result);
            return result;
        }

        /** Given a list of tuples, concatenate them using joins in a balanced binary tree. */
        private OutputPort concatenateFields(CalciteRelNode node, DBSPType groupKeyType, List<OutputPort> inputs) {
            if (inputs.size() == 1) {
                return inputs.get(0);
            }

            List<OutputPort> pairs = new ArrayList<>();
            for (int i = 0; i < inputs.size(); i += 2) {
                if (i == inputs.size() - 1) {
                    pairs.add(inputs.get(i));
                } else {
                    DBSPSimpleOperator join = this.combineTwoInputs(
                            node, groupKeyType, inputs.get(i), inputs.get(i + 1));
                    pairs.add(join.outputPort());
                }
            }
            // Recursive call with ~1/2 the number of elements
            return this.concatenateFields(node, groupKeyType, pairs);
        }

        DBSPClosureExpression rewriteStarClosure(DBSPTypeIndexedZSet inputType, DBSPClosureExpression closure) {
            // The closure has parameters with Tuple types, but the field concatenation has flattened all tuples
            // into a big wide one, we have to regroup them
            DBSPExpression[] arguments = new DBSPExpression[closure.parameters.length];
            DBSPVariablePath var = inputType.getKVRefType().var();
            arguments[0] = var.field(0).applyCloneIfNeeded();
            int index = 0;
            for (int i = 1; i < closure.parameters.length; i++) {
                DBSPTypeTuple paramType = closure.parameters[i].type.deref().to(DBSPTypeTuple.class);
                List<DBSPExpression> fields = new ArrayList<>();
                for (int j = 0; j < paramType.size(); j++, index++) {
                    fields.add(var.field(1).deref().field(index).applyCloneIfNeeded());
                }
                arguments[i] = new DBSPTupleExpression(fields, false).borrow();
            }
            DBSPExpression apply = closure.call(arguments).closure(var);
            DBSPClosureExpression result = apply.reduce(this.compiler).to(DBSPClosureExpression.class);
            if (this.delta) {
                result = result.ensureTree(this.compiler).to(DBSPClosureExpression.class);
            }
            return result;
        }
    }

    @Override
    public void postorder(DBSPStarJoinIndexOperator operator) {
        if (EXPAND_STAR) {
            var inputs = Linq.map(operator.inputs, this::mapped);
            StarJoinImplementation impl = new StarJoinImplementation(this.compiler, operator, inputs, false);
            for (var op: impl.joins)
                this.addOperator(op);
            this.map(operator, impl.result);
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPStarJoinOperator operator) {
        if (EXPAND_STAR) {
            var inputs = Linq.map(operator.inputs, this::mapped);
            StarJoinImplementation impl = new StarJoinImplementation(this.compiler, operator, inputs, false);
            for (var op: impl.joins)
                this.addOperator(op);
            this.map(operator, impl.result);
        } else {
            super.postorder(operator);
        }
    }
}
