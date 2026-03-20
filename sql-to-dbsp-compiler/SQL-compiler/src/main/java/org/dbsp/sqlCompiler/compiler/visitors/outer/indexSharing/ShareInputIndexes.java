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
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.RemoveIdentityOperators;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** An input with a primary key is indexes.  In the circuit it may be
 * followed by another MapIndex, which may feed a Join.  Remove that index
 * if it has the same key and use the input directly. */
public class ShareInputIndexes extends CircuitCloneVisitor {
    public ShareInputIndexes(DBSPCompiler compiler) {
        super(compiler, false);
    }

    record VarAndExpression(DBSPVariablePath var, DBSPExpression expression) {}

    static class JoinSource {
        final DBSPMapIndexOperator joinInput;
        /** Closure of a MapIndex operator that feeds this join input;
         * the input comes from an IndexedZSet. */
        final DBSPClosureExpression closure;

        JoinSource(DBSPMapIndexOperator operator) {
            this.closure = operator.getClosureFunction();
            this.joinInput = operator;
        }

        /** Returns a variable and an expression.
         * The result will be used in the join to obtain the input by bypassing the
         * current input.
         * @param keyVar  Variable used for the key join closure.
         */
        VarAndExpression createVariableAndValue(DBSPVariablePath keyVar) {
            DBSPVariablePath var = this.closure.parameters[0].getType().to(DBSPTypeRawTuple.class).tupFields[1].var();
            return new VarAndExpression(
                var,
                this.closure.call(new DBSPRawTupleExpression(keyVar, var)).field(1).borrow());
        }

        public OutputPort newSource() {
            return this.joinInput.input();
        }
    }

    static class JoinInputs {
        @Nullable
        JoinSource left = null;
        @Nullable
        JoinSource right = null;

        void setLeft(@Nullable JoinSource left) {
            Utilities.enforce(this.left == null);
            this.left = left;
        }

        void setRight(@Nullable JoinSource right) {
            Utilities.enforce(this.right == null);
            this.right = right;
        }

        @Override
        public String toString() {
            return "L=" + (this.left != null ? this.left.toString() : "-") + " R=" +
                    (this.right != null ? this.right.toString() : "-");
        }
    }

    DBSPClosureExpression rewriteJoinClosure(
            DBSPClosureExpression closure,
            JoinInputs inputs) {

        DBSPVariablePath keyVar = closure.parameters[0].type.var();
        DBSPVariablePath leftVar = closure.parameters[1].type.var();
        DBSPVariablePath rightVar = closure.parameters[2].type.var();
        DBSPExpression leftValue = leftVar;
        if (inputs.left != null) {
            var pair = inputs.left.createVariableAndValue(keyVar);
            leftValue = pair.expression().reduce(this.compiler);
            leftVar = pair.var();
        }
        DBSPExpression rightValue = rightVar;
        if (inputs.right != null) {
            var pair = inputs.right.createVariableAndValue(keyVar);
            rightValue = pair.expression().reduce(this.compiler);
            rightVar = pair.var();
        }
        DBSPExpression call = closure.call(keyVar, leftValue, rightValue);
        DBSPExpression reduced = call.reduce(this.compiler);
        return reduced.closure(keyVar, leftVar, rightVar);
    }

    @Nullable
    JoinSource joinInput(OutputPort input) {
        DBSPMapIndexOperator mx = input.node().as(DBSPMapIndexOperator.class);
        if (mx == null || mx.getOutputIndexedZSetType().elementType.mayBeNull)
            return null;
        var table = mx.input().node().as(DBSPSourceMapOperator.class);
        if (table == null)
            return null;
        // Check if the MapIndex key is the identity function
        DBSPClosureExpression ix = mx.getClosureFunction();
        DBSPTypeRawTuple tuple = ix.parameters[0].getType().to(DBSPTypeRawTuple.class);
        DBSPVariablePath var = tuple.tupFields[0].var();
        DBSPExpression no = new NoExpression(tuple.tupFields[1]);
        DBSPClosureExpression key = ix
                .call(new DBSPRawTupleExpression(var, no))
                .field(0)
                .closure(var)
                .reduce(this.compiler)
                .to(DBSPClosureExpression.class);
        if (!RemoveIdentityOperators.isIdentityFunction(key))
            return null;
        return new JoinSource(mx);
    }

    boolean processJoin(DBSPJoinBaseOperator join) {
        JoinInputs inputs = new JoinInputs();
        var left = this.mapped(join.left());
        var right = this.mapped(join.right());
        JoinSource newLeft = this.joinInput(left);
        inputs.setLeft(newLeft);
        JoinSource newRight = this.joinInput(right);
        inputs.setRight(newRight);

        if (inputs.left != null || inputs.right != null) {
            if (inputs.left != null)
                left = inputs.left.newSource();
            if (inputs.right != null)
                right = inputs.right.newSource();

            DBSPClosureExpression joinClosure = this.rewriteJoinClosure(join.getClosureFunction(), inputs);
            var newJoin = join.withFunctionAndInputs(joinClosure, left, right);
            this.map(join, newJoin);
            return true;
        }

        return false;
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
