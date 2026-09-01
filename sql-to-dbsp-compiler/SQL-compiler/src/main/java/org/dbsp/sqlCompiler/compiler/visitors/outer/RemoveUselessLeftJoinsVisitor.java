package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.keys.KeyAnalysis;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.util.Logger;

import javax.annotation.Nullable;

/** Removes a LEFT JOIN whose right input contributes nothing: the join function reads
 * no field of the right value, and the right input is keyed by the join index, so every
 * left row matches at most once.  The output is then exactly the left input, with the
 * join function applied to each (key, value) pair. */
public class RemoveUselessLeftJoinsVisitor extends CircuitCloneVisitor {
    final KeyAnalysis keys;

    public RemoveUselessLeftJoinsVisitor(DBSPCompiler compiler, KeyAnalysis keys) {
        super(compiler, false);
        this.keys = keys;
    }

    /** The join function applied to the (key, value) pairs of the left input alone.
     * @return null if the output depends on the right value, or if a left row can match
     *         several right rows. */
    @Nullable
    DBSPClosureExpression leftOnly(DBSPJoinBaseOperator join) {
        if (!this.keys.getKeys(join.right()).hasKeyWithinIndex())
            return null;
        DBSPClosureExpression function = join.getClosureFunction();
        DBSPVariablePath pair = join.left().getOutputIndexedZSetType().getKVRefType().var();
        DBSPExpression rightValue = new NoExpression(function.parameters[2].getType());
        DBSPExpression body = function.call(pair.field(0), pair.field(1), rightValue).reduce(this.compiler());
        if (FilterJoinVisitor.ContainsNoExpression.search(this.compiler(), body))
            return null;
        return body.closure(pair);
    }

    void replace(DBSPJoinBaseOperator join, DBSPSimpleOperator replacement) {
        Logger.INSTANCE.belowLevel(this, 1)
                .append("Removing useless ")
                .appendSupplier(join::toString)
                .append(" from ")
                .appendSupplier(() -> join.getRelNode().toString())
                .newline();
        this.compiler.reportWarning(join.getSourcePosition(), "LEFT JOIN has no effect",
                "Removed a LEFT JOIN that adds no columns to its result and matches at most " +
                        "one row for each row of its left input");
        this.map(join, replacement);
    }

    @Override
    public void postorder(DBSPLeftJoinOperator join) {
        DBSPClosureExpression function = this.leftOnly(join);
        if (function == null) {
            super.postorder(join);
            return;
        }
        this.replace(join, new DBSPMapOperator(join.getRelNode(), function,
                join.getOutputZSetType(), join.isMultiset, this.mapped(join.left())));
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator join) {
        DBSPClosureExpression function = this.leftOnly(join);
        if (function == null) {
            super.postorder(join);
            return;
        }
        this.replace(join, new DBSPMapIndexOperator(join.getRelNode(), function,
                join.getOutputIndexedZSetType(), join.isMultiset, this.mapped(join.left())));
    }
}
