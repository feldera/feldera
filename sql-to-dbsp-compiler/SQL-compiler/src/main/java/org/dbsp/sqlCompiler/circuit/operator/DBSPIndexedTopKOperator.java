package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Apply a topK operation to each of the groups in an indexed collection.
 * This always sorts the elements of each group.
 * To sort the entire collection just group by ().
 * The function is a DBSPSortExpression used to compare two elements.
 */
public class DBSPIndexedTopKOperator extends DBSPUnaryOperator {
    public final DBSPExpression limit;

    public DBSPIndexedTopKOperator(CalciteObject node, DBSPExpression function, DBSPExpression limit, DBSPOperator source) {
        super(node, "topk_custom_order", function, source.outputType, source.isMultiset, source);
        this.limit = limit;
        if (!source.outputType.is(DBSPTypeIndexedZSet.class))
            throw new InternalCompilerError("Expected the input to be an IndexedZSet type", source.outputType);
        if (!function.is(DBSPComparatorExpression.class))
            throw new InternalCompilerError("Expected a comparator expression", function);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIndexedTopKOperator(this.getNode(), this.getFunction(), this.limit, newInputs.get(0));
        return this;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPIndexedTopKOperator(this.getNode(), Objects.requireNonNull(expression), this.limit, this.input());
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }
}
