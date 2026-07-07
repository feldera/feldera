package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPConstantOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIndexedZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** Try to optimize operators applied to constant inputs */
public class PropagateConstants extends CircuitCloneVisitor {
    public PropagateConstants(DBSPCompiler compiler) {
        super(compiler, false);
    }

    boolean inputIsConstant(OutputPort port) {
        return port.node().is(DBSPConstantOperator.class);
    }

    @Nullable
    public static DBSPConstantOperator filterConstant(DBSPCompiler compiler,
            DBSPConstantOperator constant, DBSPClosureExpression filter) {
        DBSPExpression value = constant.getFunction();
        Simplify simplify = new Simplify(compiler);
        if (value.is(DBSPZSetExpression.class)) {
            DBSPZSetExpression set = value.to(DBSPZSetExpression.class);
            Map<DBSPExpression, Long> result = new HashMap<>();
            boolean evaluated = true;
            for (var entry : set.data.entrySet()) {
                DBSPExpression filtered = filter.call(entry.getKey().borrow()).reduce(compiler);
                DBSPExpression simplified = simplify.apply(filtered).to(DBSPExpression.class);
                if (simplified.is(DBSPBoolLiteral.class)) {
                    DBSPBoolLiteral b = simplified.to(DBSPBoolLiteral.class);
                    if (b.value == null) {
                        // Should not happen, since filter expressions should not be nullable,
                        // but we are a bit paranoid.
                        evaluated = false;
                        break;
                    }
                    if (b.value) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    // Could not evaluate filter
                    evaluated = false;
                    break;
                }
            }
            if (evaluated) {
                DBSPZSetExpression zset = new DBSPZSetExpression(result, set.elementType);
                boolean isDistinct = !constant.isMultiset || zset.isCertainlyDistinct();
                return new DBSPConstantOperator(constant.getRelNode(), zset, !isDistinct);
            }
        }
        return null;
    }

    @Nullable
    public static DBSPConstantOperator mapConstant(
            DBSPCompiler compiler, DBSPConstantOperator constant, DBSPClosureExpression map) {
        DBSPExpression value = constant.getFunction();
        Simplify simplify = new Simplify(compiler);
        if (value.is(DBSPZSetExpression.class)) {
            DBSPZSetExpression set = value.to(DBSPZSetExpression.class);
            DBSPZSetExpression result = new DBSPZSetExpression(map.getResultType());
            boolean evaluated = true;
            for (var entry : set.data.entrySet()) {
                DBSPExpression mapped = map.call(entry.getKey().borrow()).reduce(compiler);
                DBSPExpression simplified = simplify.apply(mapped).to(DBSPExpression.class);
                if (simplified.isCompileTimeConstant()) {
                    result.append(simplified, entry.getValue());
                } else {
                    // Could not evaluate map
                    evaluated = false;
                    break;
                }
            }
            if (evaluated) {
                return new DBSPConstantOperator(constant.getRelNode(), result, !result.isCertainlyDistinct());
            }
        }
        return null;
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (!this.inputIsConstant(source)) {
            super.postorder(operator);
            return;
        }
        DBSPConstantOperator constant = source.simpleNode().to(DBSPConstantOperator.class);
        DBSPConstantOperator filteredConstant = filterConstant(this.compiler, constant, operator.getClosureFunction());
        if (filteredConstant != null) {
            this.map(operator, filteredConstant);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPDistinctOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (source.isSimpleNode() && !source.simpleNode().isMultiset) {
            // This includes constants
            this.map(operator, source.simpleNode(), false);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPNegateOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (!this.inputIsConstant(source)) {
            super.postorder(operator);
            return;
        }
        DBSPConstantOperator constant = source.simpleNode().to(DBSPConstantOperator.class);
        DBSPZSetExpression value = constant.getFunction().as(DBSPZSetExpression.class);
        if (value != null) {
            value = value.negate();
            DBSPConstantOperator result = new DBSPConstantOperator(operator.getRelNode(), value, !value.isCertainlyDistinct());
            this.map(operator, result);
            return;
        }
        DBSPIndexedZSetExpression ix = constant.getFunction().as(DBSPIndexedZSetExpression.class);
        if (ix != null) {
            ix = ix.negate();
            DBSPConstantOperator result = new DBSPConstantOperator(operator.getRelNode(), ix, !ix.isEmpty());
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        OutputPort source = this.mapped(operator.input());
        if (!this.inputIsConstant(source)) {
            super.postorder(operator);
            return;
        }
        DBSPConstantOperator constant = source.simpleNode().to(DBSPConstantOperator.class);
        DBSPZSetExpression value = constant.getFunction().as(DBSPZSetExpression.class);
        if (value != null) {
            DBSPClosureExpression map = operator.getClosureFunction();
            DBSPConstantOperator result = mapConstant(this.compiler, constant, map);
            if (result != null) {
                this.map(operator, result);
                return;
            }
        }
        super.postorder(operator);
    }
}
