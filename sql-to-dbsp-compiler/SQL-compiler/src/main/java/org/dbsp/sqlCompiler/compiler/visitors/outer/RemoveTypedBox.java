package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;

/** Removes the {@code TypedBox} wrappers from a circuit.  A {@code TYPEDBOX} is the Rust
 * backend's {@code TypedBox::new}, an identity that boxes a window bound or a waterline into the
 * type-erased form that dbsp's dynamically typed operators take.  The Gen-2 engine evaluates over
 * typed columns and has no erased form, so {@code TYPEDBOX(e)} becomes {@code e} and
 * {@code TypedBox<T, _>} becomes {@code T}. */
public class RemoveTypedBox extends CircuitRewriter {
    public RemoveTypedBox(DBSPCompiler compiler) {
        super(compiler, new Unbox(compiler), true);
        // The operators that produce a window bound or a waterline change their output type
        this.preservesTypes = false;
    }

    /** This operator derives the type of its waterline output as {@code TypedBox<T, _>}, and its
     * consumers dereference the box; the pass can rewrite neither.  InsertLimiters rejects the
     * LATENESS that creates this operator under {@code --gen2}. */
    @Override
    public void postorder(DBSPInputMapWithWaterlineOperator operator) {
        throw new InternalCompilerError("Cannot unbox the waterline output of " + operator, operator);
    }

    static class Unbox extends InnerRewriteVisitor {
        Unbox(DBSPCompiler compiler) {
            super(compiler, false);
        }

        @Override
        public VisitDecision preorder(DBSPUnaryExpression expression) {
            if (expression.opcode != DBSPOpcode.TYPEDBOX)
                return super.preorder(expression);
            this.push(expression);
            DBSPExpression boxed = this.transform(expression.source);
            this.pop(expression);
            this.map(expression, boxed);
            return VisitDecision.STOP;
        }

        /** Catches both a {@code DBSPTypeTypedBox} and the plain {@code DBSPTypeUser} that
         * {@link InnerRewriteVisitor} builds when it rewrites one. */
        @Override
        public VisitDecision preorder(DBSPTypeUser type) {
            if (type.code != DBSPTypeCode.TYPEDBOX)
                return super.preorder(type);
            this.push(type);
            DBSPType boxed = this.transform(type.typeArgs[0]);
            this.pop(type);
            this.map(type, boxed);
            return VisitDecision.STOP;
        }
    }
}
