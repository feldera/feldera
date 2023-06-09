package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPStructExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.util.Linq;
import org.dbsp.util.Unimplemented;

import java.util.Map;

/**
 * A visitor that converts literals (e.g., DBSPZSetLiteral) into Rust expressions
 * that can be used to initialize JIT data structures.
 * A DBSPZSetLiteral is converted to a Rust StreamCollection.
 */
public class ToRustJitLiteral extends InnerRewriteVisitor {
    public ToRustJitLiteral(IErrorReporter reporter) {
        super(reporter);
    }

    void constant(String type, DBSPLiteral argument) {
        DBSPExpression result;
        if (argument.isNull) {
            result = new DBSPStructExpression(
                    DBSPTypeAny.INSTANCE.path(new DBSPPath("NullableConstant", "null")),
                    DBSPTypeAny.INSTANCE);
        } else {
            result = new DBSPStructExpression(
                    DBSPTypeAny.INSTANCE.path(
                            new DBSPPath("Constant", type)),
                    DBSPTypeAny.INSTANCE,
                    argument.getNonNullable());
            if (argument.mayBeNull()) {
                result = new DBSPStructExpression(
                        DBSPTypeAny.INSTANCE.path(new DBSPPath("Nullable")),
                        DBSPTypeAny.INSTANCE, result.some());
            } else {
                result = new DBSPStructExpression(
                        DBSPTypeAny.INSTANCE.path(new DBSPPath("NonNull")),
                        DBSPTypeAny.INSTANCE, result);
            }
        }
        this.map(argument, result);
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral node) {
         this.constant("Usize", node);
         return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral node) {
        this.constant("Isize", node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal node) {
        this.constant("I64", node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal node) {
        this.constant("I32", node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFloatLiteral node) {
        this.constant("F32", node.raw());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral node) {
        this.constant("F64", node.raw());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral node) {
        this.constant("Bool", node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral node) {
        this.constant("String", node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression node) {
        DBSPExpression[] columns = Linq.map(node.fields, this::transform, DBSPExpression.class);
        DBSPExpression result = new DBSPVecLiteral(columns);
        this.map(node, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        throw new Unimplemented(expression);
    }

    DBSPExpression convertRow(Map.Entry<DBSPExpression, Long> row) {
        DBSPExpression key = this.transform(row.getKey());
        DBSPExpression r = new DBSPStructExpression(
                DBSPTypeAny.INSTANCE.path(new DBSPPath("RowLiteral", "new")),
                DBSPTypeAny.INSTANCE, key);
        DBSPExpression value = new DBSPI32Literal(Math.toIntExact(row.getValue()));
        return new DBSPRawTupleExpression(r, value);
    }

    @Override
    public VisitDecision preorder(DBSPZSetLiteral expression) {
        DBSPExpression[] rows = new DBSPExpression[expression.size()];
        int index = 0;
        for (Map.Entry<DBSPExpression, Long> entry : expression.data.data.entrySet()) {
            DBSPExpression row = this.convertRow(entry);
            rows[index++] = row;
        }

        DBSPVecLiteral vec;
        if (rows.length == 0) {
            vec = new DBSPVecLiteral(expression.getElementType());
        } else {
            vec = new DBSPVecLiteral(rows);
        }
        DBSPExpression result = new DBSPStructExpression(
                DBSPTypeAny.INSTANCE.path(new DBSPPath("Set")),
                DBSPTypeAny.INSTANCE, vec);
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
