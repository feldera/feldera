package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPConstructorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPISizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.Linq;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

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

    void constant(DBSPTypeCode type, DBSPLiteral literal, DBSPExpression rustValue) {
        DBSPExpression result;
        if (literal.isNull) {
            result = new DBSPConstructorExpression(
                    new DBSPPath("NullableConstant", "null").toExpression(),
                    DBSPTypeAny.getDefault());
        } else {
            result = new DBSPConstructorExpression(
                    new DBSPPath("Constant", type.jitName).toExpression(),
                    DBSPTypeAny.getDefault(),
                    rustValue);
            if (literal.mayBeNull()) {
                result = new DBSPConstructorExpression(
                        new DBSPPath("Nullable").toExpression(),
                        DBSPTypeAny.getDefault(), result.some());
            } else {
                result = new DBSPConstructorExpression(
                        new DBSPPath("NonNull").toExpression(),
                        DBSPTypeAny.getDefault(), result);
            }
        }
        this.map(literal, result);
    }

    void constant(DBSPTypeCode type, DBSPLiteral literal) {
        this.constant(type, literal, literal.isNull ? literal : literal.getWithNullable(false));
    }

    @Override
    public VisitDecision preorder(DBSPTimeLiteral node) {
        TimeString str = node.value;
        DBSPStrLiteral rustLiteral = new DBSPStrLiteral(str == null ? "" : str.toString().trim());
        DBSPExpression expression = new DBSPConstructorExpression(
                new DBSPPath("NaiveTime", "parse_from_str").toExpression(),
                DBSPTypeAny.getDefault(), rustLiteral, new DBSPStrLiteral("%H:%M:%S%.f"));
        this.constant(node.getType().code, node, expression.unwrap());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral node) {
        TimestampString str = node.getTimestampString();
        DBSPStrLiteral rustLiteral = new DBSPStrLiteral(str == null ? "" : str.toString().trim());
        DBSPExpression expression = new DBSPConstructorExpression(
                new DBSPPath("NaiveDateTime", "parse_from_str").toExpression(),
                DBSPTypeAny.getDefault(), rustLiteral, new DBSPStrLiteral("%Y-%m-%d %H:%M:%S%.f"));
        this.constant(node.getType().code, node, expression.unwrap());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDateLiteral node) {
        DateString str = node.getDateString();
        DBSPStrLiteral rustLiteral = new DBSPStrLiteral(str == null ? "" : str.toString().trim());
        DBSPExpression expression = new DBSPConstructorExpression(
                new DBSPPath("NaiveDate", "parse_from_str").toExpression(),
                DBSPTypeAny.getDefault(), rustLiteral, new DBSPStrLiteral("%F"));
        this.constant(node.getType().code, node, expression.unwrap());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDecimalLiteral node) {
        this.constant(node.getType().code, node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPUSizeLiteral node) {
         this.constant(node.getType().code, node);
         return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPISizeLiteral node) {
        this.constant(node.getType().code, node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal node) {
        this.constant(node.getType().code, node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal node) {
        this.constant(node.getType().code, node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral node) {
        this.constant(node.getType().code, node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral node) {
        this.constant(node.getType().code, node);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFloatLiteral node) {
        this.constant(node.getType().code, node.raw());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral node) {
        this.constant(node.getType().code, node.raw());
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression expression) {
        this.push(expression);
        DBSPExpression[] columns = Linq.map(expression.fields, this::transform, DBSPExpression.class);
        this.pop(expression);
        DBSPExpression result = new DBSPVecLiteral(columns);
        this.map(expression, result);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPExpression expression) {
        throw new UnimplementedException(expression);
    }

    DBSPExpression convertRow(Map.Entry<DBSPExpression, Long> row) {
        DBSPExpression key = this.transform(row.getKey());
        DBSPExpression r = new DBSPConstructorExpression(
                new DBSPPath("RowLiteral", "new").toExpression(),
                DBSPTypeAny.getDefault(), key);
        DBSPExpression value = new DBSPI32Literal(Math.toIntExact(row.getValue()));
        return new DBSPRawTupleExpression(r, value);
    }

    @Override
    public VisitDecision preorder(DBSPZSetLiteral expression) {
        this.push(expression);
        DBSPExpression[] rows = new DBSPExpression[expression.size()];
        int index = 0;
        for (Map.Entry<DBSPExpression, Long> entry : expression.data.data.entrySet()) {
            DBSPExpression row = this.convertRow(entry);
            rows[index++] = row;
        }
        this.pop(expression);

        DBSPVecLiteral vec;
        if (rows.length == 0) {
            vec = new DBSPVecLiteral(expression.getElementType());
        } else {
            vec = new DBSPVecLiteral(rows);
        }
        DBSPExpression result = new DBSPConstructorExpression(
                new DBSPPath("Set").toExpression(),
                DBSPTypeAny.getDefault(), vec);
        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
