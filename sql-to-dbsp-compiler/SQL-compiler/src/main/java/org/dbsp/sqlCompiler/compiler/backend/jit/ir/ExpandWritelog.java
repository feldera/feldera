package org.dbsp.sqlCompiler.compiler.backend.jit.ir;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;

import java.util.ArrayList;
import java.util.List;

/**
 * This visitor replaces function calls to "writelog(format, argument)" with
 * more primitive operations understood by the JIT.  In particular, it generates
 * the following block expression:
 * {
 *     print((string)arg);
 *     argument
 * }
 * TODO: once JIT offers string_replace, change this code to:
 * {
 *    let arg_str = (string)argument;
 *    let formatted = format.replace("%%", arg_atr);
 *    print(formatted);
 *    argument
 * }
 */
public class ExpandWritelog extends InnerRewriteVisitor {
    public ExpandWritelog(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        this.push(expression);
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        DBSPType type = this.transform(expression.getType());
        this.pop(expression);

        DBSPExpression result = new DBSPApplyExpression(function, type, arguments);
        if (function.is(DBSPPathExpression.class)) {
            DBSPPathExpression func = function.to(DBSPPathExpression.class);
            String path = func.toString();
            if (path.equalsIgnoreCase("writelog")) {
                List<DBSPStatement> statements = new ArrayList<>();
                DBSPTypeString stringType = new DBSPTypeString(
                        CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, type.mayBeNull);
                DBSPCastExpression castToStr = new DBSPCastExpression(
                        expression.getNode(), arguments[1], stringType);
                DBSPExpression print = new DBSPApplyExpression(
                        expression.getNode(), "print", new DBSPTypeVoid(), castToStr);
                statements.add(new DBSPExpressionStatement(print));
                result = new DBSPBlockExpression(statements, arguments[1]);
            }
        }

        this.map(expression, result);
        return VisitDecision.STOP;
    }
}
