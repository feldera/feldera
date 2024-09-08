package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This visitor replaces function calls to "writelog(format, argument)" with
 * more primitive operations.  The format has the form
 * prefix%%suffix (with zero or more occurrences of %%).
 * E.g., it may generate the following block expression:
 * {
 *    print(prefix);
 *    let arg_str = (string)argument;
 *    print(arg_str);
 *    print(suffix);
 *    argument
 * }
 */
public class ExpandWriteLog extends InnerRewriteVisitor {
    public ExpandWriteLog(IErrorReporter reporter) {
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
                DBSPExpression format = arguments[0];
                if (!format.is(DBSPStringLiteral.class))
                    throw new UnsupportedException("Expected a string literal for the format", format.getNode());
                DBSPStringLiteral formatString = format.to(DBSPStringLiteral.class);
                if (formatString.isNull()) {
                    result = DBSPLiteral.none(expression.type);
                } else {
                    // Split the pattern by preserving the empty sequences
                    String[] parts = Objects.requireNonNull(formatString.value).split("%%", -1);
                    DBSPExpression castToStr = null;
                    for (String part: parts) {
                        if (castToStr == null) {
                            DBSPTypeString stringType = new DBSPTypeString(
                                    CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, type.mayBeNull);
                            castToStr = new DBSPCastExpression(
                                    expression.getNode(), arguments[1], stringType);
                            // do not print argument first time around the loop
                        } else {
                            String printFunction = type.mayBeNull ? "print_opt" : "print";
                            DBSPExpression print = new DBSPApplyExpression(
                                    expression.getNode(), printFunction, new DBSPTypeVoid(), castToStr.deepCopy().applyCloneIfNeeded());
                            statements.add(print.toStatement());
                        }
                        if (!part.isEmpty()) {
                            DBSPExpression print = new DBSPApplyExpression(
                                    expression.getNode(), "print", new DBSPTypeVoid(), new DBSPStringLiteral(part));
                            statements.add(print.toStatement());
                        }
                    }
                    result = new DBSPBlockExpression(statements, arguments[1].deepCopy().applyCloneIfNeeded());
                }
            }
        }

        this.map(expression, result);
        return VisitDecision.STOP;
    }
}