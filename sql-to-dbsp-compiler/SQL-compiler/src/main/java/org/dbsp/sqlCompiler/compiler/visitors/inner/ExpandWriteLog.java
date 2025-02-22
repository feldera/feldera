package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
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
    public ExpandWriteLog(DBSPCompiler compiler) {
        super(compiler, false);
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
            String path = func.path.asString();
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
                            DBSPTypeString stringType = DBSPTypeString.varchar(type.mayBeNull);
                            castToStr = arguments[1].cast(stringType, false);
                            // do not print argument first time around the loop
                        } else {
                            String printFunction = type.mayBeNull ? "print_opt" : "print";
                            DBSPExpression print = new DBSPApplyExpression(
                                    expression.getNode(), printFunction, DBSPTypeVoid.INSTANCE, castToStr.applyCloneIfNeeded());
                            statements.add(print.toStatement());
                        }
                        if (!part.isEmpty()) {
                            DBSPExpression print = new DBSPApplyExpression(
                                    expression.getNode(), "print", DBSPTypeVoid.INSTANCE, new DBSPStringLiteral(part));
                            statements.add(print.toStatement());
                        }
                    }
                    result = new DBSPBlockExpression(statements, arguments[1].applyCloneIfNeeded());
                }
            }
        }

        this.map(expression, result);
        return VisitDecision.STOP;
    }
}