package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/* Implement the dump() function. */
public class EliminateDump extends InnerRewriteVisitor {
    public EliminateDump(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        this.push(expression);
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        DBSPType type = this.transform(expression.getType());
        DBSPExpression result = new DBSPApplyExpression(function, type, arguments);
        DBSPPathExpression path = function.as(DBSPPathExpression.class);
        if (path != null) {
            String functionName = path.path.asString();
            if (functionName.startsWith("dump")) {
                // dump(prefix, tuple) -> { print(prefix);
                //                          print(": (")
                //                          writelog("%%,", tuple[0]), ...,
                //                          writelog("%%,", tuple[n]);
                //                          print(")\n");
                //                          tuple.clone()) }
                Utilities.enforce(arguments.length == 2, () -> "Expected 2 arguments for dump function");
                Function<DBSPExpression, DBSPExpressionStatement> makePrint = stringArgument ->
                        new DBSPApplyExpression(
                                expression.getNode(), "print", DBSPTypeVoid.INSTANCE, stringArgument)
                                .toStatement();
                List<DBSPStatement> block = new ArrayList<>();
                block.add(makePrint.apply(arguments[0]));
                block.add(makePrint.apply(new DBSPStringLiteral(": (")));
                DBSPTypeTuple tuple = arguments[1].getType().to(DBSPTypeTuple.class);
                for (int i = 0; i < tuple.size(); i++) {
                    DBSPExpression fieldI = arguments[1].field(i);
                    DBSPExpression format = new DBSPStringLiteral("%%,");
                    DBSPExpression writeLog = new DBSPApplyExpression(
                            expression.getNode(), CustomFunctions.WriteLogFunction.NAME.toLowerCase(Locale.ENGLISH),
                            fieldI.type, format, fieldI);
                    block.add(writeLog.toStatement());
                }
                block.add(makePrint.apply(new DBSPStringLiteral(")\n")));
                result = new DBSPBlockExpression(block, arguments[1].applyCloneIfNeeded());
            }
        }
        this.map(expression, result);
        this.pop(expression);
        return VisitDecision.STOP;
    }
}
