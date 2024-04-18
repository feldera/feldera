package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/*
 * Eliminate some function implementations.
 * For now just:
 * - dump(x)
 */
public class EliminateFunctions extends InnerRewriteVisitor {
    public EliminateFunctions(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression expression) {
        this.push(expression);
        CalciteObject node = expression.getNode();
        DBSPExpression[] arguments = this.transform(expression.arguments);
        DBSPExpression function = this.transform(expression.function);
        DBSPType type = this.transform(expression.getType());
        DBSPExpression result = new DBSPApplyExpression(function, type, arguments);
        DBSPPathExpression path = function.as(DBSPPathExpression.class);
        if (path != null) {
            String functionName = path.path.toString();
            if (functionName.startsWith("dump")) {
                // dump(prefix, tuple) -> { print(prefix);
                //                          print(": (")
                //                          writelog("%%,", tuple[0]), ...,
                //                          writelog("%%,", tuple[n]);
                //                          print(")\n");
                //                          tuple.clone()) }
                assert arguments.length == 2: "Expected 2 arguments for dump function";
                Function<DBSPExpression, DBSPExpressionStatement> makePrint = stringArgument ->
                        new DBSPApplyExpression(
                                expression.getNode(), "print", new DBSPTypeVoid(), stringArgument)
                                .toStatement();
                List<DBSPStatement> block = new ArrayList<>();
                block.add(makePrint.apply(arguments[0]));
                block.add(makePrint.apply(new DBSPStringLiteral(": (")));
                DBSPTypeTuple tuple = arguments[1].getType().to(DBSPTypeTuple.class);
                for (int i = 0; i < tuple.size(); i++) {
                    DBSPExpression fieldI = arguments[1].deepCopy().field(i);
                    DBSPExpression format = new DBSPStringLiteral("%%,");
                    DBSPExpression writeLog = new DBSPApplyExpression(
                            expression.getNode(), "writelog", fieldI.type, format, fieldI);
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
