package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.IsNumericType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.util.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/*
 * Eliminate some function implementations.
 * For now just:
 * - sign_*
 * - power(a, .5)
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
            if (functionName.startsWith("sign_")) {
                assert arguments.length == 1: "Expected one argument for sign function";
                // sign(x) =>
                // { let tmp = x;
                //   if tmp < 0 { -1 } else { if tmp > 0 { 1 } else { 0 }
                // }
                DBSPExpression argument = arguments[0];
                DBSPType argType = argument.getType();
                IsNumericType resultNumericType = type.to(IsNumericType.class);
                DBSPExpression zero = argType.to(IsNumericType.class).getZero();
                List<DBSPStatement> statements = new ArrayList<>();
                String tmpName = "tmp";
                statements.add(new DBSPLetStatement(tmpName, argument));
                DBSPVariablePath var = new DBSPVariablePath(tmpName, argument.getType());
                DBSPExpression compare = new DBSPBinaryExpression(node, new DBSPTypeBool(node, argType.mayBeNull),
                        DBSPOpcode.GT, var, zero);
                DBSPExpression rightResult = new DBSPIfExpression(
                        node, compare, resultNumericType.getOne(), resultNumericType.getZero());
                DBSPExpression minusOne = new DBSPUnaryExpression(
                        node, type, DBSPOpcode.NEG, resultNumericType.getOne());
                DBSPExpression leftCompare = new DBSPBinaryExpression(node, compare.getType(),
                        DBSPOpcode.LT, var, zero.deepCopy());
                DBSPExpression mux = new DBSPIfExpression(node, leftCompare, minusOne, rightResult);
                result = new DBSPBlockExpression(statements, mux);
            } else if (functionName.startsWith("power_")) {
                // power_base_exp(a, .5) -> sqrt_base(a).
                String tail = functionName.split("_")[1];
                assert arguments.length == 2: "Expected two arguments for power function";
                DBSPExpression argument = arguments[1];
                if (argument.is(DBSPDecimalLiteral.class)) {
                    DBSPDecimalLiteral dec = argument.to(DBSPDecimalLiteral.class);
                    BigDecimal pointFive = new BigDecimal(5).movePointLeft(1);
                    if (!dec.isNull && Objects.requireNonNull(dec.value).equals(pointFive)) {
                        String newName = "sqrt_" + tail;
                        result = new DBSPApplyExpression(node, newName, type, arguments[0]);
                    }
                }
            }
        }
        this.map(expression, result);
        this.pop(expression);
        return VisitDecision.STOP;
    }
}
