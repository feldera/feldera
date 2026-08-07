package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBinaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPLetExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPUnaryExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;

import java.util.List;
import java.util.function.Function;

/** Builds small inner-IR expressions for unit tests.
 * Binding constructs (let, block, lambda, closure) take a Java function
 * that receives the bound variable, so the binder and its uses share the
 * correct node identity by construction. */
// Example:
//   DBSPClosureExpression f = b.closure(b.tup(b.i32(), b.i32()), t ->
//           b.let(b.call("f", b.field(t, 0)),
//                 x -> b.call("g", x)));
// builds the closure that compiles to the Rust code
//   move |t_0: &Tup2<i32, i32>| -> i32 {
//       let t_1 = f((*t_0).0);
//       g(t_1)
//   }
public class ExpressionBuilder {
    public DBSPType i32() {
        return new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false);
    }

    /** Nullable INT */
    public DBSPType i32n() {
        return new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, true);
    }

    /** VARCHAR */
    public DBSPType str() {
        return DBSPTypeString.varchar(false);
    }

    /** Nullable VARCHAR */
    public DBSPType strn() {
        return DBSPTypeString.varchar(true);
    }

    public DBSPType bool() {
        return new DBSPTypeBool(CalciteObject.EMPTY, false);
    }

    /** Nullable BOOLEAN */
    public DBSPType booln() {
        return new DBSPTypeBool(CalciteObject.EMPTY, true);
    }

    public DBSPTypeTuple tup(DBSPType... fields) {
        return new DBSPTypeTuple(fields);
    }

    public DBSPExpression lit(int value) {
        return new DBSPI32Literal(value);
    }

    /** An INT literal, nullable if requested */
    public DBSPExpression lit(int value, boolean nullable) {
        return new DBSPI32Literal(value, nullable);
    }

    public DBSPExpression lit(String value) {
        return new DBSPStringLiteral(value);
    }

    /** A variable of the reference type; becomes a parameter when a closure is built over it */
    public DBSPVariablePath refVar(DBSPType type) {
        return type.ref().var();
    }

    /** Field of a row parameter */
    public DBSPExpression field(DBSPVariablePath row, int index) {
        return row.deref().field(index);
    }

    /** A call to a UDF returning INT; any external call is expensive */
    public DBSPExpression call(String function, DBSPExpression... args) {
        return this.call(this.i32(), function, args);
    }

    public DBSPExpression call(DBSPType returnType, String function, DBSPExpression... args) {
        return new DBSPApplyExpression(function, returnType, args);
    }

    public DBSPExpression add(DBSPExpression left, DBSPExpression right) {
        return this.binary(DBSPOpcode.ADD, left, right);
    }

    public DBSPExpression binary(DBSPOpcode opcode, DBSPExpression left, DBSPExpression right) {
        // Comparing a nullable value produces a nullable Boolean
        DBSPType type = opcode.isComparison()
                ? new DBSPTypeBool(CalciteObject.EMPTY,
                        left.getType().mayBeNull || right.getType().mayBeNull)
                : left.getType();
        return this.binary(type, opcode, left, right);
    }

    /** A binary expression with an explicit result type */
    public DBSPExpression binary(DBSPType type, DBSPOpcode opcode,
                                 DBSPExpression left, DBSPExpression right) {
        return new DBSPBinaryExpression(CalciteObject.EMPTY, type, opcode, left, right);
    }

    public DBSPExpression unary(DBSPOpcode opcode, DBSPExpression source) {
        return new DBSPUnaryExpression(CalciteObject.EMPTY, source.getType(), opcode, source);
    }

    public DBSPExpression neg(DBSPExpression source) {
        return this.unary(DBSPOpcode.NEG, source);
    }

    public DBSPExpression ifThenElse(DBSPExpression condition,
                                     DBSPExpression positive, DBSPExpression negative) {
        return new DBSPIfExpression(CalciteObject.EMPTY, condition, positive, negative);
    }

    public DBSPExpression tuple(DBSPExpression... fields) {
        return new DBSPTupleExpression(fields);
    }

    /** let var = initializer; consumer(var) */
    public DBSPExpression let(DBSPExpression initializer,
                              Function<DBSPVariablePath, DBSPExpression> consumer) {
        DBSPVariablePath var = initializer.getType().var();
        return new DBSPLetExpression(var, initializer, consumer.apply(var));
    }

    /** { let var = initializer; last(var) } */
    public DBSPExpression block(DBSPExpression initializer,
                                Function<DBSPVariablePath, DBSPExpression> last) {
        DBSPVariablePath var = initializer.getType().var();
        DBSPStatement statement = new DBSPLetStatement(var.variable, initializer);
        return new DBSPBlockExpression(List.of(statement), last.apply(var));
    }

    /** |var| body(var), a nested lambda over a value */
    public DBSPClosureExpression lambda(DBSPType paramType,
                                        Function<DBSPVariablePath, DBSPExpression> body) {
        DBSPVariablePath var = paramType.var();
        return body.apply(var).closure(var);
    }

    /** A closure over a parameter passed by reference,
     * the shape of a map function when the type is a row type */
    public DBSPClosureExpression closure(DBSPType paramType,
                                         Function<DBSPVariablePath, DBSPExpression> body) {
        DBSPVariablePath var = this.refVar(paramType);
        return body.apply(var).closure(var);
    }
}
