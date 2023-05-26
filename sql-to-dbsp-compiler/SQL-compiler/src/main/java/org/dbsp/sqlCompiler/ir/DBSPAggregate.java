package org.dbsp.sqlCompiler.ir;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperator;
import org.dbsp.sqlCompiler.circuit.DBSPNode;
import org.dbsp.sqlCompiler.circuit.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Description of an aggregate.
 * In general an aggregate performs multiple simple aggregates simultaneously.
 * These are the components.
 */
public class DBSPAggregate extends DBSPNode implements IDBSPInnerNode {
    public final DBSPVariablePath rowVar;
    public final Implementation[] components;

    public DBSPAggregate(RelNode node, DBSPVariablePath rowVar, int size) {
        super(node);
        this.rowVar = rowVar;
        this.components = new Implementation[size];
    }

    public DBSPAggregate(@Nullable Object node, DBSPVariablePath rowVar, Implementation[] components) {
        super(node);
        this.rowVar = rowVar;
        this.components = components;
    }

    public void set(int i, Implementation implementation) {
        this.components[i] = implementation;
    }

    public DBSPTypeTuple defaultZeroType() {
        return this.defaultZero().getNonVoidType().to(DBSPTypeTuple.class);
    }

    public DBSPExpression defaultZero() {
        return new DBSPTupleExpression(Linq.map(this.components, c -> c.emptySetResult, DBSPExpression.class));
    }

    public DBSPExpression getZero() {
        return new DBSPTupleExpression(Linq.map(this.components, c -> c.zero, DBSPExpression.class));
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (Implementation impl: this.components) {
            impl.accept(visitor);
        }
        visitor.postorder(this);
    }

    public DBSPClosureExpression getIncrement() {
        // Here we rely on the fact that all increment functions have the following signature:
        // closure0 = (a0: AccumType0, v: Row, w: Weight) -> AccumType9 { body0 }
        // closure1 = (a1: AccumType1, v: Row, w: Weight) -> AccumType1 { body1 }
        // And all accumulators have distinct names.
        // We generate the following closure:
        // (a: (AccumumType0, AccumType1), v: Row, W: Weight) -> (AccumType0, AccumType1) {
        //    let tmp0 = closure0(a.0, v, row);
        //    let tmp1 = closure1(a.1, v, row);
        //    (tmp0, tmp1)
        // }
        if (this.components.length == 0)
            throw new RuntimeException("Empty aggregation components");
        DBSPClosureExpression[] closures = Linq.map(this.components, c -> c.increment, DBSPClosureExpression.class);
        for (DBSPClosureExpression expr: closures) {
            if (expr.parameters.length != 3)
                throw new RuntimeException("Expected exactly 3 parameters for increment closure" + expr);
        }
        DBSPType[] accumTypes = Linq.map(closures, c -> c.parameters[0].getNonVoidType(), DBSPType.class);
        DBSPVariablePath accumParam = new DBSPTypeTuple(accumTypes).var("a");
        DBSPParameter accumulator = accumParam.asParameter();
        DBSPParameter row = closures[0].parameters[1];
        DBSPParameter weight = closures[0].parameters[2];

        List<DBSPStatement> body = new ArrayList<>();
        List<DBSPExpression> tmps = new ArrayList<>();
        for (int i = 0; i < closures.length; i++) {
            DBSPClosureExpression closure = closures[i];
            String tmp = "tmp" + i;
            DBSPExpression init = closure.call(
                    accumParam.field(i), row.asVariableReference(), weight.asVariableReference());
            DBSPLetStatement stat = new DBSPLetStatement(tmp, init);
            tmps.add(new DBSPVariablePath(tmp, init.getNonVoidType()));
            body.add(stat);
        }

        DBSPExpression last = new DBSPTupleExpression(tmps, false);
        DBSPBlockExpression block = new DBSPBlockExpression(body, last);
        return new DBSPClosureExpression(block, accumulator, row, weight);
    }

    public DBSPClosureExpression getPostprocessing() {
        DBSPClosureExpression[] closures = Linq.map(
                this.components, Implementation::getPostprocessing, DBSPClosureExpression.class);
        DBSPParameter[][] allParams = Linq.map(closures, c -> c.parameters, DBSPParameter[].class);
        int paramCount = -1;
        for (DBSPParameter[] params: allParams) {
            if (paramCount == -1)
                paramCount = params.length;
            else if (paramCount != params.length)
                throw new RuntimeException("Closures cannot be combined");
        }

        DBSPVariablePath[] resultParams = new DBSPVariablePath[paramCount];
        for (int i = 0; i < paramCount; i++) {
            int finalI = i;
            DBSPParameter[] first = Linq.map(allParams, p -> p[finalI], DBSPParameter.class);
            String name = "p" + i;
            DBSPVariablePath pi = new DBSPVariablePath(name, new DBSPTypeTuple(Linq.map(first, p -> p.type, DBSPType.class)));
            resultParams[i] = pi;
        }

        List<DBSPStatement> body = new ArrayList<>();
        List<DBSPExpression> tmps = new ArrayList<>();
        for (int i = 0; i < closures.length; i++) {
            DBSPClosureExpression closure = closures[i];
            String tmp = "tmp" + i;
            int finalI = i;
            DBSPExpression[] args = Linq.map(resultParams, p -> p.field(finalI), DBSPExpression.class);
            DBSPExpression init = closure.call(args);
            DBSPLetStatement stat = new DBSPLetStatement(tmp, init);
            tmps.add(new DBSPVariablePath(tmp, init.getNonVoidType()));
            body.add(stat);
        }

        DBSPExpression last = new DBSPTupleExpression(tmps, false);
        DBSPBlockExpression block = new DBSPBlockExpression(body, last);
        DBSPParameter[] params = Linq.map(resultParams, DBSPVariablePath::asParameter, DBSPParameter.class);
        return new DBSPClosureExpression(block, params);
    }

    /**
     * An aggregate is compiled as functional fold operation,
     * described by a zero (initial value), an increment
     * function, and a postprocessing step that makes any necessary conversions.
     * For example, AVG has a zero of (0,0), an increment of (1, value),
     * and a postprocessing step of |a| a.1/a.0.
     * Notice that the DBSP `Fold` structure has a slightly different signature
     * for the increment.
     */
    public static class Implementation extends DBSPNode implements IDBSPInnerNode {
        @Nullable
        public final SqlOperator operator;
        /**
         * Zero of the fold function.
         */
        public final DBSPExpression zero;
        /**
         * A closure with signature |accumulator, value, weight| -> accumulator
         */
        public final DBSPClosureExpression increment;
        /**
         * Function that may post-process the accumulator to produce the final result.
         */
        @Nullable
        public final DBSPClosureExpression postProcess;
        /**
         * Result produced for an empty set (DBSP produces no result in this case).
         */
        public final DBSPExpression emptySetResult;
        /**
         * Name of the Type that implements the semigroup for this operation.
         */
        public final DBSPType semigroup;

        public Implementation(
                @Nullable SqlOperator operator,
                DBSPExpression zero,
                DBSPClosureExpression increment,
                @Nullable
                DBSPClosureExpression postProcess,
                DBSPExpression emptySetResult,
                DBSPType semigroup) {
            super(operator);
            this.operator = operator;
            this.zero = zero;
            this.increment = increment;
            this.postProcess = postProcess;
            this.emptySetResult = emptySetResult;
            this.semigroup = semigroup;
            this.validate();
        }

        public Implementation(
                @Nullable SqlOperator operator,
                DBSPExpression zero,
                DBSPClosureExpression increment,
                DBSPExpression emptySetResult,
                DBSPType semigroup) {
            this(operator, zero, increment, null, emptySetResult, semigroup);
        }

        void validate() {
            if (true)
                return;
            // These validation rules actually don't apply for window-based aggregates.
            // TODO: check them for standard aggregates.
            if (this.postProcess != null) {
                if (!this.emptySetResult.getNonVoidType().sameType(this.postProcess.getResultType()))
                    throw new RuntimeException("Post-process result type " + this.postProcess.getResultType() +
                            " different from empty set type " + this.emptySetResult.getNonVoidType());
            } else {
                if (!this.emptySetResult.getNonVoidType().sameType(this.increment.getResultType())) {
                    throw new RuntimeException("Increment result type " + this.increment.getResultType() +
                            " different from empty set type " + this.emptySetResult.getNonVoidType());
                }
            }
        }

        @Override
        public void accept(InnerVisitor visitor) {
            if (!visitor.preorder(this)) return;
            this.semigroup.accept(visitor);
            this.zero.accept(visitor);
            this.increment.accept(visitor);
            if (this.postProcess != null)
                this.postProcess.accept(visitor);
            this.emptySetResult.accept(visitor);
            visitor.postorder(this);
        }

        public DBSPClosureExpression getPostprocessing() {
            if (this.postProcess != null)
                return this.postProcess;
            DBSPVariablePath var = new DBSPVariablePath("x", Objects.requireNonNull(this.increment.getResultType()));
            return var.closure(var.asParameter());
        }
    }
}
