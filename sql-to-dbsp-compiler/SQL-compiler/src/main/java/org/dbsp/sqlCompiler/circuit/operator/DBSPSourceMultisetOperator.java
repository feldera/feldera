package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.InputTableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;

public class DBSPSourceMultisetOperator extends DBSPSourceTableOperator {
    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * The table has *no* primary key, so the data can form a multiset.
     * @param node        Calcite node for the statement creating the table
     *                    that this node is created from.
     * @param sourceName  Calcite node for the identifier naming the table.
     * @param outputType  Type of table.
     * @param comment     A comment describing the operator.
     * @param name        The name of the table that this operator is created from.
     */
    public DBSPSourceMultisetOperator(
            CalciteObject node, CalciteObject sourceName,
            DBSPTypeZSet outputType, DBSPTypeStruct originalRowType, @Nullable String comment,
            InputTableMetadata metadata, String name) {
        super(node, sourceName, outputType, originalRowType, true, comment, metadata, name);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression unused, DBSPType outputType) {
        return new DBSPSourceMultisetOperator(this.getNode(), this.sourceName,
                outputType.to(DBSPTypeZSet.class), this.originalRowType,
                this.comment, this.metadata, this.tableName);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPSourceMultisetOperator(
                    this.getNode(), this.sourceName, this.getOutputZSetType(), this.originalRowType,
                    this.comment, this.metadata, this.tableName);
        return this;
    }
}
