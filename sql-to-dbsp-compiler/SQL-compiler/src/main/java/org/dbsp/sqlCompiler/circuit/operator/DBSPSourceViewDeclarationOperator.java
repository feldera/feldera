package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.DBSPPartialCircuit;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Operator temporarily used in the creation of recursive circuits.
 * Represents a view that is used in the defnition of a set of other views. */
@NonCoreIR
public final class DBSPSourceViewDeclarationOperator
        extends DBSPSourceTableOperator {
    public DBSPSourceViewDeclarationOperator(
            CalciteObject node, CalciteObject sourceName,
            DBSPTypeZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, String name) {
        super(node, "recursive", sourceName, outputType, originalRowType, true, metadata, name, null);
        assert metadata.getColumnCount() == originalRowType.fields.size();
        assert metadata.getColumnCount() == outputType.elementType.to(DBSPTypeTuple.class).size();
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
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression unused, DBSPType outputType) {
        return new DBSPSourceViewDeclarationOperator(this.getNode(), this.sourceName,
                outputType.to(DBSPTypeZSet.class), this.originalRowType,
                this.metadata, this.tableName);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OperatorPort> newInputs, boolean force) {
        assert newInputs.isEmpty();
        if (force)
            return new DBSPSourceViewDeclarationOperator(
                    this.getNode(), this.sourceName, this.getOutputZSetType(), this.originalRowType,
                    this.metadata, this.tableName);
        return this;
    }

    public String originalViewName() {
        return this.tableName.replace("-port", "");
    }

    /** Get the corresponding view operator for this view declaration */
    public DBSPViewOperator getCorrespondingView(ICircuit circuit) {
        DBSPViewOperator view = circuit.getView(this.originalViewName());
        return Objects.requireNonNull(view);
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.tableName
                + " " + this.getIdString();
    }
}
