package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

/** Represents an internal view, which does not produce output.
 * If the view does produce an output, this operator is followed by a Sink operator. */
public final class DBSPViewOperator
        extends DBSPViewBaseOperator
        implements IHasColumnsMetadata
{
    public DBSPViewOperator(
            CalciteObject node, ProgramIdentifier viewName, String query, DBSPType originalRowType,
            ViewMetadata metadata, OutputPort input) {
        super(node, "map", DBSPClosureExpression.id(), viewName, query,
                originalRowType, metadata, input);
        assert metadata.size() == originalRowType.to(DBSPTypeStruct.class).fields.size();
    }

    /** True if any column has LATENESS information */
    public boolean hasLateness() {
        return this.metadata.hasLateness();
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
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression ignoredFunction, DBSPType ignoredType) {
        return new DBSPViewOperator(this.getNode(), this.viewName, this.query, this.originalRowType,
                this.metadata, this.input()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPViewOperator(
                    this.getNode(), this.viewName, this.query, this.originalRowType,
                    this.metadata, newInputs.get(0)).copyAnnotations(this);
        return this;
    }

    @Override
    public Iterable<? extends IHasLateness> getLateness() {
        return this.metadata.columns;
    }

    @Override
    public Iterable<? extends IHasWatermark> getWatermarks() {
        // Currently no watermark information in views
        return Linq.list();
    }
}
