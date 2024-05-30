package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
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
            CalciteObject node,
            String viewName, String query, DBSPTypeStruct originalRowType,
            List<ViewColumnMetadata> metadata,
            @Nullable String comment, DBSPOperator input) {
        super(node, "map", DBSPClosureExpression.id(), viewName, query,
                originalRowType, metadata, comment, input);
        assert metadata.size() == originalRowType.fields.size();
    }

    /** True if any column has LATENESS information */
    public boolean hasLateness() {
        return Linq.any(this.metadata, m -> m.lateness != null);
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
    public DBSPOperator withFunction(@Nullable DBSPExpression ignoredFunction, DBSPType ignoredType) {
        return new DBSPViewOperator(this.getNode(), this.viewName, this.query, this.originalRowType,
                this.metadata, this.comment, this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPViewOperator(
                    this.getNode(), this.viewName, this.query, this.originalRowType,
                    this.metadata, this.comment, newInputs.get(0));
        return this;
    }

    @Override
    public Iterable<? extends IHasLateness> getLateness() {
        return this.metadata;
    }

    @Override
    public Iterable<? extends IHasWatermark> getWatermarks() {
        // Currently no watermark information in views
        return Linq.list();
    }
}
