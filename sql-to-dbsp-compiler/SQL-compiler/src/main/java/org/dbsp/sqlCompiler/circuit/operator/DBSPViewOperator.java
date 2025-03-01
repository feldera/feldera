package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Represents an internal view, which does not produce output.
 * If the view does produce an output, this operator is followed by a Sink operator. */
public final class DBSPViewOperator
        extends DBSPViewBaseOperator
        implements IHasColumnsMetadata
{
    public DBSPViewOperator(
            CalciteRelNode node, ProgramIdentifier viewName, String query, DBSPType originalRowType,
            ViewMetadata metadata, OutputPort input) {
        super(node, "map", DBSPClosureExpression.id(), viewName, query,
                originalRowType, metadata, input);
        assert !originalRowType.is(DBSPTypeStruct.class) ||
                metadata.size() == originalRowType.to(DBSPTypeStruct.class).fields.size();
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
        return new DBSPViewOperator(this.getRelNode(), this.viewName, this.query, this.originalRowType,
                this.metadata, this.input()).copyAnnotations(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPViewOperator(
                    this.getRelNode(), this.viewName, this.query, this.originalRowType,
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

    @SuppressWarnings("unused")
    public static DBSPViewOperator fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier viewName = ProgramIdentifier.fromJson(Utilities.getProperty(node, "viewName"));
        String queryOrViewName = "";
        if (node.has("query"))
            queryOrViewName = Utilities.getStringProperty(node, "query");
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPTypeStruct originalRowType = fromJsonInner(node, "originalRowType", decoder, DBSPTypeStruct.class);
        ViewMetadata metadata = ViewMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        return new DBSPViewOperator(CalciteEmptyRel.INSTANCE, viewName, queryOrViewName,
                originalRowType, metadata, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPViewOperator.class);
    }
}
