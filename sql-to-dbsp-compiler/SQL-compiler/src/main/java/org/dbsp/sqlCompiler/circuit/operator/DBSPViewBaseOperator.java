package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.ViewColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.List;

/** Base class for an operator representing a view declared by the user.
 *  If the view is an output then it is represented by a Sink operator.
 *  Otherwise, the view is represented by a DBSPViewOperator. */
public abstract class DBSPViewBaseOperator extends DBSPUnaryOperator {
    public final String viewName;
    public final String query;
    public final DBSPTypeStruct originalRowType;
    public final List<ViewColumnMetadata> metadata;

    protected DBSPViewBaseOperator(
            CalciteObject node, String operation, @Nullable DBSPExpression function,
            String viewName, String query, DBSPTypeStruct originalRowType,
            List<ViewColumnMetadata> metadata,
            @Nullable String comment, DBSPOperator input) {
        super(node, operation, function, input.outputType, input.isMultiset, input, comment);
        this.metadata = metadata;
        this.query = query;
        this.viewName = viewName;
        this.originalRowType = originalRowType;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return this.writeComments(builder, this.query)
                .append("let ")
                .append(this.getOutputName())
                .append(": ")
                .append(this.outputStreamType)
                .append(" = ")
                .append(this.input().getOutputName())
                .append(";");
    }

    @Override
    public String toString() {
        return this.getClass()
                .getSimpleName()
                .replace("DBSP", "")
                .replace("Operator", "")
                + " " + this.viewName
                + " " + this.getIdString();
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        // Two outputs are never equivalent
        return false;
    }
}
