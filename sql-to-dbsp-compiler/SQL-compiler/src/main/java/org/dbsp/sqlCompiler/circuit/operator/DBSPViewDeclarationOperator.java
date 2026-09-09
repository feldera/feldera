package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelAnd;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;

/** Operator used in the creation of recursive circuits.
 * Represents a recursive view declaration that is used in the definition of a set of other views.
 * In fact, this behaves exactly like a delay operator that closes a cycle. */
public final class DBSPViewDeclarationOperator
        extends DBSPSourceTableOperator {
    public final CalciteObject viewDeclaration;

    /** @param kind  A declaration stands for the previous value of a recursive view:
     *              a collection in the flat circuit built by the front-end, and a delta
     *              once the declaration is placed inside a nested circuit. */
    public DBSPViewDeclarationOperator(
            CalciteObject node, CalciteObject sourceName,
            DBSPTypeZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, ProgramIdentifier name, StreamKind kind) {
        this(new RelAnd(), node, sourceName, outputType, originalRowType, metadata, name, kind);
    }

    /** Copies keep the Calcite nodes accumulated by the original declaration. */
    private DBSPViewDeclarationOperator(
            CalciteRelNode relNode, CalciteObject node, CalciteObject sourceName,
            DBSPTypeZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, ProgramIdentifier name, StreamKind kind) {
        super(relNode, "Z", sourceName, outputType, originalRowType, true,
                metadata, name, kind, null);
        Utilities.enforce(metadata.getColumnCount() == originalRowType.fields.size());
        Utilities.enforce(metadata.getColumnCount() == outputType.elementType.to(DBSPTypeTuple.class).size());
        this.viewDeclaration = node;
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
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression unused, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, unused, newInputs, outputType))
            return new DBSPViewDeclarationOperator(this.getRelNode(), this.viewDeclaration, this.sourceName,
                outputType.to(DBSPTypeZSet.class), this.originalRowType,
                this.metadata, this.tableName, this.kind).copyAnnotations(this);
        return this;
    }

    @Override
    public DBSPSourceBaseOperator withKind(StreamKind kind) {
        return new DBSPViewDeclarationOperator(this.getRelNode(), this.viewDeclaration, this.sourceName,
                this.getOutputZSetType(), this.originalRowType, this.metadata, this.tableName, kind)
                .copyAnnotations(this).to(DBSPSourceBaseOperator.class);
    }

    public ProgramIdentifier originalViewName() {
        return new ProgramIdentifier(
                this.tableName.name().replace("-decl", ""),
                this.tableName.isQuoted());
    }

    /** Get the corresponding view operator for this view declaration */
    @Nullable
    public DBSPViewOperator getCorrespondingView(ICircuit circuit) {
        return circuit.getView(this.originalViewName());
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

    @SuppressWarnings("unused")
    public static DBSPViewDeclarationOperator fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier viewName = ProgramIdentifier.fromJson(Utilities.getProperty(node, "tableName"));
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPTypeStruct originalRowType = fromJsonInner(node, "originalRowType", decoder, DBSPTypeStruct.class);
        TableMetadata metadata = TableMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        return new DBSPViewDeclarationOperator(CalciteObject.EMPTY, CalciteObject.EMPTY,
                info.getZsetType(), originalRowType, metadata, viewName, decoder.sourceKind())
                .addAnnotations(info.annotations(), DBSPViewDeclarationOperator.class);
    }

    @Override
    public DBSPTypeUser getHandleType() {
        throw new InternalCompilerError("Should not be called");
    }

    @Override
    public DBSPSourceTableOperator withMetadata(TableMetadata metadata) {
        throw new UnimplementedException();
    }
}
