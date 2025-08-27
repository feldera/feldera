package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelAnd;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Operator used in the creation of recursive circuits.
 * Represents a recursive view declaration that is used in the definition of a set of other views.
 * In fact, this behaves exactly like a delay operator that closes a cycle. */
@NonCoreIR
public final class DBSPViewDeclarationOperator
        extends DBSPSourceTableOperator {
    public final CalciteObject viewDeclaration;

    public DBSPViewDeclarationOperator(
            CalciteObject node, CalciteObject sourceName,
            DBSPTypeZSet outputType, DBSPTypeStruct originalRowType,
            TableMetadata metadata, ProgramIdentifier name) {
        super(new RelAnd(), "Z " + name.name(), sourceName, outputType, originalRowType, true,
                metadata, name, null);
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
            return new DBSPViewDeclarationOperator(this.viewDeclaration, this.sourceName,
                outputType.to(DBSPTypeZSet.class), this.originalRowType,
                this.metadata, this.tableName).copyAnnotations(this);
        return this;
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
                info.getZsetType(), originalRowType, metadata, viewName)
                .addAnnotations(info.annotations(), DBSPViewDeclarationOperator.class);
    }

    @Override
    public DBSPTypeUser getHandleType() {
        throw new InternalCompilerError("Should not be called");
    }
}
