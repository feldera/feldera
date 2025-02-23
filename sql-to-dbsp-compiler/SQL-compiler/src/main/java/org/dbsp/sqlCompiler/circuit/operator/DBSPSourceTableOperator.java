package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;

import javax.annotation.Nullable;

/** Base class for source operators which represent tables. */
public abstract class DBSPSourceTableOperator
        extends DBSPSourceBaseOperator
        implements IHasColumnsMetadata {
    /** Original output row type, as a struct (not a tuple), i.e., with named columns. */
    public final DBSPTypeStruct originalRowType;
    public final CalciteObject sourceName;
    // Note: the metadata is not transformed after being set.
    // In particular, types are not rewritten.
    public final TableMetadata metadata;

    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     *
     * @param node       Calcite node for the statement creating the table
     *                   that this node is created from.
     * @param sourceName Calcite node for the identifier naming the table.
     * @param outputType Type of table.
     * @param isMultiset True if the source can produce multiset values.
     * @param name       The name of the table that this operator is created from.
     * @param comment    A comment describing the operator.
     */
    protected DBSPSourceTableOperator(
            CalciteObject node, String operation, CalciteObject sourceName,
            DBSPType outputType, DBSPTypeStruct originalRowType, boolean isMultiset,
            TableMetadata metadata, ProgramIdentifier name, @Nullable String comment) {
        super(node, operation, outputType, isMultiset, name, comment);
        this.originalRowType = originalRowType;
        this.sourceName = sourceName;
        this.metadata = metadata;
    }

    @Override
    public SourcePositionRange getSourcePosition() {
        if (!this.sourceName.isEmpty() && this.sourceName.getPositionRange().isValid())
            return this.sourceName.getPositionRange();
        return this.getNode().getPositionRange();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        visitor.property("originalRowType");
        this.originalRowType.accept(visitor);
        super.accept(visitor);
    }

    @Override
    public Iterable<? extends IHasLateness> getLateness() {
        return this.metadata.getColumns();
    }

    @Override
    public Iterable<? extends IHasWatermark> getWatermarks() {
        return this.metadata.getColumns();
    }
}
