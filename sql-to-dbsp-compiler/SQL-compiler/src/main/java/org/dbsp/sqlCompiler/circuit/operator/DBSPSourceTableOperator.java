package org.dbsp.sqlCompiler.circuit.operator;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.IColumnMetadata;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.LastRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.RelAnd;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Base class for source operators which represent tables. */
public abstract class DBSPSourceTableOperator
        extends DBSPSourceBaseOperator
        implements IHasColumnsMetadata {
    /** Original output row type, as a struct (not a tuple), i.e., with named columns. */
    public final DBSPTypeStruct originalRowType;
    public final CalciteObject sourceName;

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
            CalciteRelNode node, String operation, CalciteObject sourceName,
            DBSPType outputType, DBSPTypeStruct originalRowType, boolean isMultiset,
            TableMetadata metadata, ProgramIdentifier name, @Nullable String comment) {
        super(node, operation, outputType, isMultiset, name, metadata, comment);
        Utilities.enforce(node.is(RelAnd.class) || node.is(CalciteEmptyRel.class));
        this.originalRowType = originalRowType;
        this.sourceName = sourceName;
    }

    /** Mark the fact that a RelNode refers to this table */
    public void refer(RelNode node) {
        // Only mutating operation, used during circuit construction.
        this.getRelNode().to(RelAnd.class).add(new LastRel(node, this.getSourcePosition()));
    }

    @Override
    public DBSPTypeStruct getOriginalRowType() {
        return this.originalRowType;
    }

    @Override
    public Iterable<? extends IColumnMetadata> getColumnsMetadata() {
        return this.metadata.getColumns();
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
}
