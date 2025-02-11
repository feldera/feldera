package org.dbsp.sqlCompiler.circuit.operator;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.IHasColumnsMetadata;
import org.dbsp.sqlCompiler.compiler.IHasLateness;
import org.dbsp.sqlCompiler.compiler.IHasWatermark;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;

import javax.annotation.Nullable;
import java.util.List;

/** Base class for source operators which represent tables. */
public abstract class DBSPSourceTableOperator
        extends DBSPSourceBaseOperator
        implements IHasColumnsMetadata {
    /** List of RelNodes that refer to this table.  This is only mutated while the first graph is constructed,
     * and immutable afterward. */
    public final List<RelNode> referredFrom;
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
            TableMetadata metadata, ProgramIdentifier name, @Nullable String comment,
            List<RelNode> referredFrom) {
        super(node, operation, outputType, isMultiset, name, comment);
        this.originalRowType = originalRowType;
        this.sourceName = sourceName;
        this.metadata = metadata;
        this.referredFrom = referredFrom;
    }

    /** Mark the fact that a RelNode refers to this table */
    public void refer(RelNode node) {
        this.referredFrom.add(node);
    }

    @Override
    public SourcePositionRange getSourcePosition() {
        if (!this.sourceName.isEmpty() && this.sourceName.getPositionRange().isValid())
            return this.sourceName.getPositionRange();
        return this.getNode().getPositionRange();
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
