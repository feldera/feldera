package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.InputTableMetadata;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;

import javax.annotation.Nullable;

/** Base class for source operators which represent tables. */
public abstract class DBSPSourceTableOperator extends DBSPSourceBaseOperator {
    /** Original output row type, as a struct (not a tuple), i.e., with named columns. */
    public final DBSPTypeStruct originalRowType;
    public final CalciteObject sourceName;
    // Note: the metadata is not transformed after being set.
    // In particular, types are not rewritten.
    public final InputTableMetadata metadata;

    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * @param node        Calcite node for the statement creating the table
     *                    that this node is created from.
     * @param sourceName  Calcite node for the identifier naming the table.
     * @param outputType  Type of table.
     * @param isMultiset  True if the source can produce multiset values.
     * @param comment     A comment describing the operator.
     * @param name        The name of the table that this operator is created from.
     */
    public DBSPSourceTableOperator(
            CalciteObject node, CalciteObject sourceName,
            DBSPType outputType, DBSPTypeStruct originalRowType, boolean isMultiset,
            @Nullable String comment, InputTableMetadata metadata, String name) {
        super(node, outputType, isMultiset, comment, name);
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
}
