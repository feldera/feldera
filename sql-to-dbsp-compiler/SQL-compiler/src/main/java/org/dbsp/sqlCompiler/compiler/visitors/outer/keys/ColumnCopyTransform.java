package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;

import java.util.List;

/** How an operator copies columns from its input rows to its output rows: for an input
 * column, the output columns holding a copy of its value. */
@FunctionalInterface
public interface ColumnCopyTransform {
    /** Every column an input column is copied to; empty when the operator keeps none. */
    List<Column> copiesOf(Column column);

    /** Keeps every column where it is. */
    static ColumnCopyTransform identity() {
        return List::of;
    }
}
