package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.List;

/** Describes the "shape" of the tuples of a collection. */
public sealed interface CollectionShape permits ZSetShape, IndexedShape {
    /** A variable standing for one row of a collection of this shape and {@code type}. */
    DBSPVariablePath rowVariable(DBSPType type);

    /** The value {@code column} holds in the row that {@code row} stands for. */
    DBSPExpression field(DBSPVariablePath row, Column column);

    /** The tuple a column belongs to.  The declaration order is the order {@link Column#compareTo} uses. */
    enum Part {
        /** The element of a Z-set */
        NONE,
        /** The index of an indexed Z-set: its "key" in DBSP terms, a grouping column that
         * need not identify rows */
        INDEX,
        /** The value of an indexed Z-set */
        VALUE
    }

    /** A column of the rows a collection carries: a field of one of the row's tuples. */
    record Column(Part part, int field) implements Comparable<Column> {
        public Column {
            Utilities.enforce(field >= 0, () -> "Negative field " + field);
        }

        public static Column of(Part part, int field) {
            return new Column(part, field);
        }

        /** A column of a Z-set's element */
        public static Column none(int field) {
            return new Column(Part.NONE, field);
        }

        /** A column of an indexed Z-set's index */
        public static Column index(int field) {
            return new Column(Part.INDEX, field);
        }

        /** A column of an indexed Z-set's value */
        public static Column value(int field) {
            return new Column(Part.VALUE, field);
        }

        /** Orders columns by part, in the declaration order of {@link Part} (NONE, INDEX, VALUE),
         * then by field: index columns before value columns, and the canonical order of a set of columns. */
        @Override
        public int compareTo(Column other) {
            int result = this.part.compareTo(other.part);
            return result != 0 ? result : Integer.compare(this.field, other.field);
        }

        @Override
        public String toString() {
            return switch (this.part) {
                case NONE -> Integer.toString(this.field);
                case INDEX -> "i" + this.field;
                case VALUE -> "v" + this.field;
            };
        }
    }

    /** True if rows of this shape have the column */
    boolean contains(Column column);

    /** Every column of a row, in {@link Column#compareTo} order */
    List<Column> columns();

    /** The column at a position of the flattened row, index fields first */
    Column output(int position);

    /** Number of columns of the flattened row */
    int width();
}
