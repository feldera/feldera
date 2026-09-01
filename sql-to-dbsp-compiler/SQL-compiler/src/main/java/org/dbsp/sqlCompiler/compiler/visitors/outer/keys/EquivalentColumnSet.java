package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Part;
import org.dbsp.util.Utilities;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/** One value of a row, and every column naming it: columns of a collection that hold the same
 * data in every row.  A value tuple repeats the columns of its index, and a function may copy
 * one column to several outputs, so a value can usually be named more than one way. */
public record EquivalentColumnSet(SortedSet<Column> columns)
        implements Comparable<EquivalentColumnSet> {
    public EquivalentColumnSet {
        // The copy is owned here, so wrapping it leaves nobody able to change the set
        columns = Collections.unmodifiableSortedSet(new TreeSet<>(columns));
        Utilities.enforce(!columns.isEmpty(), () -> "Equivalent column set without columns");
    }

    public static EquivalentColumnSet of(Collection<Column> columns) {
        return new EquivalentColumnSet(new TreeSet<>(columns));
    }

    public static EquivalentColumnSet of(Column... columns) {
        return of(Arrays.asList(columns));
    }

    /** True if the value can be named by a column of the index of an indexed Z-set */
    public boolean withinIndex() {
        for (Column column : this.columns)
            if (column.part() == Part.INDEX)
                return true;
        return false;
    }

    public boolean contains(Column column) {
        return this.columns.contains(column);
    }

    /** Column by column, then by size.  A key holds its sets in this order, so that two
     * keys naming the same values compare and print alike. */
    @Override
    public int compareTo(EquivalentColumnSet other) {
        Iterator<Column> mine = this.columns.iterator();
        Iterator<Column> theirs = other.columns.iterator();
        while (mine.hasNext() && theirs.hasNext()) {
            int result = mine.next().compareTo(theirs.next());
            if (result != 0)
                return result;
        }
        return Integer.compare(this.columns.size(), other.columns.size());
    }

    /** A single column prints as itself, a larger set as its columns joined by '=' */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Column column : this.columns) {
            if (!builder.isEmpty())
                builder.append("=");
            builder.append(column);
        }
        return builder.toString();
    }
}
