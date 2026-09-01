package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/** A key of a collection: values whose combination identifies a row, each named by the set
 * of columns holding it.  A key with no values means the collection has at most one row. */
public record KeyColumns(SortedSet<EquivalentColumnSet> sets) implements Comparable<KeyColumns> {
    public KeyColumns {
        // The copy is owned here, so wrapping it leaves nobody able to change the set
        sets = Collections.unmodifiableSortedSet(new TreeSet<>(sets));
    }

    /** The key of a collection holding at most one row: no value is needed to identify it. */
    public static final KeyColumns SINGLE_ROW = KeyColumns.of();

    public static KeyColumns of(EquivalentColumnSet... sets) {
        return new KeyColumns(new TreeSet<>(Arrays.asList(sets)));
    }

    public int size() {
        return this.sets.size();
    }

    /** True if every value of the key can be named by a column of the index of an indexed Z-set */
    public boolean withinIndex() {
        for (EquivalentColumnSet set : this.sets)
            if (!set.withinIndex())
                return false;
        return true;
    }

    /** True if this key includes every value of {@code other}, and at least one more */
    public boolean strictlyContains(KeyColumns other) {
        return this.sets.containsAll(other.sets) && this.size() > other.size();
    }

    /** A key holding the values of both keys.  E.g., a row of a join output is identified
     * by a key of the left input together with a key of the right input. */
    public KeyColumns union(KeyColumns other) {
        TreeSet<EquivalentColumnSet> both = new TreeSet<>(this.sets);
        both.addAll(other.sets);
        return new KeyColumns(both);
    }

    /** This key without the values that any of {@code columns} names.  E.g., a filter that
     * equates a column with a constant leaves that value the same on every row, so the key
     * no longer needs it. */
    public KeyColumns without(Collection<Column> columns) {
        List<EquivalentColumnSet> result = new ArrayList<>();
        for (EquivalentColumnSet set : this.sets) {
            boolean fixed = false;
            for (Column column : columns) {
                if (set.contains(column)) {
                    fixed = true;
                    break;
                }
            }
            if (!fixed)
                result.add(set);
        }
        return new KeyColumns(new TreeSet<>(result));
    }

    /** This key in the output rows of an operator: each value is named by the columns that
     * {@code transform} maps its columns to.
     * @return null if some value reaches no output column, since the values that remain
     *         need not identify a row */
    @Nullable
    public KeyColumns map(ColumnCopyTransform transform) {
        List<EquivalentColumnSet> result = new ArrayList<>();
        for (EquivalentColumnSet set : this.sets) {
            List<Column> reached = new ArrayList<>();
            for (Column column : set.columns())
                reached.addAll(transform.copiesOf(column));
            if (reached.isEmpty())
                return null;
            result.add(EquivalentColumnSet.of(reached));
        }
        return new KeyColumns(new TreeSet<>(result));
    }

    /** Smaller keys first, then by their sets.  {@link Keys#of} keeps keys in this order,
     * so that a key identifying a row with fewer values outlives a longer one. */
    @Override
    public int compareTo(KeyColumns other) {
        if (this.size() != other.size())
            return Integer.compare(this.size(), other.size());
        Iterator<EquivalentColumnSet> mine = this.sets.iterator();
        Iterator<EquivalentColumnSet> theirs = other.sets.iterator();
        while (mine.hasNext()) {
            int result = mine.next().compareTo(theirs.next());
            if (result != 0)
                return result;
        }
        return 0;
    }

    @Override
    public String toString() {
        return this.sets.toString();
    }
}
