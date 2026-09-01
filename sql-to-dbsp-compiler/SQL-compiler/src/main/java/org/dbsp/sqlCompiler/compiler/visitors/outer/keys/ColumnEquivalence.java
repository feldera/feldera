package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** The columns of one collection grouped by value: two columns belong to the same
 * {@link EquivalentColumnSet} when every row carries the same value in both.  A column that
 * shares its value with no other forms a set of its own, and is not stored. */
final class ColumnEquivalence {
    /** Every column carries a value of its own. */
    static final ColumnEquivalence NONE = new ColumnEquivalence(Map.of());

    /** The set of each column that shares its value, stored under each of the set's columns. */
    private final Map<Column, EquivalentColumnSet> sets;

    private ColumnEquivalence(Map<Column, EquivalentColumnSet> sets) {
        this.sets = sets;
    }

    /** Group columns that carry equal values.  Groups sharing a column describe one value,
     * so they are merged; a group of one column says nothing and is dropped. */
    static ColumnEquivalence of(Collection<? extends Collection<Column>> groups) {
        Map<Column, TreeSet<Column>> merged = new HashMap<>();
        for (Collection<Column> group : groups) {
            if (group.size() < 2)
                continue;
            // Absorb the set of every column of the group: equality is transitive
            TreeSet<Column> combined = new TreeSet<>(group);
            for (Column column : group) {
                TreeSet<Column> known = merged.get(column);
                if (known != null)
                    combined.addAll(known);
            }
            // Every column of an absorbed set is in the combined one, so this re-points them all
            for (Column column : combined)
                merged.put(column, combined);
        }
        Map<Column, EquivalentColumnSet> sets = new HashMap<>();
        for (TreeSet<Column> group : new HashSet<>(merged.values())) {
            EquivalentColumnSet columns = EquivalentColumnSet.of(group);
            for (Column column : group)
                sets.put(column, columns);
        }
        return sets.isEmpty() ? NONE : new ColumnEquivalence(Map.copyOf(sets));
    }

    /** The columns carrying the same value as {@code column}, including itself. */
    EquivalentColumnSet setOf(Column column) {
        EquivalentColumnSet known = this.sets.get(column);
        return known == null ? EquivalentColumnSet.of(column) : known;
    }

    /** The key formed by these columns, each named by the set of columns carrying its value. */
    KeyColumns keyOf(Collection<Column> columns) {
        List<EquivalentColumnSet> result = new ArrayList<>();
        for (Column column : columns)
            result.add(this.setOf(column));
        return new KeyColumns(new TreeSet<>(result));
    }

    /** The groups this partition is built from: one per set, each listing its columns. */
    private List<List<Column>> groups() {
        List<List<Column>> result = new ArrayList<>();
        for (EquivalentColumnSet columns : new HashSet<>(this.sets.values())) {
            List<Column> group = new ArrayList<>();
            for (Column column : columns.columns())
                group.add(column);
            result.add(group);
        }
        return result;
    }

    /** This partition transformed by the specified `transform`.
     * A set the operator drops entirely disappears. */
    ColumnEquivalence after(ColumnCopyTransform transform) {
        List<List<Column>> result = new ArrayList<>();
        for (EquivalentColumnSet columns : new HashSet<>(this.sets.values())) {
            List<Column> group = new ArrayList<>();
            for (Column column : columns.columns())
                group.addAll(transform.copiesOf(column));
            result.add(group);
        }
        return of(result);
    }

    /** Only what both partitions state: two columns hold the same data here when they hold
     * the same data in both.  The rows of a sum come from either input, so a pair of columns
     * is equal throughout the sum only if it is equal throughout each input. */
    ColumnEquivalence intersect(ColumnEquivalence other) {
        if (this.isEmpty() || other.isEmpty())
            return NONE;
        List<List<Column>> common = new ArrayList<>();
        for (EquivalentColumnSet mine : new HashSet<>(this.sets.values())) {
            for (EquivalentColumnSet theirs : new HashSet<>(other.sets.values())) {
                List<Column> shared = new ArrayList<>(mine.columns());
                shared.retainAll(theirs.columns());
                common.add(shared);
            }
        }
        return of(common);
    }

    /** Merge two ColumnEquivalences. */
    ColumnEquivalence merge(ColumnEquivalence other) {
        if (this.isEmpty())
            return other;
        if (other.isEmpty())
            return this;
        List<List<Column>> both = this.groups();
        both.addAll(other.groups());
        return of(both);
    }

    boolean isEmpty() {
        return this.sets.isEmpty();
    }

    @Override
    public String toString() {
        List<EquivalentColumnSet> distinct = new ArrayList<>(new HashSet<>(this.sets.values()));
        Collections.sort(distinct);
        return distinct.toString();
    }
}
