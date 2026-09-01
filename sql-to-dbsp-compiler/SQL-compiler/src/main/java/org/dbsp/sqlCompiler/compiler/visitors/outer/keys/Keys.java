package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.util.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/** The keys of one collection, minimized: no key contains another. */
public final class Keys {
    /** Maximum number of keys kept for one collection.  A join has a key for each pair of
     * keys of its inputs, so a chain of joins can accumulate more than are worth keeping. */
    private static final int MAX_KEYS = 64;

    public static final Keys NONE = new Keys(List.of());
    public final List<KeyColumns> keys;

    private Keys(List<KeyColumns> keys) {
        this.keys = keys;
    }

    /** The keys given, minimized as by {@link #of(Collection)}. */
    public static Keys of(KeyColumns... keys) {
        return of(Arrays.asList(keys));
    }

    /** Deduplicate, drop keys that contain another key, and keep at most {@link #MAX_KEYS},
     * the smallest first: a key identifying a row with fewer values is the more useful one. */
    public static Keys of(Collection<KeyColumns> raw) {
        List<KeyColumns> sorted = new ArrayList<>(new HashSet<>(raw));
        // Insert in order of size; the rest of the order only makes it deterministic,
        // since the set above hands out its keys in no fixed order
        Collections.sort(sorted);
        // Truncate before pruning, so that the loop below is quadratic in MAX_KEYS, not in the input size
        if (sorted.size() > MAX_KEYS) {
            Logger.INSTANCE.belowLevel(KeyAnalysis.class, 1)
                    .append("Keeping ").append(MAX_KEYS).append(" of ").append(sorted.size()).append(" keys")
                    .newline();
            sorted = sorted.subList(0, MAX_KEYS);
        }
        // A key can only contain a smaller key, which precedes it; comparing against the
        // kept keys is enough
        List<KeyColumns> result = new ArrayList<>();
        for (KeyColumns key : sorted) {
            boolean minimal = true;
            for (KeyColumns kept : result)
                if (key.strictlyContains(kept)) {
                    minimal = false;
                    break;
                }
            if (minimal)
                result.add(key);
        }
        return result.isEmpty() ? NONE : new Keys(List.copyOf(result));
    }

    public boolean isEmpty() {
        return this.keys.isEmpty();
    }

    /** True if some key uses only columns of the index: each index value identifies a row. */
    public boolean hasKeyWithinIndex() {
        for (KeyColumns key : this.keys)
            if (key.withinIndex())
                return true;
        return false;
    }

    /** One key from each of {@code all}, taken together: a key of a collection whose rows
     * each combine one row from every one of them, as a join output does. */
    public static Keys combinations(List<Keys> all) {
        List<KeyColumns> result = new ArrayList<>();
        // The empty key is the identity of union: it contributes no value of its own
        result.add(KeyColumns.of());
        for (Keys keys : all) {
            List<KeyColumns> extended = new ArrayList<>();
            for (KeyColumns prefix : result)
                for (KeyColumns key : keys.keys)
                    extended.add(prefix.union(key));
            // Minimizing at each step keeps the smallest combinations and bounds the list.
            // A combination containing another would still contain it once the remaining
            // sets are added, so dropping it here drops nothing the result would have kept.
            result = Keys.of(extended).keys;
        }
        return Keys.of(result);
    }

    /** These keys and one more. */
    public Keys plus(KeyColumns key) {
        List<KeyColumns> result = new ArrayList<>(this.keys);
        result.add(key);
        return Keys.of(result);
    }

    /** Drop the specified columns from every key; an emptied key means at most one row. */
    public Keys without(Collection<Column> columns) {
        List<KeyColumns> result = new ArrayList<>();
        for (KeyColumns key : this.keys)
            result.add(key.without(columns));
        return Keys.of(result);
    }

    /** Every key in the output rows of an operator; see {@link KeyColumns#map}. */
    public Keys map(ColumnCopyTransform transform) {
        List<KeyColumns> result = new ArrayList<>();
        for (KeyColumns key : this.keys) {
            KeyColumns mapped = key.map(transform);
            if (mapped != null)
                result.add(mapped);
        }
        return Keys.of(result);
    }

    @Override
    public String toString() {
        return this.keys.toString();
    }
}
