package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.CollectionShape;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** How the output a function relates to its parameters: for each output column
 * that is a plain copy of a parameter column, the parameter column it copies.
 * @param sources the source of each output column that copies one; a column that is
 *                computed rather than copied is absent */
record Provenance(Map<Column, Source> sources) {
    /** A column of a function's parameter. */
    record Source(int parameter, Column column) implements Comparable<Source> {
        /** The source representing parameter 0 accessing {@code column} */
        static Source reading(Column column) {
            return new Source(0, column);
        }

        /** This source's column, if it is a column of the first parameter and {@code shape}
         * has it, else null. */
        @Nullable
        Column inputColumn(CollectionShape shape) {
            return this.parameter == 0 && shape.contains(this.column) ? this.column : null;
        }

        @Override
        public int compareTo(Source other) {
            int compare = Integer.compare(this.parameter, other.parameter);
            if (compare != 0)
                return compare;
            return this.column.compareTo(other.column);
        }

        @Override
        public String toString() {
            return "p" + this.parameter + "." + this.column;
        }
    }

    /** A set of parameter columns.  As the symbolic value of a scalar expression, it
     * states that the value is a copy of one of these columns. */
    record SourceSet(TreeSet<Source> set) implements Lineage.LatticeValue<SourceSet> {
        SourceSet(Source source) {
            this(new TreeSet<>());
            this.set.add(source);
        }

        /** The only column in the set, or null if the set has more than one */
        @Nullable
        Source single() {
            return this.set.size() == 1 ? this.set.first() : null;
        }

        @Override
        public SourceSet union(SourceSet other) {
            TreeSet<Source> union = new TreeSet<>(this.set);
            union.addAll(other.set);
            return new SourceSet(union);
        }

        @Override
        public SourceSet intersect(SourceSet other) {
            TreeSet<Source> intersection = new TreeSet<>(this.set);
            intersection.retainAll(other.set);
            return new SourceSet(intersection);
        }

        @Override
        public boolean isBottom() {
            return this.set.isEmpty();
        }

        @Override
        public String toString() {
            return this.set.toString();
        }
    }

    Provenance {
        sources = Map.copyOf(sources);
    }

    /** The output columns that copy {@code source}. */
    List<Column> columnsReading(Source source) {
        List<Column> result = new ArrayList<>();
        for (var entry : this.sources.entrySet())
            if (source.equals(entry.getValue()))
                result.add(entry.getKey());
        Collections.sort(result);
        return result;
    }

    /** Output columns that copy the same parameter column hold the same data. */
    ColumnEquivalence equivalence() {
        Map<Source, List<Column>> bySource = new HashMap<>();
        for (var entry : this.sources.entrySet())
            bySource.computeIfAbsent(entry.getValue(), source -> new ArrayList<>()).add(entry.getKey());
        return ColumnEquivalence.of(bySource.values());
    }
}
