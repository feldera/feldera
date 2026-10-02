package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;
import org.junit.Assert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Differential test of GC.  The program declares pairs of tables: in each pair, table
 * LATE_name has LATENESS on some columns, and table PLAIN_name has the same columns without
 * LATENESS.  View LATE_ALL computes the results under test over the LATE_ tables, and view
 * PLAIN_ALL computes the same results over the PLAIN_ tables.  The PLAIN_ tables are the oracle.
 * <ul>
 *   <li>A model of the LATE_ table's waterlines decides which changes are late.
 *   <li>Both tables of a pair receive the same changes.  The tester skips for each pair
 *       a change that is late for its LATE_ table; </li>
 *   <li>LATENESS only enables GC, so without late changes LATE_ALL and PLAIN_ALL must be
 *       equal.</li>
 *   <li>The circuit compacts after each step, so GC discards state on the LATE_ side.</li>
 *   <li>View D is the symmetric difference of LATE_ALL and PLAIN_ALL, plus the rows of
 *       ERROR_VIEW, where the circuit reports late rows</li>
 *   <li>Every step expects D to stay empty.</li>
 *   <li>The tests also assert that the circuit never receives late data: the model and
 *       the circuit agree on the waterlines.</li>
 * </ul>
 * The pairs can have rows of different types; a change goes to the pairs whose rows have its type. */
public final class DifferentialTester {
    final CompilerCircuitStream ccs;
    final List<TablePair<?>> pairs;
    /** Key: a table pair.  Value: the model of the contents and waterlines of its LATE_ table,
     * so that a change is applied to the pair only if it is not late. */
    final Map<TablePair<?>, LatenessModel<?>> models = new HashMap<>();
    /** The expected change of view D in every step: the header of its table and no rows. */
    final String expectedDChange;

    /** @param ccs      The stream of a program whose only output is view D.
     *  @param columns  The columns of views LATE_ALL and PLAIN_ALL. */
    public DifferentialTester(CompilerCircuitStream ccs, List<? extends TablePair<?>> pairs, List<String> columns) {
        this.ccs = ccs.compactAfterEachStep();
        this.pairs = List.copyOf(pairs);
        for (TablePair<?> pair : pairs)
            this.models.put(pair, new LatenessModel<>(pair.schema));
        this.expectedDChange = " side | " + String.join(" | ", columns) + " | weight\n---";
    }

    /** View D: the rows of LATE_ALL and PLAIN_ALL that only one side produces, and the rows
     * of ERROR_VIEW, such as the late rows.  LATE_ALL and PLAIN_ALL must have the same columns,
     * all VARCHAR; a row from ERROR_VIEW describes the error in the first column and has NULL
     * in the others.
     * <pre>
     * CREATE VIEW D AS
     * SELECT 'late' AS side, * FROM (SELECT * FROM LATE_ALL EXCEPT ALL SELECT * FROM PLAIN_ALL) late_extra
     * UNION ALL
     * SELECT 'plain' AS side, * FROM (SELECT * FROM PLAIN_ALL EXCEPT ALL SELECT * FROM LATE_ALL) plain_extra
     * UNION ALL
     * SELECT 'error' AS side, table_or_view_name || ': ' || message || ': ' || metadata AS c1, NULL AS c2, ... FROM ERROR_VIEW;
     * </pre>
     * @param columns  The columns of views LATE_ALL and PLAIN_ALL. */
    public static String differenceView(List<String> columns) {
        Utilities.enforce(!columns.isEmpty(), () -> "No columns");
        StringBuilder errors = new StringBuilder(
                "SELECT 'error' AS side, table_or_view_name || ': ' || message || ': ' || metadata AS ")
                .append(columns.get(0));
        for (int i = 1; i < columns.size(); i++)
            errors.append(", NULL AS ").append(columns.get(i));
        return """
                CREATE VIEW D AS
                SELECT 'late' AS side, * FROM (SELECT * FROM LATE_ALL EXCEPT ALL SELECT * FROM PLAIN_ALL) late_extra
                UNION ALL
                SELECT 'plain' AS side, * FROM (SELECT * FROM PLAIN_ALL EXCEPT ALL SELECT * FROM LATE_ALL) plain_extra
                UNION ALL
                """ + errors + " FROM ERROR_VIEW;";
    }

    /** Insert {@code rows} in one step. */
    public void insert(Record... rows) {
        this.step(Set.of(), List.of(rows), List.of());
    }

    /** Insert {@code rows} in one step, which must reach at least the pairs named {@code mustReach}. */
    public void insert(Set<String> mustReach, Record... rows) {
        this.step(mustReach, List.of(rows), List.of());
    }

    /** Delete {@code rows} in one step. */
    public void delete(Record... rows) {
        this.step(Set.of(), List.of(), List.of(rows));
    }

    /** Delete {@code rows} in one step, which must reach at least the pairs named {@code mustReach}. */
    public void delete(Set<String> mustReach, Record... rows) {
        this.step(mustReach, List.of(), List.of(rows));
    }

    /** The model of the LATE_ table of {@code pair}. */
    @SuppressWarnings("unchecked")
    <R extends Record> LatenessModel<R> model(TablePair<R> pair) {
        // The constructor creates the model of each pair from the schema of the same pair
        return (LatenessModel<R>) this.models.get(pair);
    }

    /** The statements inserting {@code row} into both tables of {@code pair}, or an empty
     * string if the row belongs to other pairs, does not fit, or is late. */
    <R extends Record> String insert(TablePair<R> pair, Record row) {
        if (!pair.rowClass.isInstance(row))
            return "";
        R typed = pair.rowClass.cast(row);
        if (!pair.fits(typed) || !this.model(pair).insert(typed))
            return "";
        return pair.statements("INSERT INTO ", typed);
    }

    /** The statements deleting {@code row} from both tables of {@code pair}, or an empty
     * string if the row belongs to other pairs, does not fit, is late, or is absent. */
    <R extends Record> String delete(TablePair<R> pair, Record row) {
        if (!pair.rowClass.isInstance(row))
            return "";
        R typed = pair.rowClass.cast(row);
        if (!pair.fits(typed) || !this.model(pair).delete(typed))
            return "";
        return pair.statements("REMOVE FROM ", typed);
    }

    /** Run one step: apply {@code inserts} and {@code deletes} to every pair that accepts them,
     * then check that view D does not change.  Each pair skips the rows of other pairs, the rows
     * that do not fit, and the rows that are late for its LATE_ table; the changes of the step
     * then move the waterlines of the next steps.
     * @param mustReach  Names of the pairs that must receive at least one change of the step. */
    void step(Set<String> mustReach, List<Record> inserts, List<Record> deletes) {
        StringBuilder sql = new StringBuilder();
        Set<String> reached = new HashSet<>();
        for (TablePair<?> pair : this.pairs) {
            StringBuilder changes = new StringBuilder();
            for (Record row : inserts)
                changes.append(this.insert(pair, row));
            for (Record row : deletes)
                changes.append(this.delete(pair, row));
            // The changes of the step move the waterlines of the next steps
            this.models.get(pair).commit();
            if (!changes.isEmpty())
                reached.add(pair.name);
            sql.append(changes);
        }
        Assert.assertTrue("Reached " + reached + ", expected " + mustReach, reached.containsAll(mustReach));
        this.ccs.step(sql.toString(), this.expectedDChange);
    }
}
