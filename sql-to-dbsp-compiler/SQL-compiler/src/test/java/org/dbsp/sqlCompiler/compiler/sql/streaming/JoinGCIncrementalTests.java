package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CountGCOperators;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Comprehensive test covering GC of joins: inner, outer, and ASOF joins, 
 * with a waterline on either input or on both. */
public class JoinGCIncrementalTests extends StreamingTestBase {
    /** A row of the left input: key k, time t, value v. */
    record Left(int k, @Nullable Integer t, int v) {}

    /** A row of the right input: key k, time t, value v. */
    record Right(int k, @Nullable Integer t, int v) {}

    /** The LATENESS of the columns that have one. */
    static final int LATENESS = 10;

    /** A join of left input l and right input r.
     * @param name      Name of the view.
     * @param join      The text that follows the left input {@code l}: a join with the right
     *                  input {@code R r} and its condition, such as {@code LEFT JOIN R r ON l.t = r.t},
     *                  or a WHERE clause whose subquery reads {@code R r}.  {@code R} is replaced by
     *                  the name of the right table.
     * @param hasRight  True if the output has the columns of the right input; false for EXISTS,
     *                  NOT EXISTS, and NOT IN, whose output rows are rows of the left input. */
    record JoinShape(String name, String join, boolean hasRight) {
        /** The FROM clause, and WHERE clause if any, that join table {@code left} with table {@code right}. */
        String from(String left, String right) {
            return "FROM " + left + " l " + this.join.replaceAll("\\bR r\\b", right + " r");
        }
    }

    static final List<JoinShape> SHAPES = List.of(
            new JoinShape("inner_t", "JOIN R r ON l.t = r.t", true),
            new JoinShape("inner_kt", "JOIN R r ON l.k = r.k AND l.t = r.t", true),
            new JoinShape("inner_expr", "JOIN R r ON l.t + 1 = r.t", true),
            new JoinShape("inner_k", "JOIN R r ON l.k = r.k", true),
            new JoinShape("inner_filter", "JOIN R r ON l.k = r.k AND l.t >= r.t", true),
            new JoinShape("left_t", "LEFT JOIN R r ON l.t = r.t", true),
            new JoinShape("left_kt", "LEFT JOIN R r ON l.k = r.k AND l.t = r.t", true),
            new JoinShape("left_k", "LEFT JOIN R r ON l.k = r.k", true),
            new JoinShape("left_filter_anti", "LEFT JOIN R r ON l.k = r.k AND l.t >= r.t", true),
            new JoinShape("right_t", "RIGHT JOIN R r ON l.t = r.t", true),
            new JoinShape("right_k", "RIGHT JOIN R r ON l.k = r.k", true),
            new JoinShape("full_t_anti", "FULL JOIN R r ON l.t = r.t", true),
            new JoinShape("full_k_anti", "FULL JOIN R r ON l.k = r.k", true),
            new JoinShape("asof", "LEFT ASOF JOIN R r MATCH_CONDITION (l.t >= r.t) ON l.k = r.k", true),
            new JoinShape("not_exists_t", "WHERE NOT EXISTS (SELECT 1 FROM R r WHERE r.t = l.t)", false),
            new JoinShape("not_exists_k", "WHERE NOT EXISTS (SELECT 1 FROM R r WHERE r.k = l.k)", false),
            new JoinShape("exists_t", "WHERE EXISTS (SELECT 1 FROM R r WHERE r.t = l.t)", false),
            new JoinShape("exists_k", "WHERE EXISTS (SELECT 1 FROM R r WHERE r.k = l.k)", false),
            new JoinShape("not_in_t", "WHERE l.t NOT IN (SELECT r.t FROM R r)", false));

    /** The columns of LATE_ALL and PLAIN_ALL, all VARCHAR: j names the branch (the shape and the
     * inputs with LATENESS), lk, lt, lv are the columns of the left row, and rk, rt, rv those of the right
     * row.  A branch that counts the join's output rows for each l.t, or for each r.t, puts that
     * time in lt and the count in lv. */
    static final List<String> COLUMNS = List.of("j", "lk", "lt", "lv", "rk", "rt", "rv");

    /** One SELECT per shape of {@code shapes}, over left input {@code left} and right input {@code right}.  The
     * casts remove the waterlines, so that GC does not prune the traces of the operators that
     * compute view D, which compares the two sides.
     * @param counts  True to add, for each shape, the number of output rows for each l.t and for
     *                each r.t. */
    static List<String> selects(List<JoinShape> shapes, String label, String left, String right, boolean counts) {
        List<String> result = new ArrayList<>();
        for (JoinShape shape : shapes) {
            String from = shape.from(left, right);
            String rightColumns = shape.hasRight ?
                    "CAST(r.k AS VARCHAR) AS rk, CAST(r.t AS VARCHAR) AS rt, CAST(r.v AS VARCHAR) AS rv " :
                    "CAST(NULL AS VARCHAR) AS rk, CAST(NULL AS VARCHAR) AS rt, CAST(NULL AS VARCHAR) AS rv ";
            result.add("SELECT '" + shape.name + label + "' AS j, " +
                    "CAST(l.k AS VARCHAR) AS lk, CAST(l.t AS VARCHAR) AS lt, CAST(l.v AS VARCHAR) AS lv, " +
                    rightColumns + from);
            if (!counts)
                continue;
            String[] sides = shape.hasRight ? new String[] { "l", "r" } : new String[] { "l" };
            for (String side : sides) {
                String column = side + "t";
                result.add("SELECT 'count_" + column + "_" + shape.name + label + "' AS j, " +
                        "CAST(NULL AS VARCHAR) AS lk, CAST(" + column + " AS VARCHAR) AS lt, " +
                        "CAST(COUNT(*) AS VARCHAR) AS lv, CAST(NULL AS VARCHAR) AS rk, " +
                        "CAST(NULL AS VARCHAR) AS rt, CAST(NULL AS VARCHAR) AS rv " +
                        "FROM (SELECT " + side + ".t AS " + column + " " + from + ") joined GROUP BY " + column);
            }
        }
        return result;
    }

    /** Which inputs have LATENESS on t, and how much. */
    enum LateInputs {
        /** The left input only. */
        L(LATENESS, 0),
        /** The right input only. */
        R(0, LATENESS),
        /** Both inputs. */
        LR(LATENESS, LATENESS),
        /** Both inputs, with a larger LATENESS on the right, so that the right input accepts
         * older rows than the left. */
        LR_LARGE(LATENESS, 6 * LATENESS);

        /** The LATENESS of t in the left input; 0 for none. */
        final int left;
        /** The LATENESS of t in the right input; 0 for none. */
        final int right;

        LateInputs(int left, int right) {
            this.left = left;
            this.right = right;
        }
    }

    /** Two table pairs for each choice of the inputs with LATENESS, LEFT_choice and RIGHT_choice,
     * with LATENESS on t in the inputs that the choice names.
     * <pre>
     * CREATE TABLE LATE_LEFT_L (k INT NOT NULL, t INT LATENESS 10, v INT NOT NULL);
     * CREATE TABLE LATE_RIGHT_L (k INT NOT NULL, t INT, v INT NOT NULL);
     * </pre> */
    static List<TablePair<?>> createTablePairs() {
        List<TablePair<?>> result = new ArrayList<>();
        for (LateInputs lateInputs : LateInputs.values()) {
            int left = lateInputs.left;
            int right = lateInputs.right;
            result.add(new TablePair<>("LEFT_" + lateInputs, true, Left.class,
                    left == 0 ? Set.of() : Set.of("t"), left));
            result.add(new TablePair<>("RIGHT_" + lateInputs, true, Right.class,
                    right == 0 ? Set.of() : Set.of("t"), right));
        }
        return result;
    }

    /* The generated program for the shape inner_t starts with
     * CREATE TABLE LATE_LEFT_L (k INT NOT NULL, t INT LATENESS 10, v INT NOT NULL);
     * ...
     * CREATE LOCAL VIEW LATE_ALL AS
     * SELECT 'inner_t_L' AS j, CAST(l.k AS VARCHAR) AS lk, ..., CAST(r.v AS VARCHAR) AS rv
     *   FROM LATE_LEFT_L l JOIN LATE_RIGHT_L r ON l.t = r.t
     * UNION ALL ... the counts per l.t and per r.t, then the same over LEFT_R and RIGHT_R, ...;
     */
    static String differentialProgram(List<TablePair<?>> pairs, JoinShape shape) {
        StringBuilder sql = new StringBuilder();
        for (TablePair<?> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (LateInputs lateInputs : LateInputs.values())
                branches.addAll(selects(List.of(shape), "_" + lateInputs, prefix + "LEFT_" + lateInputs,
                        prefix + "RIGHT_" + lateInputs, true));
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(COLUMNS));
        return sql.toString();
    }

    /** The rows of the first step of the differential test. */
    static final List<Record> INITIAL = List.of(
            new Left(0, 100, 1), new Left(0, 101, 2), new Left(1, 100, 3), new Left(1, 103, 4),
            new Left(2, 105, 5), new Left(0, null, 6), new Left(0, 110, 7),
            new Right(0, 100, 10), new Right(1, 100, 11), new Right(1, 99, 12), new Right(1, 104, 13),
            new Right(0, 102, 14), new Right(2, null, 15), new Right(3, 106, 16), new Right(0, 111, 17));

    /** Every join shape, and every choice of the inputs with LATENESS, gives the same output with and
     * without LATENESS.  One program per shape: shapes that share an index share one integral, and
     * a shape without GC would then remove the GC of the others. */
    @Test
    public void sameOutputWithoutLateness() {
        for (JoinShape shape : SHAPES) {
            List<TablePair<?>> pairs = createTablePairs();
            var tester = new DifferentialTester(this.getCCS(differentialProgram(pairs, shape)), pairs, COLUMNS);
            steps(tester);
        }
    }

    /** The steps of the differential test. */
    static void steps(DifferentialTester tester) {
        // No waterline yet: matches on t, on k, and on t + 1, unmatched rows on both sides,
        // and a NULL t on each side
        tester.insert(INITIAL.toArray(new Record[0]));
        // Group 9 carries the time: move every waterline to 140
        tester.insert(new Left(9, 150, 0), new Right(9, 150, 0));
        // New matches at the waterline, on both sides in one step
        tester.insert(new Left(0, 140, 20), new Right(0, 140, 21));
        // A right row far below the left waterline, where only the left input has a waterline:
        // it matches left rows at t = 100 that GC must have kept
        tester.insert(Set.of("RIGHT_L"), new Right(0, 100, 24));
        // The mirror case: a left row far below the right waterline
        tester.insert(Set.of("LEFT_R"), new Left(1, 99, 25));
        // A right row that gives an unmatched left row of the outer joins its first match
        tester.insert(Set.of("RIGHT_L"), new Right(4, 105, 26));
        // Rows with a NULL t, which are never late, and the delete of an old one
        tester.insert(Set.of("LEFT_LR", "RIGHT_LR"), new Left(1, null, 27), new Right(0, null, 28));
        tester.delete(Set.of("LEFT_LR"), new Left(0, null, 6));
        // Retract a match at the waterline
        tester.delete(Set.of("RIGHT_LR"), new Right(0, 140, 21));
        // A left row at an old t where both inputs already have rows, where only the right input has
        // a waterline: the right rows at that t stay matched
        tester.insert(Set.of("LEFT_R"), new Left(0, 100, 40));
        // Delete the only right match of an old left row: the outer joins null-pad it again
        tester.delete(Set.of("RIGHT_L"), new Right(4, 105, 26));
        // A new left row whose ASOF match is an old right row below the waterline
        tester.insert(Set.of("LEFT_R", "LEFT_LR", "LEFT_LR_LARGE"), new Left(3, 145, 33));
        // Move every waterline to 190, then change both sides again after the second compaction
        tester.insert(new Left(9, 200, 0), new Right(9, 200, 0));
        tester.insert(Set.of("LEFT_LR", "RIGHT_LR"), new Left(0, 195, 29), new Right(0, 191, 31));
        // A right row that only the key k joins with old left rows
        tester.insert(Set.of("RIGHT_L"), new Right(1, 120, 32));
        // Delete the right rows with a NULL t, which are never late: NOT IN, which returned no
        // rows while a NULL was present, now returns old left rows
        tester.delete(Set.of("RIGHT_L", "RIGHT_R", "RIGHT_LR", "RIGHT_LR_LARGE"),
                new Right(2, null, 15), new Right(0, null, 28));
        // A left row whose key no right row has, then the first right row with that key
        tester.insert(Set.of("LEFT_L", "LEFT_R", "LEFT_LR", "LEFT_LR_LARGE"), new Left(5, 196, 34));
        tester.insert(Set.of("RIGHT_L", "RIGHT_R", "RIGHT_LR", "RIGHT_LR_LARGE"), new Right(5, 197, 35));
        // A right row with a NULL t again: NOT IN returns no rows
        tester.insert(Set.of("RIGHT_L", "RIGHT_R", "RIGHT_LR", "RIGHT_LR_LARGE"), new Right(6, null, 36));
    }

    /** The rows of the first step, inserted in one step into tables without LATENESS. */
    static String insertInitial() {
        StringBuilder result = new StringBuilder();
        for (Record row : INITIAL) {
            if (row instanceof Left l)
                result.append("INSERT INTO L VALUES (").append(l.k).append(", ").append(l.t).append(", ").append(l.v).append(");\n");
            else if (row instanceof Right r)
                result.append("INSERT INTO R VALUES (").append(r.k).append(", ").append(r.t).append(", ").append(r.v).append(");\n");
        }
        return result.toString();
    }

    /** Every join shape computes the results of Postgres over the rows of the first step of the
     * differential test.  Expected outputs computed by Postgres, where the ASOF join is a
     * {@code LEFT JOIN LATERAL} that picks the latest right row at or before {@code l.t}; no two
     * right rows with the same key have the same t. */
    @Test
    public void sameOutputAsPostgres() {
        String program = """
                CREATE TABLE L (k INT NOT NULL, t INT, v INT NOT NULL);
                CREATE TABLE R (k INT NOT NULL, t INT, v INT NOT NULL);
                CREATE VIEW V AS
                """ + String.join("\nUNION ALL ", selects(SHAPES, "", "L", "R", false)) + ";";
        var ccs = this.getCCS(program).withStringTrim();
        ccs.stepWeightOne(insertInitial(), """
             j                | lk   | lt   | lv   | rk   | rt   | rv
            ---------------------------------------------------------
             asof             | 0    | 100  | 1    | 0    | 100  | 10
             asof             | 0    | 101  | 2    | 0    | 100  | 10
             asof             | 0    | 110  | 7    | 0    | 102  | 14
             asof             | 0    |NULL  | 6    |NULL  |NULL  |NULL
             asof             | 1    | 100  | 3    | 1    | 100  | 11
             asof             | 1    | 103  | 4    | 1    | 100  | 11
             asof             | 2    | 105  | 5    |NULL  |NULL  |NULL
             exists_k         | 0    | 100  | 1    |NULL  |NULL  |NULL
             exists_k         | 0    | 101  | 2    |NULL  |NULL  |NULL
             exists_k         | 0    | 110  | 7    |NULL  |NULL  |NULL
             exists_k         | 0    |NULL  | 6    |NULL  |NULL  |NULL
             exists_k         | 1    | 100  | 3    |NULL  |NULL  |NULL
             exists_k         | 1    | 103  | 4    |NULL  |NULL  |NULL
             exists_k         | 2    | 105  | 5    |NULL  |NULL  |NULL
             exists_t         | 0    | 100  | 1    |NULL  |NULL  |NULL
             exists_t         | 1    | 100  | 3    |NULL  |NULL  |NULL
             full_k_anti      | 0    | 100  | 1    | 0    | 100  | 10
             full_k_anti      | 0    | 100  | 1    | 0    | 102  | 14
             full_k_anti      | 0    | 100  | 1    | 0    | 111  | 17
             full_k_anti      | 0    | 101  | 2    | 0    | 100  | 10
             full_k_anti      | 0    | 101  | 2    | 0    | 102  | 14
             full_k_anti      | 0    | 101  | 2    | 0    | 111  | 17
             full_k_anti      | 0    | 110  | 7    | 0    | 100  | 10
             full_k_anti      | 0    | 110  | 7    | 0    | 102  | 14
             full_k_anti      | 0    | 110  | 7    | 0    | 111  | 17
             full_k_anti      | 0    |NULL  | 6    | 0    | 100  | 10
             full_k_anti      | 0    |NULL  | 6    | 0    | 102  | 14
             full_k_anti      | 0    |NULL  | 6    | 0    | 111  | 17
             full_k_anti      | 1    | 100  | 3    | 1    | 100  | 11
             full_k_anti      | 1    | 100  | 3    | 1    | 104  | 13
             full_k_anti      | 1    | 100  | 3    | 1    | 99   | 12
             full_k_anti      | 1    | 103  | 4    | 1    | 100  | 11
             full_k_anti      | 1    | 103  | 4    | 1    | 104  | 13
             full_k_anti      | 1    | 103  | 4    | 1    | 99   | 12
             full_k_anti      | 2    | 105  | 5    | 2    |NULL  | 15
             full_k_anti      |NULL  |NULL  |NULL  | 3    | 106  | 16
             full_t_anti      | 0    | 100  | 1    | 0    | 100  | 10
             full_t_anti      | 0    | 100  | 1    | 1    | 100  | 11
             full_t_anti      | 0    | 101  | 2    |NULL  |NULL  |NULL
             full_t_anti      | 0    | 110  | 7    |NULL  |NULL  |NULL
             full_t_anti      | 0    |NULL  | 6    |NULL  |NULL  |NULL
             full_t_anti      | 1    | 100  | 3    | 0    | 100  | 10
             full_t_anti      | 1    | 100  | 3    | 1    | 100  | 11
             full_t_anti      | 1    | 103  | 4    |NULL  |NULL  |NULL
             full_t_anti      | 2    | 105  | 5    |NULL  |NULL  |NULL
             full_t_anti      |NULL  |NULL  |NULL  | 0    | 102  | 14
             full_t_anti      |NULL  |NULL  |NULL  | 0    | 111  | 17
             full_t_anti      |NULL  |NULL  |NULL  | 1    | 104  | 13
             full_t_anti      |NULL  |NULL  |NULL  | 1    | 99   | 12
             full_t_anti      |NULL  |NULL  |NULL  | 2    |NULL  | 15
             full_t_anti      |NULL  |NULL  |NULL  | 3    | 106  | 16
             inner_expr       | 0    | 101  | 2    | 0    | 102  | 14
             inner_expr       | 0    | 110  | 7    | 0    | 111  | 17
             inner_expr       | 1    | 103  | 4    | 1    | 104  | 13
             inner_expr       | 2    | 105  | 5    | 3    | 106  | 16
             inner_filter     | 0    | 100  | 1    | 0    | 100  | 10
             inner_filter     | 0    | 101  | 2    | 0    | 100  | 10
             inner_filter     | 0    | 110  | 7    | 0    | 100  | 10
             inner_filter     | 0    | 110  | 7    | 0    | 102  | 14
             inner_filter     | 1    | 100  | 3    | 1    | 100  | 11
             inner_filter     | 1    | 100  | 3    | 1    | 99   | 12
             inner_filter     | 1    | 103  | 4    | 1    | 100  | 11
             inner_filter     | 1    | 103  | 4    | 1    | 99   | 12
             inner_k          | 0    | 100  | 1    | 0    | 100  | 10
             inner_k          | 0    | 100  | 1    | 0    | 102  | 14
             inner_k          | 0    | 100  | 1    | 0    | 111  | 17
             inner_k          | 0    | 101  | 2    | 0    | 100  | 10
             inner_k          | 0    | 101  | 2    | 0    | 102  | 14
             inner_k          | 0    | 101  | 2    | 0    | 111  | 17
             inner_k          | 0    | 110  | 7    | 0    | 100  | 10
             inner_k          | 0    | 110  | 7    | 0    | 102  | 14
             inner_k          | 0    | 110  | 7    | 0    | 111  | 17
             inner_k          | 0    |NULL  | 6    | 0    | 100  | 10
             inner_k          | 0    |NULL  | 6    | 0    | 102  | 14
             inner_k          | 0    |NULL  | 6    | 0    | 111  | 17
             inner_k          | 1    | 100  | 3    | 1    | 100  | 11
             inner_k          | 1    | 100  | 3    | 1    | 104  | 13
             inner_k          | 1    | 100  | 3    | 1    | 99   | 12
             inner_k          | 1    | 103  | 4    | 1    | 100  | 11
             inner_k          | 1    | 103  | 4    | 1    | 104  | 13
             inner_k          | 1    | 103  | 4    | 1    | 99   | 12
             inner_k          | 2    | 105  | 5    | 2    |NULL  | 15
             inner_kt         | 0    | 100  | 1    | 0    | 100  | 10
             inner_kt         | 1    | 100  | 3    | 1    | 100  | 11
             inner_t          | 0    | 100  | 1    | 0    | 100  | 10
             inner_t          | 0    | 100  | 1    | 1    | 100  | 11
             inner_t          | 1    | 100  | 3    | 0    | 100  | 10
             inner_t          | 1    | 100  | 3    | 1    | 100  | 11
             left_filter_anti | 0    | 100  | 1    | 0    | 100  | 10
             left_filter_anti | 0    | 101  | 2    | 0    | 100  | 10
             left_filter_anti | 0    | 110  | 7    | 0    | 100  | 10
             left_filter_anti | 0    | 110  | 7    | 0    | 102  | 14
             left_filter_anti | 0    |NULL  | 6    |NULL  |NULL  |NULL
             left_filter_anti | 1    | 100  | 3    | 1    | 100  | 11
             left_filter_anti | 1    | 100  | 3    | 1    | 99   | 12
             left_filter_anti | 1    | 103  | 4    | 1    | 100  | 11
             left_filter_anti | 1    | 103  | 4    | 1    | 99   | 12
             left_filter_anti | 2    | 105  | 5    |NULL  |NULL  |NULL
             left_k           | 0    | 100  | 1    | 0    | 100  | 10
             left_k           | 0    | 100  | 1    | 0    | 102  | 14
             left_k           | 0    | 100  | 1    | 0    | 111  | 17
             left_k           | 0    | 101  | 2    | 0    | 100  | 10
             left_k           | 0    | 101  | 2    | 0    | 102  | 14
             left_k           | 0    | 101  | 2    | 0    | 111  | 17
             left_k           | 0    | 110  | 7    | 0    | 100  | 10
             left_k           | 0    | 110  | 7    | 0    | 102  | 14
             left_k           | 0    | 110  | 7    | 0    | 111  | 17
             left_k           | 0    |NULL  | 6    | 0    | 100  | 10
             left_k           | 0    |NULL  | 6    | 0    | 102  | 14
             left_k           | 0    |NULL  | 6    | 0    | 111  | 17
             left_k           | 1    | 100  | 3    | 1    | 100  | 11
             left_k           | 1    | 100  | 3    | 1    | 104  | 13
             left_k           | 1    | 100  | 3    | 1    | 99   | 12
             left_k           | 1    | 103  | 4    | 1    | 100  | 11
             left_k           | 1    | 103  | 4    | 1    | 104  | 13
             left_k           | 1    | 103  | 4    | 1    | 99   | 12
             left_k           | 2    | 105  | 5    | 2    |NULL  | 15
             left_kt          | 0    | 100  | 1    | 0    | 100  | 10
             left_kt          | 0    | 101  | 2    |NULL  |NULL  |NULL
             left_kt          | 0    | 110  | 7    |NULL  |NULL  |NULL
             left_kt          | 0    |NULL  | 6    |NULL  |NULL  |NULL
             left_kt          | 1    | 100  | 3    | 1    | 100  | 11
             left_kt          | 1    | 103  | 4    |NULL  |NULL  |NULL
             left_kt          | 2    | 105  | 5    |NULL  |NULL  |NULL
             left_t           | 0    | 100  | 1    | 0    | 100  | 10
             left_t           | 0    | 100  | 1    | 1    | 100  | 11
             left_t           | 0    | 101  | 2    |NULL  |NULL  |NULL
             left_t           | 0    | 110  | 7    |NULL  |NULL  |NULL
             left_t           | 0    |NULL  | 6    |NULL  |NULL  |NULL
             left_t           | 1    | 100  | 3    | 0    | 100  | 10
             left_t           | 1    | 100  | 3    | 1    | 100  | 11
             left_t           | 1    | 103  | 4    |NULL  |NULL  |NULL
             left_t           | 2    | 105  | 5    |NULL  |NULL  |NULL
             not_exists_t     | 0    | 101  | 2    |NULL  |NULL  |NULL
             not_exists_t     | 0    | 110  | 7    |NULL  |NULL  |NULL
             not_exists_t     | 0    |NULL  | 6    |NULL  |NULL  |NULL
             not_exists_t     | 1    | 103  | 4    |NULL  |NULL  |NULL
             not_exists_t     | 2    | 105  | 5    |NULL  |NULL  |NULL
             right_k          | 0    | 100  | 1    | 0    | 100  | 10
             right_k          | 0    | 100  | 1    | 0    | 102  | 14
             right_k          | 0    | 100  | 1    | 0    | 111  | 17
             right_k          | 0    | 101  | 2    | 0    | 100  | 10
             right_k          | 0    | 101  | 2    | 0    | 102  | 14
             right_k          | 0    | 101  | 2    | 0    | 111  | 17
             right_k          | 0    | 110  | 7    | 0    | 100  | 10
             right_k          | 0    | 110  | 7    | 0    | 102  | 14
             right_k          | 0    | 110  | 7    | 0    | 111  | 17
             right_k          | 0    |NULL  | 6    | 0    | 100  | 10
             right_k          | 0    |NULL  | 6    | 0    | 102  | 14
             right_k          | 0    |NULL  | 6    | 0    | 111  | 17
             right_k          | 1    | 100  | 3    | 1    | 100  | 11
             right_k          | 1    | 100  | 3    | 1    | 104  | 13
             right_k          | 1    | 100  | 3    | 1    | 99   | 12
             right_k          | 1    | 103  | 4    | 1    | 100  | 11
             right_k          | 1    | 103  | 4    | 1    | 104  | 13
             right_k          | 1    | 103  | 4    | 1    | 99   | 12
             right_k          | 2    | 105  | 5    | 2    |NULL  | 15
             right_k          |NULL  |NULL  |NULL  | 3    | 106  | 16
             right_t          | 0    | 100  | 1    | 0    | 100  | 10
             right_t          | 0    | 100  | 1    | 1    | 100  | 11
             right_t          | 1    | 100  | 3    | 0    | 100  | 10
             right_t          | 1    | 100  | 3    | 1    | 100  | 11
             right_t          |NULL  |NULL  |NULL  | 0    | 102  | 14
             right_t          |NULL  |NULL  |NULL  | 0    | 111  | 17
             right_t          |NULL  |NULL  |NULL  | 1    | 104  | 13
             right_t          |NULL  |NULL  |NULL  | 1    | 99   | 12
             right_t          |NULL  |NULL  |NULL  | 2    |NULL  | 15
             right_t          |NULL  |NULL  |NULL  | 3    | 106  | 16""");
    }

    /** The GC operators of each join shape, for each choice of the inputs with LATENESS: K is a
     * RetainKeys operator, V a RetainValues operator, and N a RetainNValues operator. */
    static final String EXPECTED_GC = """
            shape            | L        | R        | LR       | LR_LARGE
            inner_t          | K        | K        | KK       | KK
            inner_kt         | K        | K        | KK       | KK
            inner_expr       | K        | K        | KK       | KK
            inner_k          | -        | -        | -        | -
            inner_filter     | -        | V        | V        | V
            left_t           | -        | K        | KK       | KK
            left_kt          | -        | K        | KK       | KK
            left_k           | -        | -        | -        | -
            left_filter_anti | -        | V        | V        | V
            right_t          | K        | -        | KK       | KK
            right_k          | -        | -        | -        | -
            full_t_anti      | -        | -        | -        | -
            full_k_anti      | -        | -        | -        | -
            asof             | -        | -        | NV       | NV
            not_exists_t     | -        | KKN      | KKN      | KKN
            not_exists_k     | -        | N        | N        | N
            exists_t         | K        | KK       | KKK      | KKK
            exists_k         | -        | -        | -        | -
            not_in_t         | -        | KKN      | KKN      | KKN
            """;

    /** The declaration of an input of a join, with LATENESS on t unless {@code lateness} is 0. */
    static String createInput(String name, int lateness) {
        return "CREATE TABLE " + name + " (k INT NOT NULL, t INT" +
                (lateness == 0 ? "" : " LATENESS " + lateness) + ", v INT NOT NULL);\n";
    }

    /** Each join shape has GC exactly where the waterlines allow it and no consumer keeps a full
     * copy of the input. */
    @Test
    public void gcOperators() {
        StringBuilder header = new StringBuilder(String.format("%-16s", "shape"));
        for (LateInputs lateInputs : LateInputs.values())
            header.append(String.format(" | %-8s", lateInputs));
        StringBuilder actual = new StringBuilder(header.toString().stripTrailing()).append("\n");
        for (JoinShape shape : SHAPES) {
            StringBuilder line = new StringBuilder(String.format("%-16s", shape.name));
            for (LateInputs lateInputs : LateInputs.values()) {
                String sql = createInput("L", lateInputs.left) + createInput("R", lateInputs.right) +
                        "CREATE VIEW V AS SELECT * " + shape.from("L", "R") + ";";
                var cc = this.getCC(sql);
                CountGCOperators operators = new CountGCOperators(cc.compiler);
                cc.visit(operators);
                line.append(String.format(" | %-8s", operators.kinds()));
            }
            // A text block has no trailing spaces
            actual.append(line.toString().stripTrailing()).append("\n");
        }
        Assert.assertEquals(EXPECTED_GC, actual.toString());
    }

    /** A view with four aggregates over table S, which compiles to a star join of the four
     * aggregates on the group key.
     * @param name   Name of the view.
     * @param query  The query; {@code S} is replaced by the name of the table. */
    record StarView(String name, String query) {
        /** The query over table {@code table}. */
        String over(String table) {
            return this.query.replaceAll("\\bS\\b", table);
        }
    }

    /** The casts remove the waterlines, so that GC does not prune the traces of the operators
     * that compute view D, which compares the two sides. */
    static final List<StarView> STAR_VIEWS = List.of(
            new StarView("star_t", "SELECT 'star_t' AS j, CAST(NULL AS VARCHAR) AS k, CAST(t AS VARCHAR) AS t, " +
                    "CAST(MIN(v) AS VARCHAR) AS a1, CAST(MAX(v) AS VARCHAR) AS a2, " +
                    "CAST(SUM(v) AS VARCHAR) AS a3, CAST(COUNT(*) AS VARCHAR) AS a4 FROM S GROUP BY t"),
            new StarView("star_kt", "SELECT 'star_kt' AS j, CAST(k AS VARCHAR) AS k, CAST(t AS VARCHAR) AS t, " +
                    "CAST(MIN(v) AS VARCHAR) AS a1, CAST(MAX(v) AS VARCHAR) AS a2, " +
                    "CAST(SUM(v) AS VARCHAR) AS a3, CAST(COUNT(*) AS VARCHAR) AS a4 FROM S GROUP BY k, t"),
            new StarView("star_k", "SELECT 'star_k' AS j, CAST(k AS VARCHAR) AS k, CAST(NULL AS VARCHAR) AS t, " +
                    "CAST(MIN(t) AS VARCHAR) AS a1, CAST(MAX(v) AS VARCHAR) AS a2, " +
                    "CAST(SUM(v) AS VARCHAR) AS a3, CAST(COUNT(*) AS VARCHAR) AS a4 FROM S GROUP BY k"),
            new StarView("star_t_having", "SELECT 'star_t_having' AS j, CAST(NULL AS VARCHAR) AS k, " +
                    "CAST(t AS VARCHAR) AS t, CAST(MIN(v) AS VARCHAR) AS a1, CAST(MAX(v) AS VARCHAR) AS a2, " +
                    "CAST(SUM(v) AS VARCHAR) AS a3, CAST(COUNT(*) AS VARCHAR) AS a4 FROM S GROUP BY t " +
                    "HAVING COUNT(*) > 1"));

    /** The columns of the star views, all VARCHAR: the view, the group key k and t, and the four
     * aggregates. */
    static final List<String> STAR_COLUMNS = List.of("j", "k", "t", "a1", "a2", "a3", "a4");

    /** Views with several aggregates give the same output with and without LATENESS on t. */
    @Test
    public void starJoinsSameOutputWithoutLateness() {
        TablePair<Left> pair = new TablePair<>("STAR", true, Left.class, Set.of("t"), LATENESS);
        StringBuilder sql = new StringBuilder();
        sql.append(pair.create("LATE_", true)).append(pair.create("PLAIN_", false));
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (StarView view : STAR_VIEWS)
                branches.add(view.over(prefix + "STAR"));
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(STAR_COLUMNS));
        var tester = new DifferentialTester(this.getCCS(sql.toString()), List.of(pair), STAR_COLUMNS);
        // No waterline yet: groups of one and of several rows, and a NULL t
        List<Record> lefts = new ArrayList<>();
        for (Record row : INITIAL)
            if (row instanceof Left)
                lefts.add(row);
        tester.insert(lefts.toArray(new Record[0]));
        // Group 9 carries the time: move the waterline to 140
        tester.insert(new Left(9, 150, 0));
        // New rows at the waterline, one joining an existing group of k
        tester.insert(Set.of("STAR"), new Left(0, 140, 20), new Left(1, 141, 21));
        // Delete one of them: the aggregates of its groups change back
        tester.delete(Set.of("STAR"), new Left(0, 140, 20));
        // Rows with a NULL t, which are never late, and the delete of an old one
        tester.insert(Set.of("STAR"), new Left(2, null, 22));
        tester.delete(Set.of("STAR"), new Left(0, null, 6));
        // Move the waterline to 190, then change the groups again after the second compaction
        tester.insert(new Left(9, 200, 0));
        tester.insert(Set.of("STAR"), new Left(0, 195, 29), new Left(0, 195, 30));
    }

    /** The GC operators of each star view: K is a RetainKeys operator, N a RetainNValues operator. */
    static final String EXPECTED_STAR_GC = """
            star_t        | StarJoin      | KKKK
            star_kt       | StarJoin      | KKKK
            star_k        | StarJoin      | N
            star_t_having | StarJoinIndex | KKKK
            """;

    /** Each star view compiles to a star join, with GC where the waterline of t allows it. */
    @Test
    public void starJoinGcOperators() {
        StringBuilder actual = new StringBuilder();
        for (StarView view : STAR_VIEWS) {
            var cc = this.getCC(createInput("S", LATENESS) + "CREATE VIEW V AS " + view.over("S") + ";");
            CountGCOperators operators = new CountGCOperators(cc.compiler);
            cc.visit(operators);
            String line = String.format("%-13s | %-13s | %s", view.name,
                    String.join(" ", operators.starJoins), operators.kinds());
            actual.append(line.stripTrailing()).append("\n");
        }
        Assert.assertEquals(EXPECTED_STAR_GC, actual.toString());
    }

    /** The star views compute the results of Postgres over the left rows of the first step of the
     * join differential test.  Expected outputs computed by Postgres. */
    @Test
    public void starJoinsSameOutputAsPostgres() {
        List<String> branches = new ArrayList<>();
        for (StarView view : STAR_VIEWS)
            branches.add(view.over("S"));
        String program = createInput("S", 0) + "CREATE VIEW V AS\n" + String.join("\nUNION ALL ", branches) + ";";
        StringBuilder inserts = new StringBuilder();
        for (Record row : INITIAL)
            if (row instanceof Left l)
                inserts.append("INSERT INTO S VALUES (").append(l.k).append(", ").append(l.t)
                        .append(", ").append(l.v).append(");\n");
        var ccs = this.getCCS(program).withStringTrim();
        ccs.stepWeightOne(inserts.toString(), """
             j             | k    | t    | a1  | a2 | a3 | a4
            -------------------------------------------------
             star_k        | 0    |NULL  | 100 | 7  | 16 | 4
             star_k        | 1    |NULL  | 100 | 4  | 7  | 2
             star_k        | 2    |NULL  | 105 | 5  | 5  | 1
             star_kt       | 0    | 100  | 1   | 1  | 1  | 1
             star_kt       | 0    | 101  | 2   | 2  | 2  | 1
             star_kt       | 0    | 110  | 7   | 7  | 7  | 1
             star_kt       | 0    |NULL  | 6   | 6  | 6  | 1
             star_kt       | 1    | 100  | 3   | 3  | 3  | 1
             star_kt       | 1    | 103  | 4   | 4  | 4  | 1
             star_kt       | 2    | 105  | 5   | 5  | 5  | 1
             star_t        |NULL  | 100  | 1   | 3  | 4  | 2
             star_t        |NULL  | 101  | 2   | 2  | 2  | 1
             star_t        |NULL  | 103  | 4   | 4  | 4  | 1
             star_t        |NULL  | 105  | 5   | 5  | 5  | 1
             star_t        |NULL  | 110  | 7   | 7  | 7  | 1
             star_t        |NULL  |NULL  | 6   | 6  | 6  | 1
             star_t_having |NULL  | 100  | 1   | 3  | 4  | 2""");
    }
}
