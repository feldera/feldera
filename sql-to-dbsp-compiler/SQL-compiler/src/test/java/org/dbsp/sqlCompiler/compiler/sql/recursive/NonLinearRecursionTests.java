package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

import java.util.Map;

/** Recursive programs that join relations of the recursion with each other.
 *
 * <p>The steps of each program include changes that make such a join compute
 * output for the same later iteration more than once.  Where a fact has
 * several derivations, a later step deletes all of them, which exposes any
 * that were lost.
 *
 * <p>The programs are ports of the Rust tests in
 * {@code crates/dbsp/src/operator/nonlinear_recursion_tests}.  All expected
 * outputs were cross-checked against them: fed the same steps, the Rust
 * programs produce the same outputs after every step.  The MIN column of
 * {@link #billOfMaterials}, which its Rust program lacks, was checked against
 * a variant of that program that computes it. */
public class NonLinearRecursionTests extends BaseSQLTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.optimizationLevel = 2;
        return options;
    }

    /** Two mutually recursive relations, each extended with paths of the other.
     *
     * <p>{@code e(x, y)} and {@code f(x, y)} are edges, and {@code r(x, y)} and
     * {@code s(x, y)} paths, from {@code x} to {@code y}:
     * <pre>
     * r(x, y) :- e(x, y).
     * r(x, z) :- r(x, y), s(y, z).
     * s(x, y) :- f(x, y).
     * s(x, z) :- s(x, y), r(y, z).
     * </pre>
     * Port of {@code MutualPaths} in {@code closure.rs}. */
    @Test
    public void mutualPaths() {
        String sql = """
                CREATE TABLE e(src INT NOT NULL, dst INT NOT NULL);
                CREATE TABLE f(src INT NOT NULL, dst INT NOT NULL);
                DECLARE RECURSIVE VIEW r(src INT NOT NULL, dst INT NOT NULL);
                DECLARE RECURSIVE VIEW s(src INT NOT NULL, dst INT NOT NULL);
                CREATE VIEW r AS
                SELECT src, dst FROM e
                UNION
                SELECT r.src, s.dst FROM r JOIN s ON r.dst = s.src;
                CREATE VIEW s AS
                SELECT src, dst FROM f
                UNION
                SELECT s.src, r.dst FROM s JOIN r ON s.dst = r.src;""";
        var ccs = this.getCCS(sql);
        // The chain 0 -> 1 -> ... -> 8 in both e and f.
        ccs.stepMultiView("""
                INSERT INTO e VALUES(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7),
                    (7, 8);
                INSERT INTO f VALUES(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7),
                    (7, 8);""", Map.of(
                "r", """
                 src | dst | weight
                -----+-----+--------
                   0 |   1 |      1
                   0 |   2 |      1
                   0 |   3 |      1
                   0 |   4 |      1
                   0 |   5 |      1
                   0 |   6 |      1
                   0 |   7 |      1
                   0 |   8 |      1
                   1 |   2 |      1
                   1 |   3 |      1
                   1 |   4 |      1
                   1 |   5 |      1
                   1 |   6 |      1
                   1 |   7 |      1
                   1 |   8 |      1
                   2 |   3 |      1
                   2 |   4 |      1
                   2 |   5 |      1
                   2 |   6 |      1
                   2 |   7 |      1
                   2 |   8 |      1
                   3 |   4 |      1
                   3 |   5 |      1
                   3 |   6 |      1
                   3 |   7 |      1
                   3 |   8 |      1
                   4 |   5 |      1
                   4 |   6 |      1
                   4 |   7 |      1
                   4 |   8 |      1
                   5 |   6 |      1
                   5 |   7 |      1
                   5 |   8 |      1
                   6 |   7 |      1
                   6 |   8 |      1
                   7 |   8 |      1""",
                "s", """
                 src | dst | weight
                -----+-----+--------
                   0 |   1 |      1
                   0 |   2 |      1
                   0 |   3 |      1
                   0 |   4 |      1
                   0 |   5 |      1
                   0 |   6 |      1
                   0 |   7 |      1
                   0 |   8 |      1
                   1 |   2 |      1
                   1 |   3 |      1
                   1 |   4 |      1
                   1 |   5 |      1
                   1 |   6 |      1
                   1 |   7 |      1
                   1 |   8 |      1
                   2 |   3 |      1
                   2 |   4 |      1
                   2 |   5 |      1
                   2 |   6 |      1
                   2 |   7 |      1
                   2 |   8 |      1
                   3 |   4 |      1
                   3 |   5 |      1
                   3 |   6 |      1
                   3 |   7 |      1
                   3 |   8 |      1
                   4 |   5 |      1
                   4 |   6 |      1
                   4 |   7 |      1
                   4 |   8 |      1
                   5 |   6 |      1
                   5 |   7 |      1
                   5 |   8 |      1
                   6 |   7 |      1
                   6 |   8 |      1
                   7 |   8 |      1"""));
        // A new edge at the start of e.
        ccs.stepMultiView("""
                INSERT INTO e VALUES(9, 0);""", Map.of(
                "r", """
                 src | dst | weight
                -----+-----+--------
                   9 |   0 |      1
                   9 |   1 |      1
                   9 |   2 |      1
                   9 |   3 |      1
                   9 |   4 |      1
                   9 |   5 |      1
                   9 |   6 |      1
                   9 |   7 |      1
                   9 |   8 |      1""",
                "s", """
                 src | dst | weight
                -----+-----+--------"""));
        // Cutting both chains exposes lost derivations.
        ccs.stepMultiView("""
                REMOVE FROM e VALUES(4, 5);
                REMOVE FROM f VALUES(4, 5);""", Map.of(
                "r", """
                 src | dst | weight
                -----+-----+--------
                   0 |   5 |     -1
                   0 |   6 |     -1
                   0 |   7 |     -1
                   0 |   8 |     -1
                   1 |   5 |     -1
                   1 |   6 |     -1
                   1 |   7 |     -1
                   1 |   8 |     -1
                   2 |   5 |     -1
                   2 |   6 |     -1
                   2 |   7 |     -1
                   2 |   8 |     -1
                   3 |   5 |     -1
                   3 |   6 |     -1
                   3 |   7 |     -1
                   3 |   8 |     -1
                   4 |   5 |     -1
                   4 |   6 |     -1
                   4 |   7 |     -1
                   4 |   8 |     -1
                   9 |   5 |     -1
                   9 |   6 |     -1
                   9 |   7 |     -1
                   9 |   8 |     -1""",
                "s", """
                 src | dst | weight
                -----+-----+--------
                   0 |   5 |     -1
                   0 |   6 |     -1
                   0 |   7 |     -1
                   0 |   8 |     -1
                   1 |   5 |     -1
                   1 |   6 |     -1
                   1 |   7 |     -1
                   1 |   8 |     -1
                   2 |   5 |     -1
                   2 |   6 |     -1
                   2 |   7 |     -1
                   2 |   8 |     -1
                   3 |   5 |     -1
                   3 |   6 |     -1
                   3 |   7 |     -1
                   3 |   8 |     -1
                   4 |   5 |     -1
                   4 |   6 |     -1
                   4 |   7 |     -1
                   4 |   8 |     -1"""));
        ccs.stepMultiView("""
                INSERT INTO e VALUES(4, 5);
                INSERT INTO f VALUES(4, 5);""", Map.of(
                "r", """
                 src | dst | weight
                -----+-----+--------
                   0 |   5 |      1
                   0 |   6 |      1
                   0 |   7 |      1
                   0 |   8 |      1
                   1 |   5 |      1
                   1 |   6 |      1
                   1 |   7 |      1
                   1 |   8 |      1
                   2 |   5 |      1
                   2 |   6 |      1
                   2 |   7 |      1
                   2 |   8 |      1
                   3 |   5 |      1
                   3 |   6 |      1
                   3 |   7 |      1
                   3 |   8 |      1
                   4 |   5 |      1
                   4 |   6 |      1
                   4 |   7 |      1
                   4 |   8 |      1
                   9 |   5 |      1
                   9 |   6 |      1
                   9 |   7 |      1
                   9 |   8 |      1""",
                "s", """
                 src | dst | weight
                -----+-----+--------
                   0 |   5 |      1
                   0 |   6 |      1
                   0 |   7 |      1
                   0 |   8 |      1
                   1 |   5 |      1
                   1 |   6 |      1
                   1 |   7 |      1
                   1 |   8 |      1
                   2 |   5 |      1
                   2 |   6 |      1
                   2 |   7 |      1
                   2 |   8 |      1
                   3 |   5 |      1
                   3 |   6 |      1
                   3 |   7 |      1
                   3 |   8 |      1
                   4 |   5 |      1
                   4 |   6 |      1
                   4 |   7 |      1
                   4 |   8 |      1"""));
        // A new edge at the start of f, then a cut in e alone.
        ccs.stepMultiView("""
                INSERT INTO f VALUES(10, 9);""", Map.of(
                "r", """
                 src | dst | weight
                -----+-----+--------""",
                "s", """
                 src | dst | weight
                -----+-----+--------
                  10 |   0 |      1
                  10 |   1 |      1
                  10 |   2 |      1
                  10 |   3 |      1
                  10 |   4 |      1
                  10 |   5 |      1
                  10 |   6 |      1
                  10 |   7 |      1
                  10 |   8 |      1
                  10 |   9 |      1"""));
        ccs.stepMultiView("""
                REMOVE FROM e VALUES(4, 5);""", Map.of(
                "r", """
                 src | dst | weight
                -----+-----+--------
                   4 |   5 |     -1
                   4 |   6 |     -1
                   4 |   7 |     -1
                   4 |   8 |     -1""",
                "s", """
                 src | dst | weight
                -----+-----+--------
                   3 |   5 |     -1
                   3 |   6 |     -1
                   3 |   7 |     -1
                   3 |   8 |     -1"""));
    }

    /** Evaluates expression trees that read each other's totals, joining each
     * node's definition with one child's value at a time.
     *
     * <p>In the rules below, {@code t} is a tree and {@code n} a node of it:
     * <ul>
     * <li>{@code leaves(t, n, v)}: {@code n} is a leaf with value {@code v}, which
     * may be NULL.</li>
     * <li>{@code inner_nodes(t, n, is_add, lhs, rhs)}: {@code n} adds the values of
     * nodes {@code lhs} and {@code rhs} if {@code is_add}, and multiplies them
     * otherwise.</li>
     * <li>{@code refs(t, n, target)}: {@code n} reads the total of tree
     * {@code target}, which is the value of its root, with NULL read as 0.</li>
     * <li>{@code roots(t, n)}: {@code n} is the root of {@code t}.</li>
     * </ul>
     * <pre>
     * node_values(t, n, v) :- leaves(t, n, v).
     * node_values(t, n, apply(is_add, l, r)) :-
     *     inner_nodes(t, n, is_add, lhs, rhs),
     *     node_values(t, lhs, l),
     *     node_values(t, rhs, r).
     * node_values(t, n, total) :- refs(t, n, target), totals(target, total).
     * totals(t, coalesce(v, 0)) :- roots(t, root), node_values(t, root, v).
     * </pre>
     * {@code apply} adds or multiplies, and is NULL if either operand is.
     *
     * <p>Port of {@code TreeEvaluation(Plan::ByChild)} in {@code tree.rs}. */
    @Test
    public void treeEvaluationByChild() {
        String sql = """
                CREATE TABLE leaves(tree INT NOT NULL, node INT NOT NULL, val BIGINT);
                CREATE TABLE inner_nodes(tree INT NOT NULL, node INT NOT NULL,
                    is_add BOOLEAN NOT NULL, lhs INT NOT NULL, rhs INT NOT NULL);
                CREATE TABLE refs(tree INT NOT NULL, node INT NOT NULL, target INT NOT NULL);
                CREATE TABLE roots(tree INT NOT NULL, node INT NOT NULL);
                DECLARE RECURSIVE VIEW node_values(tree INT NOT NULL, node INT NOT NULL, val BIGINT);
                CREATE LOCAL VIEW totals AS
                SELECT r.tree, COALESCE(v.val, 0) AS total
                FROM roots r JOIN node_values v ON v.tree = r.tree AND v.node = r.node;
                CREATE VIEW node_values AS
                SELECT tree, node, val FROM leaves
                UNION
                SELECT i.tree, i.node, CASE WHEN i.is_add THEN l.val + r.val ELSE l.val * r.val END
                FROM node_values l JOIN node_values r ON l.tree = r.tree
                JOIN inner_nodes i ON i.tree = l.tree AND i.lhs = l.node AND i.rhs = r.node
                UNION
                SELECT f.tree, f.node, t.total FROM refs f JOIN totals t ON t.tree = f.target;""";
        var ccs = this.getCCS(sql);
        // Tree 3, `x * 5 + 0 * 1 * 2`, with `x` NULL.
        ccs.step("""
                INSERT INTO leaves VALUES(3, 0, NULL), (3, 1, 5), (3, 10, 0), (3, 11, 1),
                    (3, 13, 2);
                INSERT INTO inner_nodes VALUES(3, 2, false, 0, 1), (3, 12, false, 10, 11),
                    (3, 14, false, 12, 13), (3, 99, true, 2, 14);
                INSERT INTO roots VALUES(3, 99);""", """
                 tree | node | val  | weight
                ------+------+------+--------
                    3 |    0 | NULL |      1
                    3 |    1 |    5 |      1
                    3 |    2 | NULL |      1
                    3 |   10 |    0 |      1
                    3 |   11 |    1 |      1
                    3 |   12 |    0 |      1
                    3 |   13 |    2 |      1
                    3 |   14 |    0 |      1
                    3 |   99 | NULL |      1""");
        // Setting `x` changes values in the first iterations, where they meet
        // values that the previous step derived in later ones.
        ccs.step("""
                REMOVE FROM leaves VALUES(3, 0, NULL);
                INSERT INTO leaves VALUES(3, 0, 10);""", """
                 tree | node | val  | weight
                ------+------+------+--------
                    3 |    0 | NULL |     -1
                    3 |    0 |   10 |      1
                    3 |    2 | NULL |     -1
                    3 |    2 |   50 |      1
                    3 |   99 | NULL |     -1
                    3 |   99 |   50 |      1""");
        // Tree 1, `x * 1` with `x` NULL, and tree 2, `ref(1) + 0`.
        ccs.step("""
                INSERT INTO leaves VALUES(1, 0, NULL), (1, 1, 1), (2, 1, 0);
                INSERT INTO inner_nodes VALUES(1, 2, false, 0, 1), (2, 2, true, 0, 1);
                INSERT INTO refs VALUES(2, 0, 1);
                INSERT INTO roots VALUES(1, 2), (2, 2);""", """
                 tree | node | val  | weight
                ------+------+------+--------
                    1 |    0 | NULL |      1
                    1 |    1 |    1 |      1
                    1 |    2 | NULL |      1
                    2 |    0 |    0 |      1
                    2 |    1 |    0 |      1
                    2 |    2 |    0 |      1""");
        // Setting `x` of tree 1 changes the total that tree 2 reads.
        ccs.step("""
                REMOVE FROM leaves VALUES(1, 0, NULL);
                INSERT INTO leaves VALUES(1, 0, 4);""", """
                 tree | node | val  | weight
                ------+------+------+--------
                    1 |    0 | NULL |     -1
                    1 |    0 |    4 |      1
                    1 |    2 | NULL |     -1
                    1 |    2 |    4 |      1
                    2 |    0 |    0 |     -1
                    2 |    0 |    4 |      1
                    2 |    2 |    0 |     -1
                    2 |    2 |    4 |      1""");
        // Tree 0, whose nodes `a + d` and `c + d` share the deep child `d`.
        ccs.step("""
                INSERT INTO leaves VALUES(0, 0, NULL), (0, 1, NULL), (0, 2, 7), (0, 3, 1);
                INSERT INTO inner_nodes VALUES(0, 4, false, 0, 3), (0, 5, false, 1, 3),
                    (0, 6, false, 5, 3), (0, 7, false, 2, 3), (0, 8, false, 7, 3),
                    (0, 9, false, 8, 3), (0, 10, false, 9, 3), (0, 11, false, 10, 3),
                    (0, 12, true, 4, 11), (0, 13, true, 6, 11), (0, 14, true, 12, 13);
                INSERT INTO roots VALUES(0, 14);""", """
                 tree | node | val  | weight
                ------+------+------+--------
                    0 |    0 | NULL |      1
                    0 |    1 | NULL |      1
                    0 |    2 |    7 |      1
                    0 |    3 |    1 |      1
                    0 |    4 | NULL |      1
                    0 |    5 | NULL |      1
                    0 |    6 | NULL |      1
                    0 |    7 |    7 |      1
                    0 |    8 |    7 |      1
                    0 |    9 |    7 |      1
                    0 |   10 |    7 |      1
                    0 |   11 |    7 |      1
                    0 |   12 | NULL |      1
                    0 |   13 | NULL |      1
                    0 |   14 | NULL |      1""");
        // Setting the leaves under `a` and `c` changes them in consecutive
        // iterations, and both meet `d`, derived in a later one.
        ccs.step("""
                REMOVE FROM leaves VALUES(0, 0, NULL), (0, 1, NULL);
                INSERT INTO leaves VALUES(0, 0, 2), (0, 1, 3);""", """
                 tree | node | val  | weight
                ------+------+------+--------
                    0 |    0 | NULL |     -1
                    0 |    0 |    2 |      1
                    0 |    1 | NULL |     -1
                    0 |    1 |    3 |      1
                    0 |    4 | NULL |     -1
                    0 |    4 |    2 |      1
                    0 |    5 | NULL |     -1
                    0 |    5 |    3 |      1
                    0 |    6 | NULL |     -1
                    0 |    6 |    3 |      1
                    0 |   12 | NULL |     -1
                    0 |   12 |    9 |      1
                    0 |   13 | NULL |     -1
                    0 |   13 |   10 |      1
                    0 |   14 | NULL |     -1
                    0 |   14 |   19 |      1""");
        // Removing the root of tree 1 retracts the total that tree 2 reads.
        ccs.step("""
                REMOVE FROM roots VALUES(1, 2);""", """
                 tree | node | val | weight
                ------+------+-----+--------
                    2 |    0 |   4 |     -1
                    2 |    2 |   4 |     -1""");
    }

    /** A points-to analysis with fields, whose store and load rules join two
     * relations of the recursion.
     *
     * <p>In the rules below:
     * <ul>
     * <li>{@code allocs(var, obj)}: the statement {@code var = new obj}.</li>
     * <li>{@code assigns(dst, src)}: the statement {@code dst = src}.</li>
     * <li>{@code stores(base, field, src)}: the statement {@code base.field = src}.</li>
     * <li>{@code loads(dst, base, field)}: the statement {@code dst = base.field}.</li>
     * <li>{@code points_to(var, obj)}: variable {@code var} may point to object
     * {@code obj}.</li>
     * <li>{@code field_points_to(base_obj, field, obj)}: field {@code field} of
     * object {@code base_obj} may point to object {@code obj}.</li>
     * </ul>
     * <pre>
     * points_to(var, obj) :- allocs(var, obj).
     * points_to(dst, obj) :- assigns(dst, src), points_to(src, obj).
     * field_points_to(base_obj, field, obj) :-
     *     stores(base, field, src),
     *     points_to(base, base_obj),
     *     points_to(src, obj).
     * points_to(dst, obj) :-
     *     loads(dst, base, field),
     *     points_to(base, base_obj),
     *     field_points_to(base_obj, field, obj).
     * </pre>
     * Port of {@code PointsToAnalysis} in {@code points_to.rs}. */
    @Test
    public void pointsToAnalysis() {
        String sql = """
                CREATE TABLE allocs(var INT NOT NULL, obj INT NOT NULL);
                CREATE TABLE assigns(dst INT NOT NULL, src INT NOT NULL);
                CREATE TABLE stores(base INT NOT NULL, field INT NOT NULL, src INT NOT NULL);
                CREATE TABLE loads(dst INT NOT NULL, base INT NOT NULL, field INT NOT NULL);
                DECLARE RECURSIVE VIEW points_to(var INT NOT NULL, obj INT NOT NULL);
                CREATE LOCAL VIEW field_points_to AS
                SELECT b.obj AS base_obj, s.field, f.obj
                FROM stores s JOIN points_to b ON b.var = s.base JOIN points_to f ON f.var = s.src;
                CREATE VIEW points_to AS
                SELECT var, obj FROM allocs
                UNION
                SELECT a.dst, p.obj FROM assigns a JOIN points_to p ON p.var = a.src
                UNION
                SELECT l.dst, f.obj
                FROM loads l JOIN points_to b ON b.var = l.base
                JOIN field_points_to f ON f.base_obj = b.obj AND f.field = l.field;""";
        var ccs = this.getCCS(sql);
        // Object 100 flows along the chain 0 -> 1 -> ... -> 8; stores and
        // loads through variable 8 use fields 0 and 1.
        ccs.step("""
                INSERT INTO allocs VALUES(0, 100);
                INSERT INTO assigns VALUES(1, 0), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5),
                    (7, 6), (8, 7);
                INSERT INTO stores VALUES(8, 0, 20), (8, 1, 22);
                INSERT INTO loads VALUES(30, 8, 0), (31, 8, 1);""", """
                 var | obj | weight
                -----+-----+--------
                   0 | 100 |      1
                   1 | 100 |      1
                   2 | 100 |      1
                   3 | 100 |      1
                   4 | 100 |      1
                   5 | 100 |      1
                   6 | 100 |      1
                   7 | 100 |      1
                   8 | 100 |      1""");
        // Variable 20 points to a new object in each of two consecutive
        // iterations, and both times meets the store through variable 8.
        ccs.step("""
                INSERT INTO allocs VALUES(20, 200), (19, 201);
                INSERT INTO assigns VALUES(20, 19);""", """
                 var | obj | weight
                -----+-----+--------
                  19 | 201 |      1
                  20 | 200 |      1
                  20 | 201 |      1
                  30 | 200 |      1
                  30 | 201 |      1""");
        ccs.step("""
                INSERT INTO allocs VALUES(22, 202), (21, 203);
                INSERT INTO assigns VALUES(22, 21);""", """
                 var | obj | weight
                -----+-----+--------
                  21 | 203 |      1
                  22 | 202 |      1
                  22 | 203 |      1
                  31 | 202 |      1
                  31 | 203 |      1""");
        // Retracting the trigger, cutting the chain, and restoring both.
        ccs.step("""
                REMOVE FROM allocs VALUES(20, 200), (19, 201);
                REMOVE FROM assigns VALUES(20, 19);""", """
                 var | obj | weight
                -----+-----+--------
                  19 | 201 |     -1
                  20 | 200 |     -1
                  20 | 201 |     -1
                  30 | 200 |     -1
                  30 | 201 |     -1""");
        ccs.step("""
                REMOVE FROM assigns VALUES(4, 3);""", """
                 var | obj | weight
                -----+-----+--------
                   4 | 100 |     -1
                   5 | 100 |     -1
                   6 | 100 |     -1
                   7 | 100 |     -1
                   8 | 100 |     -1
                  31 | 202 |     -1
                  31 | 203 |     -1""");
        ccs.step("""
                INSERT INTO allocs VALUES(20, 200), (19, 201);
                INSERT INTO assigns VALUES(20, 19);""", """
                 var | obj | weight
                -----+-----+--------
                  19 | 201 |      1
                  20 | 200 |      1
                  20 | 201 |      1""");
        ccs.step("""
                INSERT INTO assigns VALUES(4, 3);""", """
                 var | obj | weight
                -----+-----+--------
                   4 | 100 |      1
                   5 | 100 |      1
                   6 | 100 |      1
                   7 | 100 |      1
                   8 | 100 |      1
                  30 | 200 |      1
                  30 | 201 |      1
                  31 | 202 |      1
                  31 | 203 |      1""");
    }

    /** A bill of materials whose MIN, MAX, and SUM over a recursive view the
     * compiler combines with a star join inside the recursion.
     *
     * <p>In the rules below:
     * <ul>
     * <li>{@code leaves(part, duration)}: leaf part {@code part} takes
     * {@code duration} to make.</li>
     * <li>{@code part_of(part, assembly)}: {@code part} is one of the parts of
     * {@code assembly}.</li>
     * <li>{@code stats(part, first_ready, finished, leaf_parts)}: the first leaf
     * part under {@code part} is ready at {@code first_ready}, {@code part} is
     * finished at {@code finished}, and it takes {@code leaf_parts} leaf
     * parts.</li>
     * </ul>
     * <pre>
     * stats(part, d, d, 1) :- leaves(part, d).
     * stats(assembly, min(first), max(finished) + 1, sum(leaf_parts)) :-
     *     part_of(part, assembly),
     *     stats(part, first, finished, leaf_parts).
     * </pre>
     * The second rule groups by {@code assembly}, and its aggregates range over
     * every match of its body.
     *
     * <p>Port of {@code BillOfMaterials} in {@code bill_of_materials.rs}, with a
     * MIN column added. */
    @Test
    public void billOfMaterials() {
        String sql = """
                CREATE TABLE part_of(part INT NOT NULL, assembly INT NOT NULL);
                CREATE TABLE leaves(part INT NOT NULL, duration INT NOT NULL);
                DECLARE RECURSIVE VIEW stats(part INT NOT NULL, first_ready INT NOT NULL,
                    finished INT NOT NULL, leaf_parts BIGINT NOT NULL);
                CREATE VIEW stats AS
                SELECT part, duration AS first_ready, duration AS finished, CAST(1 AS BIGINT) AS leaf_parts
                FROM leaves
                UNION
                SELECT p.assembly, MIN(s.first_ready), MAX(s.finished) + 1, SUM(s.leaf_parts)
                FROM part_of p JOIN stats s ON s.part = p.part
                GROUP BY p.assembly;""";
        var ccs = this.getCCS(sql);
        // Assembly 20 consists of leaf part 1, assembly 21 (made of leaf part
        // 2), and the last assembly of the chain 10 -> 11 -> ... -> 17.
        // Its aggregates change in iterations 1, 2, and 8.
        ccs.step("""
                INSERT INTO part_of VALUES(1, 20), (21, 20), (17, 20), (2, 21), (10, 11),
                    (11, 12), (12, 13), (13, 14), (14, 15), (15, 16), (16, 17);
                INSERT INTO leaves VALUES(1, 5), (2, 5), (10, 1);""", """
                 part | first_ready | finished | leaf_parts | weight
                ------+-------------+----------+------------+--------
                    1 |           5 |        5 |          1 |      1
                    2 |           5 |        5 |          1 |      1
                   10 |           1 |        1 |          1 |      1
                   11 |           1 |        2 |          1 |      1
                   12 |           1 |        3 |          1 |      1
                   13 |           1 |        4 |          1 |      1
                   14 |           1 |        5 |          1 |      1
                   15 |           1 |        6 |          1 |      1
                   16 |           1 |        7 |          1 |      1
                   17 |           1 |        8 |          1 |      1
                   20 |           1 |        9 |          3 |      1
                   21 |           5 |        6 |          1 |      1""");
        // Retiming leaf parts 1 and 2 changes the aggregates of assembly 20 in
        // iterations 1 and 2, where they meet the ones of iteration 8.
        ccs.step("""
                REMOVE FROM leaves VALUES(1, 5), (2, 5);
                INSERT INTO leaves VALUES(1, 6), (2, 9);""", """
                 part | first_ready | finished | leaf_parts | weight
                ------+-------------+----------+------------+--------
                    1 |           5 |        5 |          1 |     -1
                    1 |           6 |        6 |          1 |      1
                    2 |           5 |        5 |          1 |     -1
                    2 |           9 |        9 |          1 |      1
                   20 |           1 |        9 |          3 |     -1
                   20 |           1 |       11 |          3 |      1
                   21 |           5 |        6 |          1 |     -1
                   21 |           9 |       10 |          1 |      1""");
        // Detaching the chain, restoring the times, and reattaching it.
        ccs.step("""
                REMOVE FROM part_of VALUES(17, 20);""", """
                 part | first_ready | finished | leaf_parts | weight
                ------+-------------+----------+------------+--------
                   20 |           1 |       11 |          3 |     -1
                   20 |           6 |       11 |          2 |      1""");
        ccs.step("""
                REMOVE FROM leaves VALUES(1, 6), (2, 9);
                INSERT INTO leaves VALUES(1, 5), (2, 5);""", """
                 part | first_ready | finished | leaf_parts | weight
                ------+-------------+----------+------------+--------
                    1 |           5 |        5 |          1 |      1
                    1 |           6 |        6 |          1 |     -1
                    2 |           5 |        5 |          1 |      1
                    2 |           9 |        9 |          1 |     -1
                   20 |           5 |        7 |          2 |      1
                   20 |           6 |       11 |          2 |     -1
                   21 |           5 |        6 |          1 |      1
                   21 |           9 |       10 |          1 |     -1""");
        ccs.step("""
                INSERT INTO part_of VALUES(17, 20);""", """
                 part | first_ready | finished | leaf_parts | weight
                ------+-------------+----------+------------+--------
                   20 |           1 |        9 |          3 |      1
                   20 |           5 |        7 |          2 |     -1""");
    }

    /** Trips over road and rail legs, with a full outer join of two relations
     * of the recursion.
     *
     * <p>In the rules below:
     * <ul>
     * <li>{@code road(x, y)}: a road leg from {@code x} to {@code y}.</li>
     * <li>{@code rail(x, y)}: a rail leg from {@code x} to {@code y}.</li>
     * <li>{@code by_road(x, y)}: a trip from {@code x} to {@code y} whose last leg
     * is by road.</li>
     * <li>{@code by_rail(x, y)}: a trip from {@code x} to {@code y} whose last leg
     * is by rail.</li>
     * <li>{@code trips(x, y, by_road, by_rail)}: a trip from {@code x} to
     * {@code y}, where {@code by_road} and {@code by_rail} tell whether its last
     * leg can be by road and whether it can be by rail.</li>
     * </ul>
     * <pre>
     * by_road(x, y)            :- road(x, y).
     * by_road(x, z)            :- trips(x, y, _, _), road(y, z).
     * by_rail(x, y)            :- rail(x, y).
     * by_rail(x, z)            :- trips(x, y, _, _), rail(y, z).
     * trips(x, y, true, true)  :- by_road(x, y), by_rail(x, y).
     * trips(x, y, true, false) :- by_road(x, y), !by_rail(x, y).
     * trips(x, y, false, true) :- by_rail(x, y), !by_road(x, y).
     * </pre>
     * The last three rules are the full outer join.
     *
     * <p>Port of {@code MultimodalTrips} in {@code trips.rs}. */
    @Test
    public void multimodalTrips() {
        String sql = """
                CREATE TABLE road(src INT NOT NULL, dst INT NOT NULL);
                CREATE TABLE rail(src INT NOT NULL, dst INT NOT NULL);
                DECLARE RECURSIVE VIEW trips(src INT, dst INT, by_road BOOLEAN NOT NULL, by_rail BOOLEAN NOT NULL);
                CREATE LOCAL VIEW by_road AS
                SELECT src, dst FROM road
                UNION
                SELECT t.src, l.dst FROM trips t JOIN road l ON l.src = t.dst;
                CREATE LOCAL VIEW by_rail AS
                SELECT src, dst FROM rail
                UNION
                SELECT t.src, l.dst FROM trips t JOIN rail l ON l.src = t.dst;
                CREATE VIEW trips AS
                SELECT COALESCE(a.src, b.src) AS src, COALESCE(a.dst, b.dst) AS dst,
                    a.src IS NOT NULL AS by_road, b.src IS NOT NULL AS by_rail
                FROM by_road a FULL OUTER JOIN by_rail b ON a.src = b.src AND a.dst = b.dst;""";
        var ccs = this.getCCS(sql);
        // A rail line 0 -> ... -> 8 and a road 0 -> 11 -> ... -> 17 -> 8 of
        // 8 legs each.
        ccs.step("""
                INSERT INTO rail VALUES(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7),
                    (7, 8);
                INSERT INTO road VALUES(0, 11), (11, 12), (12, 13), (13, 14), (14, 15),
                    (15, 16), (16, 17), (17, 8);""", """
                 src | dst | by_road | by_rail | weight
                -----+-----+---------+---------+--------
                   0 |   1 |   false |    true |      1
                   0 |   2 |   false |    true |      1
                   0 |   3 |   false |    true |      1
                   0 |   4 |   false |    true |      1
                   0 |   5 |   false |    true |      1
                   0 |   6 |   false |    true |      1
                   0 |   7 |   false |    true |      1
                   0 |   8 |    true |    true |      1
                   0 |  11 |    true |   false |      1
                   0 |  12 |    true |   false |      1
                   0 |  13 |    true |   false |      1
                   0 |  14 |    true |   false |      1
                   0 |  15 |    true |   false |      1
                   0 |  16 |    true |   false |      1
                   0 |  17 |    true |   false |      1
                   1 |   2 |   false |    true |      1
                   1 |   3 |   false |    true |      1
                   1 |   4 |   false |    true |      1
                   1 |   5 |   false |    true |      1
                   1 |   6 |   false |    true |      1
                   1 |   7 |   false |    true |      1
                   1 |   8 |   false |    true |      1
                   2 |   3 |   false |    true |      1
                   2 |   4 |   false |    true |      1
                   2 |   5 |   false |    true |      1
                   2 |   6 |   false |    true |      1
                   2 |   7 |   false |    true |      1
                   2 |   8 |   false |    true |      1
                   3 |   4 |   false |    true |      1
                   3 |   5 |   false |    true |      1
                   3 |   6 |   false |    true |      1
                   3 |   7 |   false |    true |      1
                   3 |   8 |   false |    true |      1
                   4 |   5 |   false |    true |      1
                   4 |   6 |   false |    true |      1
                   4 |   7 |   false |    true |      1
                   4 |   8 |   false |    true |      1
                   5 |   6 |   false |    true |      1
                   5 |   7 |   false |    true |      1
                   5 |   8 |   false |    true |      1
                   6 |   7 |   false |    true |      1
                   6 |   8 |   false |    true |      1
                   7 |   8 |   false |    true |      1
                  11 |   8 |    true |   false |      1
                  11 |  12 |    true |   false |      1
                  11 |  13 |    true |   false |      1
                  11 |  14 |    true |   false |      1
                  11 |  15 |    true |   false |      1
                  11 |  16 |    true |   false |      1
                  11 |  17 |    true |   false |      1
                  12 |   8 |    true |   false |      1
                  12 |  13 |    true |   false |      1
                  12 |  14 |    true |   false |      1
                  12 |  15 |    true |   false |      1
                  12 |  16 |    true |   false |      1
                  12 |  17 |    true |   false |      1
                  13 |   8 |    true |   false |      1
                  13 |  14 |    true |   false |      1
                  13 |  15 |    true |   false |      1
                  13 |  16 |    true |   false |      1
                  13 |  17 |    true |   false |      1
                  14 |   8 |    true |   false |      1
                  14 |  15 |    true |   false |      1
                  14 |  16 |    true |   false |      1
                  14 |  17 |    true |   false |      1
                  15 |   8 |    true |   false |      1
                  15 |  16 |    true |   false |      1
                  15 |  17 |    true |   false |      1
                  16 |   8 |    true |   false |      1
                  16 |  17 |    true |   false |      1
                  17 |   8 |    true |   false |      1""");
        // A direct road leg from 0 to 8; deleting the last rail leg then
        // exposes lost updates.
        ccs.step("""
                INSERT INTO road VALUES(0, 8);""", """
                 src | dst | by_road | by_rail | weight
                -----+-----+---------+---------+--------""");
        ccs.step("""
                REMOVE FROM rail VALUES(7, 8);""", """
                 src | dst | by_road | by_rail | weight
                -----+-----+---------+---------+--------
                   0 |   8 |    true |   false |      1
                   0 |   8 |    true |    true |     -1
                   1 |   8 |   false |    true |     -1
                   2 |   8 |   false |    true |     -1
                   3 |   8 |   false |    true |     -1
                   4 |   8 |   false |    true |     -1
                   5 |   8 |   false |    true |     -1
                   6 |   8 |   false |    true |     -1
                   7 |   8 |   false |    true |     -1""");
        ccs.step("""
                INSERT INTO rail VALUES(7, 8);""", """
                 src | dst | by_road | by_rail | weight
                -----+-----+---------+---------+--------
                   0 |   8 |    true |   false |     -1
                   0 |   8 |    true |    true |      1
                   1 |   8 |   false |    true |      1
                   2 |   8 |   false |    true |      1
                   3 |   8 |   false |    true |      1
                   4 |   8 |   false |    true |      1
                   5 |   8 |   false |    true |      1
                   6 |   8 |   false |    true |      1
                   7 |   8 |   false |    true |      1""");
        // A direct rail leg, then every road derivation of 0 -> 8 deleted.
        ccs.step("""
                INSERT INTO rail VALUES(0, 8);""", """
                 src | dst | by_road | by_rail | weight
                -----+-----+---------+---------+--------""");
        ccs.step("""
                REMOVE FROM road VALUES(0, 8), (17, 8);""", """
                 src | dst | by_road | by_rail | weight
                -----+-----+---------+---------+--------
                   0 |   8 |   false |    true |      1
                   0 |   8 |    true |    true |     -1
                  11 |   8 |    true |   false |     -1
                  12 |   8 |    true |   false |     -1
                  13 |   8 |    true |   false |     -1
                  14 |   8 |    true |   false |     -1
                  15 |   8 |    true |   false |     -1
                  16 |   8 |    true |   false |     -1
                  17 |   8 |    true |   false |     -1""");
    }
}
