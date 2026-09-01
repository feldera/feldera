package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape.Column;
import org.dbsp.sqlCompiler.ir.type.CollectionShape.Part;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.CollectionShape.Column.index;
import static org.dbsp.sqlCompiler.ir.type.CollectionShape.Column.none;
import static org.dbsp.sqlCompiler.ir.type.CollectionShape.Column.value;

/** Tests for {@link ColumnEquivalence}. */
public class ColumnEquivalenceTests {
    /** of([i0, v0]) puts both columns in one set: they hold the same data in every row.
     * A column in no group, here v1, gets a set holding only itself. */
    @Test
    public void groupedColumnsGetTheSameSet() {
        ColumnEquivalence equivalence = ColumnEquivalence.of(List.of(List.of(index(0), value(0))));
        EquivalentColumnSet expected = EquivalentColumnSet.of(index(0), value(0));
        Assert.assertEquals(expected, equivalence.setOf(index(0)));
        Assert.assertEquals(expected, equivalence.setOf(value(0)));
        Assert.assertEquals(EquivalentColumnSet.of(value(1)), equivalence.setOf(value(1)));
    }

    /** of([i0, v0], [v0, v3]) yields the single set {i0, v0, v3}: v0 is in both groups,
     * so all three columns hold the same data. */
    @Test
    public void groupsSharingAColumnBecomeOneSet() {
        ColumnEquivalence equivalence = ColumnEquivalence.of(List.of(
                List.of(index(0), value(0)), List.of(value(0), value(3))));
        EquivalentColumnSet expected = EquivalentColumnSet.of(index(0), value(0), value(3));
        Assert.assertEquals(expected, equivalence.setOf(index(0)));
        Assert.assertEquals(expected, equivalence.setOf(value(3)));
    }

    /** of([i0]) stores nothing: a group of a single column states no equality. */
    @Test
    public void groupOfOneColumnIsIgnored() {
        Assert.assertTrue(ColumnEquivalence.of(List.of(List.of(index(0)))).isEmpty());
    }

    /** An operator that keeps index column i0 and copies value column v0 to v2 turns the
     * set {i0, v0} into {i0, v2}. */
    @Test
    public void afterRewritesColumnsThroughTheTransform() {
        ColumnEquivalence input = ColumnEquivalence.of(List.of(List.of(index(0), value(0))));
        ColumnCopyTransform transform = column -> switch (column.part()) {
            case INDEX -> List.of(column);
            case VALUE -> column.field() == 0 ? List.of(value(2)) : List.of();
            case NONE -> List.of();
        };
        Assert.assertEquals(EquivalentColumnSet.of(index(0), value(2)),
                input.after(transform).setOf(index(0)));
    }

    /** An operator that keeps index column i0 and drops the whole value tuple leaves the
     * set {i0, v0} with i0 alone, which states no equality, so the result is empty. */
    @Test
    public void afterForgetsASetReducedToOneColumn() {
        ColumnEquivalence input = ColumnEquivalence.of(List.of(List.of(index(0), value(0))));
        ColumnCopyTransform keepsIndex =
                column -> column.part() == Part.INDEX ? List.of(column) : List.of();
        Assert.assertTrue(input.after(keepsIndex).isEmpty());
    }

    /** Merging the set {0, 1} with the set {1, 2} yields {0, 1, 2}.  A join does this:
     * both of its inputs reach the output column holding the join key, so every column
     * they name there holds the same data. */
    @Test
    public void mergeUnitesSetsThatShareAColumn() {
        ColumnEquivalence left = ColumnEquivalence.of(List.of(List.of(none(0), none(1))));
        ColumnEquivalence right = ColumnEquivalence.of(List.of(List.of(none(1), none(2))));
        Assert.assertEquals(EquivalentColumnSet.of(none(0), none(1), none(2)),
                left.merge(right).setOf(none(2)));
    }

    /** Only equalities stated by both partitions survive: {0, 1, 2} against {1, 2, 3} keeps
     * {1, 2}, and a pair the two disagree about keeps nothing. */
    @Test
    public void intersectKeepsOnlyWhatBothState() {
        ColumnEquivalence wide = ColumnEquivalence.of(List.of(List.of(none(0), none(1), none(2))));
        ColumnEquivalence other = ColumnEquivalence.of(List.of(List.of(none(1), none(2), none(3))));
        Assert.assertEquals(EquivalentColumnSet.of(none(1), none(2)), wide.intersect(other).setOf(none(1)));
        Assert.assertEquals(EquivalentColumnSet.of(none(0)), wide.intersect(other).setOf(none(0)));
        ColumnEquivalence disjoint = ColumnEquivalence.of(List.of(List.of(none(3), none(4))));
        Assert.assertTrue(wide.intersect(disjoint).isEmpty());
    }

    /** A partition stating no equality intersects to nothing. */
    @Test
    public void intersectWithAnEmptyPartitionKeepsNothing() {
        ColumnEquivalence known = ColumnEquivalence.of(List.of(List.of(index(0), value(0))));
        Assert.assertTrue(known.intersect(ColumnEquivalence.NONE).isEmpty());
        Assert.assertTrue(ColumnEquivalence.NONE.intersect(known).isEmpty());
    }

    /** Merging with a partition that states no equality returns the same sets, either way round. */
    @Test
    public void mergeWithAnEmptyPartitionChangesNothing() {
        ColumnEquivalence known = ColumnEquivalence.of(List.of(List.of(index(0), value(0))));
        EquivalentColumnSet expected = EquivalentColumnSet.of(index(0), value(0));
        Assert.assertEquals(expected, known.merge(ColumnEquivalence.NONE).setOf(index(0)));
        Assert.assertEquals(expected, ColumnEquivalence.NONE.merge(known).setOf(value(0)));
    }

    /** Assert that every column of {@code expected} is named by exactly that set. */
    static void assertOneSet(EquivalentColumnSet expected, ColumnEquivalence equivalence) {
        for (Column column : expected.columns())
            Assert.assertEquals(expected, equivalence.setOf(column));
    }

    /** The same equalities in any order describe the same partition: 0=1, 1=2 and 2=3 put
     * all four columns in one set whether the groups arrive in that order, reversed, or with
     * the group that bridges the other two arriving last. */
    @Test
    public void mergingDoesNotDependOnGroupOrder() {
        EquivalentColumnSet all = EquivalentColumnSet.of(none(0), none(1), none(2), none(3));
        assertOneSet(all, ColumnEquivalence.of(List.of(
                List.of(none(0), none(1)), List.of(none(1), none(2)), List.of(none(2), none(3)))));
        assertOneSet(all, ColumnEquivalence.of(List.of(
                List.of(none(2), none(3)), List.of(none(1), none(2)), List.of(none(0), none(1)))));
        assertOneSet(all, ColumnEquivalence.of(List.of(
                List.of(none(0), none(1)), List.of(none(2), none(3)), List.of(none(1), none(2)))));
    }

    /** A group repeated, and a group whose columns a larger group already covers, add nothing. */
    @Test
    public void repeatedAndCoveredGroupsAddNothing() {
        EquivalentColumnSet all = EquivalentColumnSet.of(none(0), none(1), none(2));
        assertOneSet(all, ColumnEquivalence.of(List.of(
                List.of(none(0), none(1), none(2)),
                List.of(none(0), none(1)),
                List.of(none(0), none(1), none(2)))));
    }

    /** Groups with no column in common describe different data and stay apart. */
    @Test
    public void groupsWithoutACommonColumnStayApart() {
        ColumnEquivalence equivalence = ColumnEquivalence.of(List.of(
                List.of(none(0), none(1)), List.of(none(2), none(3))));
        Assert.assertEquals(EquivalentColumnSet.of(none(0), none(1)), equivalence.setOf(none(1)));
        Assert.assertEquals(EquivalentColumnSet.of(none(2), none(3)), equivalence.setOf(none(3)));
    }

    /** Two partitions merged in either order describe the same equalities. */
    @Test
    public void mergeDoesNotDependOnTheOrderOfTheTwoPartitions() {
        ColumnEquivalence left = ColumnEquivalence.of(List.of(List.of(none(0), none(1))));
        ColumnEquivalence right = ColumnEquivalence.of(List.of(List.of(none(1), none(2))));
        EquivalentColumnSet all = EquivalentColumnSet.of(none(0), none(1), none(2));
        assertOneSet(all, left.merge(right));
        assertOneSet(all, right.merge(left));
    }
}
