package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.ir.type.CollectionShape;

import static org.dbsp.sqlCompiler.ir.type.CollectionShape.Column.index;
import static org.dbsp.sqlCompiler.ir.type.CollectionShape.Column.none;
import static org.dbsp.sqlCompiler.ir.type.CollectionShape.Column.value;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link KeyColumns}, and for the key sets {@link Keys} builds from them. */
public class KeyColumnsTests {
    /** The copies made by the map_index {@code |t: &Tup3| (Tup1::new(t.0), Tup3::new(t.0, t.1, t.2))}:
     * t.0 is copied twice, to i0 and v0; t.1 and t.2 once, to v1 and v2. */
    static final ColumnCopyTransform INDEX_BY_0 = column -> switch (column.field()) {
        case 0 -> List.of(index(0), value(0));
        case 1 -> List.of(value(1));
        case 2 -> List.of(value(2));
        default -> List.of();
    };

    /** A key naming columns of a Z-set, where no two columns hold the same data. */
    static KeyColumns key(CollectionShape.Column... columns) {
        return ColumnEquivalence.NONE.keyOf(List.of(columns));
    }

    /** Each value of the key is named by every column it reaches: t.0 reaches i0 and v0,
     * so one set names both. */
    @Test
    public void mapNamesAValueByEveryColumnItReaches() {
        // Key [0, 1]
        Assert.assertEquals("[i0=v0, v1]", key(none(0), none(1)).map(INDEX_BY_0).toString());
        // Key [1]
        Assert.assertEquals("[v1]", key(none(1)).map(INDEX_BY_0).toString());
        // Key []: a collection of at most one row stays one of at most one row
        Assert.assertEquals("[]", key().map(INDEX_BY_0).toString());
        // Key [0, 3]: column 3 reaches no output column, so the output has no key
        Assert.assertNull(key(none(0), none(3)).map(INDEX_BY_0));
    }

    /** A function copying every column twice used to yield one key per choice of copy,
     * 2^n of them.  Naming each value by both its copies gives a single key instead. */
    @Test
    public void mapKeepsOneKeyWhenEveryColumnIsCopiedTwice() {
        // The copies made by |t: &Tup3| (Tup3::new(t.0, t.1, t.2), Tup3::new(t.0, t.1, t.2)):
        ColumnCopyTransform twoCopies = column -> List.of(index(column.field()), value(column.field()));
        Assert.assertEquals("[i0=v0, i1=v1, i2=v2]",
                key(none(0), none(1), none(2)).map(twoCopies).toString());
    }

    /** {@link Keys#of} keeps the smallest keys when there are too many */
    @Test
    public void keySetIsBounded() {
        // 1000 two-column keys [i, i + 1000]; no two share a column, so none contains another
        List<KeyColumns> many = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
            many.add(key(none(i), none(i + 1000)));
        // One key on column 5000 alone, the smallest key of the set
        many.add(key(none(5000)));
        Keys keys = Keys.of(many);
        Assert.assertTrue(keys.keys.size() < many.size());
        // The smallest key must be kept, and sorts first
        Assert.assertEquals("[5000]", keys.keys.get(0).toString());
    }

    /** Each side has one narrow key and eight wide ones, so the 81 combinations range from
     * two columns to eight.  The bound keeps the smallest, not the ones built first: the
     * narrow key of one side combined with a wide key of the other is built late and must
     * still outlive the wide-with-wide combinations. */
    @Test
    public void combinationsKeepTheSmallestKeys() {
        List<KeyColumns> left = new ArrayList<>();
        List<KeyColumns> right = new ArrayList<>();
        left.add(key(none(0)));
        right.add(key(none(100)));
        for (int i = 1; i < 9; i++) {
            left.add(key(none(10 * i), none(10 * i + 1), none(10 * i + 2), none(10 * i + 3)));
            right.add(key(none(100 + 10 * i), none(101 + 10 * i),
                    none(102 + 10 * i), none(103 + 10 * i)));
        }
        Keys combined = Keys.combinations(List.of(Keys.of(left), Keys.of(right)));
        Assert.assertTrue(combined.keys.size() < left.size() * right.size());
        // The two narrow keys together
        Assert.assertEquals("[0, 100]", combined.keys.get(0).toString());
        // The last wide key of the left side with the narrow key of the right side
        Assert.assertTrue(combined.toString(),
                combined.keys.contains(key(none(80), none(81), none(82), none(83), none(100))));
    }
}
