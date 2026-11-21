package org.apache.calcite.rel;

import org.apache.calcite.rel.type.RelDataType;

/** Restore the declared row object after checking derived field types.
 * Output names are stored metadata: rewrites can retain names that a fresh
 * constructor would derive differently from its current inputs. */
public final class FelderaS1RelFactory {
    private FelderaS1RelFactory() { }

    public static void retainRowType(RelNode relation, RelDataType row) {
        RelDataType derived = relation.getRowType();
        boolean compatible = derived.getStructKind() == row.getStructKind()
                && derived.isNullable() == row.isNullable() && derived.getFieldCount() == row.getFieldCount();
        if (compatible)
            for (int index = 0; index < row.getFieldCount(); index++)
                compatible &= derived.getFieldList().get(index).getType().equals(row.getFieldList().get(index).getType());
        if (!compatible)
            throw new IllegalArgumentException("Derived relation type differs from declared row: "
                    + relation.getRowType().getFullTypeString() + " versus " + row.getFullTypeString());
        ((AbstractRelNode) relation).rowType = row;
    }
}
