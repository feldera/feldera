package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** A change is a set of ZSets, one for each input or output */
public interface IChange {
    /** Simplify the change using the Simplify visitor. */
    IChange simplify();

    /** The number of sets in the change */
    int setCount();

    /** Get a set in the change */
    DBSPZSetLiteral getSet(int index);

    /** Get the type of a set in the change */
    default DBSPType getSetType(int index) {
        return this.getSet(index).getType();
    }

    /** Get the type of an element of a set in the change */
    default DBSPType getSetElementType(int index) {
        return this.getSet(index).getElementType();
    }
}
