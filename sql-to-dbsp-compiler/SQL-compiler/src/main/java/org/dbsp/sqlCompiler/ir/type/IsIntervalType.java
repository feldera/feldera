package org.dbsp.sqlCompiler.ir.type;

/** interface implemented by Interval types. */
public interface IsIntervalType extends IsBoundedType {
    /** This type if its last field is the unit of the representation (MONTH or SECOND);
     * otherwise the interval type of the same kind and nullability whose only field is that unit.
     * Every value of the representation is a value of the result. */
    DBSPType finestType();
}
