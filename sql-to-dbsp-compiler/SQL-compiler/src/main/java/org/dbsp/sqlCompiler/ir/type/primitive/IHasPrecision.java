package org.dbsp.sqlCompiler.ir.type.primitive;

/** Interface implemented by some types that have precision */
public interface IHasPrecision {
    int UNLIMITED_PRECISION = -1;

    /** The precision. */
    int getPrecision();
}
