package org.dbsp.simulator.types;

/**
 * Interface implemented by Sql types.
 */
public interface SqlType {
    int INFINITE_PRECISION = -1;

    SqlTypeName getTypeName();

    /**
     * Not all types have precision, but many do.
     */
    int getPrecision();
}
