package org.dbsp.simulator.types;

/**
 * Interface implemented by Sql types.
 */
public interface SqlType extends DataType {
    SqlTypeName getTypeName();
}
