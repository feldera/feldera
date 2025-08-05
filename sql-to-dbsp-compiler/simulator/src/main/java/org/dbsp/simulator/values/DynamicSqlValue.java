package org.dbsp.simulator.values;

import org.dbsp.simulator.types.SqlType;
import org.dbsp.simulator.util.ICastable;

/**
 * Interface implemented by dynamically-typed values that can be represented in Sql.
 */
public interface DynamicSqlValue extends ICastable, Comparable<DynamicSqlValue> {
    SqlType getType();
    boolean isNull();
}
