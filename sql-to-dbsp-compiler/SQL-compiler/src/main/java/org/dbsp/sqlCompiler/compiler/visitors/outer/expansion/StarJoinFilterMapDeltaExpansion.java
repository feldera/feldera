package org.dbsp.sqlCompiler.compiler.visitors.outer.expansion;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;

import javax.annotation.Nullable;

/** Expansion of a {@link org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator}.
 * This could be expanded in several ways.  We choose to expand it like a standard {@link DBSPStarJoinOperator}
 * followed by a {@link DBSPFilterOperator} and a {@link DBSPMapOperator}. */
public class StarJoinFilterMapDeltaExpansion extends StarJoinDeltaExpansion {
    public final DBSPFilterOperator filter;
    /** map can be Map or MapIndex */
    @Nullable
    public final DBSPSimpleOperator map;

    public StarJoinFilterMapDeltaExpansion(
            StarJoinDeltaExpansion joinExpansion,
            DBSPFilterOperator filter,
            @Nullable DBSPSimpleOperator map) {
        super(joinExpansion.integrators, joinExpansion.joins, joinExpansion.sum);
        this.filter = filter;
        this.map = map;
    }

    /** The last operator in the expansion */
    public DBSPSimpleOperator last() {
        if (this.map != null)
            return this.map;
        return this.filter;
    }
}
