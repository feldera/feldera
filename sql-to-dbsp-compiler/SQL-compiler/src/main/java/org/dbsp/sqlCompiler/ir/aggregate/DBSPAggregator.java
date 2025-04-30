package org.dbsp.sqlCompiler.ir.aggregate;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Represents roughly the DBSP Aggregator trait */
public abstract class DBSPAggregator extends DBSPExpression {
    protected DBSPAggregator(CalciteObject node, DBSPType type) {
        super(node, type);
    }
}
