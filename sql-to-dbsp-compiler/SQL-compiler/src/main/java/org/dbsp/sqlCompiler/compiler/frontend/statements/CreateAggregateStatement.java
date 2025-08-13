package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.AggregateFunctionDescription;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlUserDefinedAggregationFunction;

public class CreateAggregateStatement extends RelStatement {
    public final SqlUserDefinedAggFunction function;

    public CreateAggregateStatement(ParsedStatement node, SqlUserDefinedAggFunction function) {
        super(node);
        this.function = function;
    }

    public SqlUserDefinedAggregationFunction getFunction() {
        return (SqlUserDefinedAggregationFunction) this.function;
    }
}
