package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ExternalFunction;

/** Represents a CREATE FUNCTION statement */
public class CreateFunctionStatement extends FrontEndStatement {
    public final ExternalFunction function;

    public CreateFunctionStatement(SqlNode node, String statement, ExternalFunction function) {
        super(node, statement);
        this.function = function;
    }
}
