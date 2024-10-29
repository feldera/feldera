package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ExternalFunction;

/** Represents a CREATE FUNCTION statement */
public class CreateFunctionStatement extends FrontEndStatement {
    public final ExternalFunction function;

    public CreateFunctionStatement(CalciteCompiler.ParsedStatement node, String statement, ExternalFunction function) {
        super(node, statement);
        this.function = function;
    }
}
