package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ExternalFunction;

/** Represents a CREATE FUNCTION statement */
public class CreateFunctionStatement extends RelStatement {
    public final ExternalFunction function;

    public CreateFunctionStatement(ParsedStatement node, ExternalFunction function) {
        super(node);
        this.function = function;
    }
}
