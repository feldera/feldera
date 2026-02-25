package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rex.RexNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;

/** Representation of a statement of the form 'SET VARIABLE = value;` in SQL */
public class SetOptionStatement extends RelStatement {
    public final ProgramIdentifier identifier;
    public final RexNode value;

    public SetOptionStatement(ParsedStatement statement, ProgramIdentifier identifier, RexNode value) {
        super(statement);
        this.identifier = identifier;
        this.value = value;
    }
}
