package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;

import java.util.List;

/** A statement which declares a recursive view */
public class DeclareViewStatement extends CreateRelationStatement {
    public DeclareViewStatement(CalciteCompiler.ParsedStatement node, ProgramIdentifier relationName,
                                List<RelColumnMetadata> columns) {
        super(node, relationName, columns, null);
    }

    /** Given a view name, return the name of the corresponding fake "port" view */
    public static ProgramIdentifier inputViewName(ProgramIdentifier name) {
        return new ProgramIdentifier(name.name() + "-port", name.isQuoted());
    }

    /** Not the actual view name, but a name for a fake temporary view */
    @Override
    public ProgramIdentifier getName() {
        return inputViewName(super.getName());
    }
}
