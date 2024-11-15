package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;

import java.util.List;

/** A statement which declares a recursive view */
public class DeclareViewStatement extends CreateRelationStatement {
    public DeclareViewStatement(CalciteCompiler.ParsedStatement node, String relationName,
                                boolean nameIsQuoted, List<RelColumnMetadata> columns) {
        super(node, relationName, nameIsQuoted, columns, null);
    }

    /** Given a view name, return the name of the corresponding fake "port" view */
    public static String inputViewName(String name) {
        return name + "-port";
    }

    /** Not the actual view name, but a name for a fake temporary view */
    @Override
    public String getName() {
        return inputViewName(super.getName());
    }
}
