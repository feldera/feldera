package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;

import java.util.List;

public class DeclareViewStatement extends CreateRelationStatement {
    public DeclareViewStatement(CalciteCompiler.ParsedStatement node, String relationName,
                                boolean nameIsQuoted, List<RelColumnMetadata> columns) {
        super(node, relationName, nameIsQuoted, columns, null);
    }

    /** Given a view name, return the name of the corresponding fake "input" view */
    public static String inputViewName(String name) {
        return name + "-input";
    }

    @Override
    public String getName() {
        return inputViewName(super.getName());
    }
}
