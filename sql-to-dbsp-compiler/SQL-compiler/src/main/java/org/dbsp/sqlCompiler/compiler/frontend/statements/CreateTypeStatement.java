package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;

public class CreateTypeStatement extends FrontEndStatement {
    public final SqlCreateType createType;
    public final ProgramIdentifier typeName;
    // We have to be careful with this one, since it has had
    // struct references expanded.  We have to reconstruct them
    // from the createType field.
    public final RelDataType relDataType;

    public CreateTypeStatement(CalciteCompiler.ParsedStatement node,
                               SqlCreateType createType, ProgramIdentifier typeName, RelDataType relDataType) {
        super(node);
        this.createType = createType;
        this.typeName = typeName;
        this.relDataType = relDataType;
    }
}
