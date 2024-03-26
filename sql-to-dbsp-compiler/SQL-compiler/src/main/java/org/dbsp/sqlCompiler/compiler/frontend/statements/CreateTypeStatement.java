package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateType;

public class CreateTypeStatement extends FrontEndStatement {
    public final SqlCreateType createType;
    public final String typeName;
    // We have to be careful with this one, since it has had
    // struct references expanded.  We have to reconstruct them
    // from the createType field.
    public final RelDataType relDataType;

    public CreateTypeStatement(SqlNode node, String statement,
                               SqlCreateType createType, String typeName, RelDataType relDataType) {
        super(node, statement, null);
        this.createType = createType;
        this.typeName = typeName;
        this.relDataType = relDataType;
    }
}
