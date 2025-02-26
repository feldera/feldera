package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public class CalciteRelDataType extends CalciteObject {
    final RelDataType relType;

    CalciteRelDataType(RelDataType relType) {
        this.relType = relType;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public String toString() {
        return this.relType.toString();
    }

    public SourcePositionRange getPositionRange() {
        SqlIdentifier sqlIdentifier = this.relType.getSqlIdentifier();
        if (sqlIdentifier != null)
            return new SourcePositionRange(sqlIdentifier.getParserPosition());
        return SourcePositionRange.INVALID;
    }
}
