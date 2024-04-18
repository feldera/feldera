package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

public class CalciteSqlParserPos extends CalciteObject {
    final SqlParserPos pos;

    CalciteSqlParserPos(SqlParserPos pos) {
        this.pos = pos;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public String toString() {
        return this.pos.toString();
    }

    @Override
    public SourcePositionRange getPositionRange() {
        return new SourcePositionRange(this.pos);
    }
}
