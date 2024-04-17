package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.sql.SqlOperator;

public class CalciteSqlOperator extends CalciteObject {
    final SqlOperator operator;

    CalciteSqlOperator(SqlOperator operator) {
        this.operator = operator;
    }

    public boolean isEmpty() {
        return false;
    }

    @Override
    public String toString() {
        return this.operator.toString();
    }
}
