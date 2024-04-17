package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.ICastable;

/** This is a base class for classes that wrap
 * a variety of possible Calcite IR objects
 * that can be used to report errors. */
public class CalciteObject implements ICastable, IHasSourcePositionRange {
    public static final CalciteObject EMPTY = new CalciteObject();

    public boolean isEmpty() {
        return true;
    }

    @Override
    public String toString() {
        return "";
    }

    public String toInternalString() {
        return this.toString();
    }

    public SourcePositionRange getPositionRange() {
        return SourcePositionRange.INVALID;
    }

    public static CalciteObject create(RelNode node) {
        return new CalciteRelNode(node);
    }

    public static CalciteObject create(SqlNode node) {
        return new CalciteSqlNode(node);
    }

    public static CalciteObject create(RelDataType type) {
        return new CalciteRelDataType(type);
    }

    public static CalciteObject create(SqlOperator operator) {
        return new CalciteSqlOperator(operator);
    }

    public static CalciteObject create(RexNode node) {
        return new CalciteRexNode(node);
    }

    public static CalciteObject create(SqlParserPos pos) { return new CalciteSqlParserPos(pos); }
}
