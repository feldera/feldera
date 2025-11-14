package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.dbsp.sqlCompiler.compiler.IHasSourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.util.ICastable;

import javax.annotation.Nullable;

/** This is a base class for classes that wrap
 * a variety of possible Calcite IR objects
 * that can be used to report errors. */
public class CalciteObject implements ICastable, IHasSourcePositionRange {
    public static final CalciteObject EMPTY = new CalciteObject();
    public final SourcePositionRange position;

    public CalciteObject() {
        this(SourcePositionRange.INVALID);
    }

    public CalciteObject(SourcePositionRange position) {
        this.position = position;
    }

    public boolean isEmpty() {
        return true;
    }

    @Override
    public String toString() {
        return "";
    }

    /** Format the object in a way that can displayed in an error message */
    public String getMessage() { return this.toString(); }

    public String toInternalString() {
        return this.toString();
    }

    public SourcePositionRange getPositionRange() {
        return this.position;
    }

    public static IntermediateRel create(RelNode node, SourcePositionRange range) {
        return new IntermediateRel(node, range);
    }

    public static IntermediateRel create(RelNode node) {
        return create(node, SourcePositionRange.INVALID);
    }

    public static CalciteSqlNode create(SqlNode node) {
        return new CalciteSqlNode(node);
    }

    public static CalciteObject create(ParsedStatement node) {
        return new CalciteSqlNode(node.statement());
    }

    public static CalciteObject create(RelDataType type) {
        return new CalciteRelDataType(type);
    }

    public static CalciteObject create(@Nullable RelNode context, RexNode node) {
        return new CalciteRexNode(context, node);
    }

    public static CalciteObject create(@Nullable RelNode context, AggregateCall node) {
        return new CalciteAggregateNode(context, node);
    }

    public static CalciteObject create(SqlParserPos pos) { return new CalciteSqlParserPos(pos); }

    public static CalciteObject create(SourcePositionRange pos) { return new CalciteSqlParserPos(pos); }
}
