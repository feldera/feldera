package org.dbsp.sqlCompiler.compiler.frontend;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;

import javax.annotation.Nullable;

/**
 * This class wraps a variety of possible Calcite IR objects
 * that can be used to report errors.  It is possible for all fields to be null.
 */
public class CalciteObject {
    @Nullable
    final SqlOperator operator;
    @Nullable
    final RelNode relNode;
    @Nullable
    final SqlNode sqlNode;
    @Nullable
    final RelDataType relType;
    @Nullable
    final RexNode rexNode;

    private CalciteObject(
            @Nullable SqlOperator operator,
            @Nullable RelNode relNode,
            @Nullable SqlNode sqlNode,
            @Nullable RelDataType relType,
            @Nullable RexNode rexNode) {
        this.operator = operator;
        this.relNode = relNode;
        this.sqlNode = sqlNode;
        this.relType = relType;
        this.rexNode = rexNode;
    }

    public static final CalciteObject EMPTY = new CalciteObject();

    protected CalciteObject() {
        this(null, null, null, null, null);
    }

    public CalciteObject(SqlOperator operator) {
        this(operator, null, null, null, null);
    }

    public CalciteObject(RelNode node) {
        this(null, node, null, null, null);
    }

    public CalciteObject(SqlNode node) {
        this(null, null, node, null, null);
    }

    public CalciteObject(RelDataType type) {
        this(null, null, null, type, null);
    }

    public CalciteObject(RexNode rexNode) {
        this(null, null, null, null, rexNode);
    }

    public boolean isEmpty() {
        return this.operator == null
                && this.relNode == null
                && this.sqlNode == null
                && this.relType == null
                && this.rexNode == null
                ;
    }

    @Override
    public String toString() {
        if (this.operator != null)
            return this.operator.toString();
        else if (this.relNode != null) {
            try {
                RelToSqlConverter converter =
                        new RelToSqlConverter(SqlDialect.DatabaseProduct.UNKNOWN.getDialect());
                SqlNode node = converter.visitRoot(this.relNode).asStatement();
                return node.toString();
            } catch (Exception ex) {
                // Sometimes Calcite crashes when converting rel to SQL
                return this.relNode.toString();
            }
        } else if (this.sqlNode != null)
            return this.sqlNode.toString();
        else if (this.relType != null)
            return this.relType.toString();
        else if (this.rexNode != null)
            return this.rexNode.toString();
        return "";
    }

    public String toInternalString() {
        if (this.operator != null)
            return this.operator.toString();
        else if (this.relNode != null) {
            return this.relNode.toString();
        } else if (this.sqlNode != null)
            return this.sqlNode.toString();
        else if (this.relType != null)
            return this.relType.toString();
        else if (this.rexNode != null)
            return this.rexNode.toString();
        return "";
    }

    public SourcePositionRange getPositionRange() {
        // TODO: are there more cases?
        if (this.sqlNode != null) {
            return new SourcePositionRange(this.sqlNode.getParserPosition());
        } else if (this.relType != null) {
            SqlIdentifier sqlIdentifier = this.relType.getSqlIdentifier();
            if (sqlIdentifier != null)
                return new SourcePositionRange(sqlIdentifier.getParserPosition());
        }
        return SourcePositionRange.INVALID;
    }
}
