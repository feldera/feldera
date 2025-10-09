package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.util.Utilities;

import java.util.Map;
import java.util.Objects;

/** This visitor can be used to serialize ZSet literals to a SQL representation. */
public class ToSqlVisitor extends InnerVisitor {
    private final StringBuilder appendable;

    public ToSqlVisitor(DBSPCompiler compiler, StringBuilder destination) {
        super(compiler);
        this.appendable = destination;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral literal) {
        if (literal.value != null)
            this.appendable.append(Objects.requireNonNull(literal.value));
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPRealLiteral literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral literal) {
        if (literal.value != null)
            this.appendable.append(Utilities.doubleQuote(literal.value, false));
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append(DBSPNullLiteral.NULL);
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression node) {
        Utilities.enforce(node.fields != null);
        for (DBSPExpression expression : node.fields) {
            expression.accept(this);
            this.appendable.append(",");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetExpression expression) {
        for (Map.Entry<DBSPExpression, Long> entry: expression.data.entrySet()) {
            DBSPExpression key = entry.getKey();
            long value = entry.getValue();
            if (value < 0)
                throw new UnsupportedException("ZSet with negative weights is not representable as SQL",
                        expression.getNode());
            for (; value != 0; value--) {
                key.accept(this);
                this.appendable.append("\n");
            }
        }
        return VisitDecision.STOP;
    }
}
