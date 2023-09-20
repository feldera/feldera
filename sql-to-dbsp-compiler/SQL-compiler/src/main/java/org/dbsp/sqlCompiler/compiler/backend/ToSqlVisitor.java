/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPFloatLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * This visitor can be used to serialize ZSet literals to a SQL representation.
 */
public class ToSqlVisitor extends InnerVisitor {
    private final StringBuilder appendable;

    public ToSqlVisitor(IErrorReporter reporter, StringBuilder destination) {
        super(reporter);
        this.appendable = destination;
    }

    @Override
    public VisitDecision preorder(DBSPI32Literal literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPI64Literal literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTimestampLiteral literal) {
        if (!literal.isNull)
            this.appendable.append(Objects.requireNonNull(literal.value));
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPFloatLiteral literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPDoubleLiteral literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPStringLiteral literal) {
        if (literal.value != null)
            this.appendable.append(Utilities.doubleQuote(literal.value));
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPBoolLiteral literal) {
        if (literal.value != null)
            this.appendable.append(literal.value);
        else
            this.appendable.append("NULL");
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPTupleExpression node) {
        for (DBSPExpression expression : node.fields) {
            expression.accept(this);
            this.appendable.append(",");
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPZSetLiteral literal) {
        for (Map.Entry<DBSPExpression, Long> entry: literal.data.data.entrySet()) {
            DBSPExpression key = entry.getKey();
            long value = entry.getValue();
            if (value < 0)
                throw new UnsupportedException("ZSet with negative weights is not representable as SQL",
                        literal.getNode());
            for (; value != 0; value--) {
                key.accept(this);
                this.appendable.append("\n");
            }
        }
        return VisitDecision.STOP;
    }
}
