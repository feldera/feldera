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

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * This visitor can be used to serialize ZSet literals to a CSV representation.
 * Notice that there is *always* a trailing comma after the last column.
 * This allows the deserialized unambiguously figure out missing values in the last column.
 */
public class ToCsvVisitor extends InnerVisitor {
    private final Appendable appendable;
    public final Supplier<String> nullRepresentation;

    public ToCsvVisitor(DBSPCompiler compiler, Appendable destination, Supplier<String> nullRepresentation) {
        super(compiler, false);
        this.appendable = destination;
        this.nullRepresentation = nullRepresentation;
    }

    @Override
    public boolean preorder(DBSPI32Literal literal) {
        try {
            if (literal.value != null)
                this.appendable.append(Integer.toString(literal.value));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPI64Literal literal) {
        try {
            if (literal.value != null)
                this.appendable.append(Long.toString(literal.value));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPTimestampLiteral literal) {
        try {
            if (!literal.isNull)
                this.appendable.append(Long.toString(Objects.requireNonNull(literal.value)));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPFloatLiteral literal) {
        try {
            if (literal.value != null)
                this.appendable.append(Float.toString(literal.value));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPDoubleLiteral literal) {
        try {
            if (literal.value != null)
                this.appendable.append(Double.toString(literal.value));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPStringLiteral literal) {
        try {
            if (literal.value != null)
                this.appendable.append(Utilities.doubleQuote(literal.value));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPBoolLiteral literal) {
        try {
            if (literal.value != null)
                this.appendable.append(Boolean.toString(literal.value));
            else
                this.appendable.append(this.nullRepresentation.get());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression node) {
        try {
            for (DBSPExpression expression : node.fields) {
                expression.accept(this);
                this.appendable.append(",");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPZSetLiteral literal) {
        try {
            for (Map.Entry<DBSPExpression, Long> entry: literal.data.data.entrySet()) {
                DBSPExpression key = entry.getKey();
                long value = entry.getValue();
                if (value < 0)
                    throw new RuntimeException("ZSet with negative weights is not representable as CSV");
                for (; value != 0; value--) {
                    key.accept(this);
                    this.appendable.append("\n");
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return false;
    }

    /**
     * Write a literal to a file as a csv format.
     * @param compiler    A reference to a DBSPCompiler.
     * @param fileName    File to write to.
     * @param literal Literal to write.
     */
    public static File toCsv(DBSPCompiler compiler, String fileName, DBSPZSetLiteral literal) throws IOException {
        StringBuilder builder = new StringBuilder();
        ToCsvVisitor visitor = new ToCsvVisitor(compiler, builder, () -> "");
        visitor.traverse(literal);
        File file = new File(fileName);
        FileWriter writer = new FileWriter(file);
        writer.write(builder.toString());
        writer.close();
        return file;
    }
}
