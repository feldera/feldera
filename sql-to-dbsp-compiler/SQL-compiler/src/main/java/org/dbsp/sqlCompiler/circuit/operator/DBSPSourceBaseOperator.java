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

package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/**
 * Base class for source operators.
 */
public abstract class DBSPSourceBaseOperator extends DBSPOperator {
    public final String tableName;

    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * @param node        Calcite node for the statement creating the table
     *                    that this node is created from.
     * @param isMultiset  True if the source data can be a multiset.
     * @param outputType  Type of table.
     * @param comment     A comment describing the operator.
     * @param tableName   The name of the table that this operator is created from.
     */
    public DBSPSourceBaseOperator(
            CalciteObject node,
            DBSPType outputType, boolean isMultiset, @Nullable String comment,
            String tableName) {
        super(node, "source " + tableName, null, outputType, isMultiset, comment);
        this.tableName = tableName;
    }

    public String getTableName() {
        return this.tableName;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return this.writeComments(builder)
                .append("let ")
                .append(this.getOutputName())
                .append(" = ")
                .append(this.tableName)
                .append("();");
    }
}
