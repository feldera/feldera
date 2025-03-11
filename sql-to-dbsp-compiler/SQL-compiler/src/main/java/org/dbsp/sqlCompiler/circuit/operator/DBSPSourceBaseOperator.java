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

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;

/** Base class for source operators. */
public abstract class DBSPSourceBaseOperator extends DBSPSimpleOperator {
    public final ProgramIdentifier tableName;

    /** Create a DBSP operator that is a source to the dataflow graph.
     *
     * @param node       Calcite node for the statement creating the table
     *                   that this node is created from.
     * @param outputType The output type of the table.
     * @param operation  Type of table.
     * @param isMultiset True if the source data can be a multiset.
     * @param tableName  The name of the table that this operator is created from.
     * @param comment    A comment describing the operator. */
    protected DBSPSourceBaseOperator(
            CalciteRelNode node, String operation, DBSPType outputType, boolean isMultiset,
            ProgramIdentifier tableName, @Nullable String comment) {
        super(node, operation + " " + tableName, null, outputType, isMultiset, comment);
        this.tableName = tableName;
    }

    public ProgramIdentifier getTableName() {
        return this.tableName;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return this.writeComments(builder)
                .append("let ")
                .append(this.getNodeName(false))
                .append(" = ")
                .append(this.tableName.toString())
                .append("();");
    }

    @Override
    public boolean equivalent(DBSPOperator other) {
        // A source is never equivalent with another operator
        return false;
    }
}
