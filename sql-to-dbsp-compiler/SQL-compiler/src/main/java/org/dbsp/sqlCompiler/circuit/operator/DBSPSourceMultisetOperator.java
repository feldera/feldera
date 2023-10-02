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

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;

import javax.annotation.Nullable;
import java.util.List;

public class DBSPSourceMultisetOperator extends DBSPSourceBaseOperator {
    /**
     * Create a DBSP operator that is a source to the dataflow graph.
     * The table has *no* primary key, so the data can form a multiset.
     * @param node        Calcite node for the statement creating the table
     *                    that this node is created from.
     * @param sourceName  Calcite node for the identifier naming the table.
     * @param outputType  Type of table.
     * @param comment     A comment describing the operator.
     * @param name        The name of the table that this operator is created from.
     */
    public DBSPSourceMultisetOperator(
            CalciteObject node, CalciteObject sourceName,
            DBSPType outputType, DBSPTypeStruct originalRowType, @Nullable String comment,
            List<InputColumnMetadata> metadata, String name) {
        super(node, sourceName, outputType, originalRowType, comment, metadata, name);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression unused, DBSPType outputType) {
        return new DBSPSourceMultisetOperator(this.getNode(), this.sourceName, outputType, this.originalRowType,
                this.comment, this.metadata, this.outputName);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPSourceMultisetOperator(
                    this.getNode(), this.sourceName, this.outputType, this.originalRowType,
                    this.comment, this.metadata, this.outputName);
        return this;
    }
}
