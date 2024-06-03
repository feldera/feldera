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
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Generate an IndexedZSet by applying a function to every element of an input dataset.
 * Output is always an IndexedZSet.  Input can be a ZSet or an IndexedZSet */
public final class DBSPMapIndexOperator extends DBSPUnaryOperator {
    /** Create an MapIndexOperator
     * @param node            Corresponding Calcite node.
     * @param indexFunction   Function that indexes.  The function has the shape
     *                        |row| (key(row), value(row)).
     * @param outputType      Type of output stream element.
     * @param input           Source operator. */
    public DBSPMapIndexOperator(CalciteObject node, DBSPExpression indexFunction,
                                DBSPTypeIndexedZSet outputType,
                                DBSPOperator input) {
        super(node, "map_index", indexFunction, outputType, input.isMultiset, input);
        DBSPType outputElementType = outputType.getKVType();
        // Expression must return a tuple that is composed of a key and a value
        this.checkResultType(indexFunction, outputElementType);
        this.checkArgumentFunctionType(indexFunction, 0, input);
    }

    /** Create an MapIndexOperator
     * @param node            Corresponding Calcite node.
     * @param indexFunction   Function that indexes.  The function has the shape
     *                        |row| (key(row), value(row)).
     * @param isMultiset      True if the input is a multiset.
     * @param outputType      Type of output stream element.
     * @param input           Source operator. */
    public DBSPMapIndexOperator(CalciteObject node, DBSPExpression indexFunction,
                                DBSPTypeIndexedZSet outputType,
                                boolean isMultiset,
                                DBSPOperator input) {
        super(node, "map_index", indexFunction, outputType, isMultiset, input);
        DBSPType outputElementType = outputType.getKVType();
        // Expression must return a tuple that is composed of a key and a value
        this.checkResultType(indexFunction, outputElementType);
        this.checkArgumentFunctionType(indexFunction, 0, input);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType type) {
        DBSPTypeIndexedZSet ixOutputType = type.to(DBSPTypeIndexedZSet.class);
        return new DBSPMapIndexOperator(
                this.getNode(), Objects.requireNonNull(expression),
                ixOutputType, this.input());
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPMapIndexOperator(
                    this.getNode(), this.getFunction(),
                    this.getOutputIndexedZSetType(), newInputs.get(0));
        return this;
    }
}
