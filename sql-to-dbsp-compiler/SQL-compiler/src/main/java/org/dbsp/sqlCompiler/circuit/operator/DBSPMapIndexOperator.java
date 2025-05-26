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

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** Generate an IndexedZSet by applying a function to every element of an input dataset.
 * Output is always an IndexedZSet.  Input can be a ZSet or an IndexedZSet */
public final class DBSPMapIndexOperator extends DBSPUnaryOperator {
    /** Create an MapIndexOperator
     * @param node            Corresponding Calcite node.
     * @param indexFunction   Function that indexes.
     * @param outputType      Type of output stream element.
     * @param input           Source operator. */
    public DBSPMapIndexOperator(CalciteRelNode node, DBSPExpression indexFunction,
                                DBSPTypeIndexedZSet outputType,
                                OutputPort input) {
        this(node, indexFunction, outputType, input.isMultiset(), input);
    }

    static DBSPTypeIndexedZSet extractType(DBSPClosureExpression expression) {
        return new DBSPTypeIndexedZSet(expression.getResultType().to(DBSPTypeRawTuple.class));
    }

    /** Create an MapIndexOperator
     * @param node            Corresponding Calcite node.
     * @param indexFunction   Function that indexes.
     * @param input           Source operator. */
    public DBSPMapIndexOperator(CalciteRelNode node, DBSPClosureExpression indexFunction,
                                OutputPort input) {
        this(node, indexFunction, extractType(indexFunction), input.isMultiset(), input);
    }

    /** Create an MapIndexOperator
     * @param node            Corresponding Calcite node.
     * @param indexFunction   Function that indexes.  The function has the shape
     *                        |row| (key(row), value(row)).
     * @param isMultiset      True if the input is a multiset.
     * @param outputType      Type of output stream element.
     * @param input           Source operator. */
    public DBSPMapIndexOperator(CalciteRelNode node, DBSPExpression indexFunction,
                                DBSPTypeIndexedZSet outputType,
                                boolean isMultiset,
                                OutputPort input) {
        super(node, "map_index", indexFunction, outputType, isMultiset, input);
        DBSPType outputElementType = outputType.getKVType();
        // Expression must return a tuple that is composed of a key and a value
        this.checkResultType(indexFunction, outputElementType);
        checkArgumentFunctionType(indexFunction, input);
        this.checkParameterCount(indexFunction, 1);
    }

    public DBSPType getKeyType() {
        return this.getOutputIndexedZSetType().keyType;
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
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            DBSPTypeIndexedZSet ixOutputType = outputType.to(DBSPTypeIndexedZSet.class);
            return new DBSPMapIndexOperator(
                    this.getRelNode(), Objects.requireNonNull(function),
                    ixOutputType, newInputs.get(0)).copyAnnotations(this);
        }
        return this;
    }

    // equivalent inherited from base class

    @SuppressWarnings("unused")
    public static DBSPMapIndexOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = DBSPSimpleOperator.commonInfoFromJson(node, decoder);
        return new DBSPMapIndexOperator(CalciteEmptyRel.INSTANCE,
                info.getClosureFunction(), info.getIndexedZsetType(), info.getInput(0))
                .addAnnotations(info.annotations(), DBSPMapIndexOperator.class);
    }
}
