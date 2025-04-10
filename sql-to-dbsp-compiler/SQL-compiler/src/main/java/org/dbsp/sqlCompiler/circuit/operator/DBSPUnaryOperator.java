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

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

/** Base class for all DBSP query operators that have a single input. */
public abstract class DBSPUnaryOperator extends DBSPSimpleOperator {
    protected DBSPUnaryOperator(CalciteRelNode node, String operation,
                                @Nullable DBSPExpression function, DBSPType outputType,
                                boolean isMultiset, OutputPort source,
                                boolean containsIntegrator) {
        this(node, operation, function, outputType, isMultiset, source, null, containsIntegrator);
    }

    protected DBSPUnaryOperator(CalciteRelNode node, String operation,
                                @Nullable DBSPExpression function, DBSPType outputType,
                                boolean isMultiset, OutputPort source) {
        this(node, operation, function, outputType, isMultiset, source, null, false);
    }

    @SuppressWarnings("SameParameterValue")
    protected DBSPUnaryOperator(CalciteRelNode node, String operation,
                                @Nullable DBSPExpression function, DBSPType outputType,
                                boolean isMultiset, OutputPort source,
                                @Nullable String comment,
                                boolean containsIntegrator) {
        super(node, operation, function, outputType, isMultiset, comment, containsIntegrator);
        this.addInput(source);
    }

    // Default implementation.
    @Override
    public DBSPSimpleOperator withFunction(@Nullable DBSPExpression unused, DBSPType outputType) {
        return this;
    }

    /** The only input of this operator. */
    public OutputPort input() {
        return this.inputs.get(0);
    }
}
