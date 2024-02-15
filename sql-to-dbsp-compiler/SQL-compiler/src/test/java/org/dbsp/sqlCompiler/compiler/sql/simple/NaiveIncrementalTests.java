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

package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;

// Runs the EndToEnd tests but on an input stream with 3 elements each and
// using an incremental non-optimized circuit.
public class NaiveIncrementalTests extends EndToEndTests {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(true, false);
        return new DBSPCompiler(options);
    }

    @Override
    public void testQuery(String query, DBSPZSetLiteral firstOutput) {
        DBSPZSetLiteral input = createInput();
        DBSPZSetLiteral secondOutput = DBSPZSetLiteral.emptyWithElementType(
                firstOutput.getElementType());
        DBSPZSetLiteral thirdOutput = secondOutput.minus(firstOutput);
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, firstOutput),
                // Add an empty input
                new InputOutputPair(empty, secondOutput),
                // Subtract the first input
                new InputOutputPair(input.negate(), thirdOutput)
        );
    }

    @Override
    void testConstantOutput(String query, DBSPZSetLiteral output) {
        DBSPZSetLiteral input = createInput();
        DBSPZSetLiteral e = DBSPZSetLiteral.emptyWithElementType(output.getElementType());
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, output),
                // Add an empty input
                new InputOutputPair(NaiveIncrementalTests.empty, e),
                // Subtract the first input
                new InputOutputPair(input.negate(), e)
        );
    }

    @Override
    void testAggregate(String query,
                       DBSPZSetLiteral firstOutput,
                       DBSPZSetLiteral outputForEmptyInput) {
        DBSPZSetLiteral input = createInput();
        DBSPZSetLiteral secondOutput = DBSPZSetLiteral.emptyWithElementType(
                firstOutput.getElementType());
        DBSPZSetLiteral thirdOutput = outputForEmptyInput.minus(firstOutput);
        this.invokeTestQueryBase(query,
                // Add first input
                new InputOutputPair(input, firstOutput),
                // Add an empty input
                new InputOutputPair(empty, secondOutput),
                // Subtract the first input
                new InputOutputPair(input.negate(), thirdOutput)
        );
    }
}
