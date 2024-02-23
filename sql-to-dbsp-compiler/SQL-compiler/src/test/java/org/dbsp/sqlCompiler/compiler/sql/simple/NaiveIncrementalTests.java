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
        Change input = createInput();
        Change secondOutput = Change.singleEmptyWithElementType(firstOutput.getElementType());
        Change thirdOutput = new Change(secondOutput.getSet(0).minus(firstOutput));
        this.invokeTestQueryBase(query,
                new InputOutputChangeStream()
                        .addPair(input, new Change(firstOutput))  // Add first input
                        .addPair(new Change(empty), secondOutput) // Add an empty input
                        .addPair(new Change(input.getSet(0).negate()), thirdOutput) // Subtract the first input
        );
    }

    @Override
    void testConstantOutput(String query, DBSPZSetLiteral output) {
        Change input = createInput();
        Change e = Change.singleEmptyWithElementType(output.getElementType());
        this.invokeTestQueryBase(query,
                new InputOutputChangeStream()
                        .addPair(input, new Change(output))                  // Add first input
                        .addPair(new Change(NaiveIncrementalTests.empty), e) // Add an empty input
                        .addPair(new Change(input.getSet(0).negate()), e));  // Subtract the first input
    }

    @Override
    void testAggregate(String query,
                       DBSPZSetLiteral firstOutput,
                       DBSPZSetLiteral outputForEmptyInput) {
        Change input = createInput();
        Change secondOutput = Change.singleEmptyWithElementType(firstOutput.getElementType());
        Change thirdOutput = new Change(outputForEmptyInput.minus(firstOutput));
        this.invokeTestQueryBase(query,
                new InputOutputChangeStream()
                        .addPair(input, new Change(firstOutput))    // Add first input
                        .addPair(new Change(empty), secondOutput)  // Add an empty input
                        .addPair(new Change(input.getSet(0).negate()), thirdOutput));  // Subtract the first input
    }
}
