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
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChangeStream;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.junit.Test;

// Runs the EndToEnd tests but on an input stream with 3 elements each and
// using an incremental non-optimized circuit.
public class NaiveIncrementalTests extends EndToEndTests {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.optimizationLevel = 0;
        return options;
    }

    @Override
    public void testQuery(String query, DBSPZSetExpression firstOutput) {
        Change secondOutput = Change.singleEmptyWithElementType("V", firstOutput.getElementType());
        Change thirdOutput = new Change(secondOutput.getSet(0).minus(firstOutput));
        this.invokeTestQueryBase(query,
                new InputOutputChangeStream()
                        .addPair(INPUT, new Change("V", firstOutput))  // Add first input
                        .addPair(new Change("T", EMPTY), secondOutput) // Add an empty input
                        .addPair(new Change(INPUT.getSet(0).negate()), thirdOutput) // Subtract the first input
        );
    }

    @Override
    void testConstantOutput(String query, DBSPZSetExpression output) {
        Change input = INPUT;
        Change e = Change.singleEmptyWithElementType("V", output.getElementType());
        this.invokeTestQueryBase(query,
                new InputOutputChangeStream()
                        .addPair(input, new Change("V", output))                  // Add first input
                        .addPair(new Change("T", NaiveIncrementalTests.EMPTY), e) // Add an empty input
                        .addPair(new Change(input.getSet(0).negate()), e));  // Subtract the first input
    }

    @Override
    void testAggregate(String query,
                       DBSPZSetExpression firstOutput,
                       DBSPZSetExpression outputForEmptyInput) {
        Change input = INPUT;
        Change secondOutput = Change.singleEmptyWithElementType("V", firstOutput.getElementType());
        Change thirdOutput = new Change("V", outputForEmptyInput.minus(firstOutput));
        this.invokeTestQueryBase(query,
                new InputOutputChangeStream()
                        .addPair(input, new Change("V", firstOutput))   // Add first input
                        .addPair(new Change("T", EMPTY), secondOutput)  // Add an empty input
                        .addPair(new Change(input.getSet(0).negate()), thirdOutput));  // Subtract the first input
    }

    @Test
    public void divTest2() {
        // Do not run this test in incremental mode, since it produces
        // a division by 0 on an empty input.
    }
}
