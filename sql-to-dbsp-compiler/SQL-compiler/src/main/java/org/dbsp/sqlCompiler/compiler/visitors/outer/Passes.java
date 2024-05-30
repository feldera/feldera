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

package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToDotVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.List;

public class Passes implements IWritesLogs, CircuitTransform {
    final IErrorReporter errorReporter;
    public final List<CircuitTransform> passes;
    // Generate a new name for each dumped circuit.
    static int dumped = 0;

    public Passes(IErrorReporter reporter, CircuitTransform... passes) {
        this.errorReporter = reporter;
        this.passes = Linq.list(passes);
    }

    public Passes(IErrorReporter reporter, List<CircuitTransform> passes) {
        this.errorReporter = reporter;
        this.passes = passes;
    }

    public void add(CircuitTransform pass) {
        this.passes.add(pass);
    }

    public void add(InnerRewriteVisitor inner) {
        this.passes.add(new CircuitRewriter(this.errorReporter, inner));
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        boolean details = this.getDebugLevel() >= 4;
        if (this.getDebugLevel() >= 3) {
            String name = String.format("%02d-", dumped++) + "before.png";
            Logger.INSTANCE.belowLevel(this, 3)
                    .append("Writing circuit to ")
                    .append(name)
                    .newline();
            ToDotVisitor.toDot(this.errorReporter, name, details, "png", circuit);
        }
        for (CircuitTransform pass: this.passes) {
            Logger.INSTANCE.belowLevel("Passes", 1)
                    .append("Executing ")
                    .append(pass.toString())
                    .newline();
            circuit = pass.apply(circuit);
            if (this.getDebugLevel() >= 3) {
                String name = String.format("%02d-", dumped++) + pass.toString().replace(" ", "_") + ".png";
                Logger.INSTANCE.belowLevel(this, 3)
                        .append("Writing circuit to ")
                        .append(name)
                        .newline();
                ToDotVisitor.toDot(this.errorReporter, name, details, "png", circuit);
            }
        }
        return circuit;
    }

    @Override
    public String toString() {
        StringBuilder names = new StringBuilder();
        boolean first = true;
        for (CircuitTransform pass: this.passes) {
            if (!first)
                names.append("-");
            first = false;
            names.append(pass.toString());
        }
        return names.toString();
    }
}
