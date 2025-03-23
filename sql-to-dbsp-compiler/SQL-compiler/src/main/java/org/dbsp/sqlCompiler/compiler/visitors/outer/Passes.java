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
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.backend.dot.ToDot;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;

import java.util.List;

public class Passes implements IWritesLogs, CircuitTransform, ICompilerComponent {
    final DBSPCompiler compiler;
    public final List<CircuitTransform> passes;
    // Generate a new name for each dumped circuit.
    static int dumped = 0;
    final long id;
    final String name;

    public Passes(String name, DBSPCompiler reporter, CircuitTransform... passes) {
        this(name, reporter, Linq.list(passes));
    }

    public Passes(String name, DBSPCompiler compiler, List<CircuitTransform> passes) {
        this.compiler = compiler;
        this.passes = passes;
        this.id = CircuitVisitor.crtId++;
        this.name = name;
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }

    public void add(CircuitTransform pass) {
        this.passes.add(pass);
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        int details = this.getDebugLevel();
        if (this.getDebugLevel() >= 3) {
            String name = String.format("%02d-", dumped++) + "before" +
                    this.toString().replace(" ", "_") + ".png";
            ToDot.dump(this.compiler, name, details, "png", circuit);
        }
        long begin = System.currentTimeMillis();
        Logger.INSTANCE.belowLevel(this, 2)
                .append(this.toString())
                .append(" starting ")
                .append(this.passes.size())
                .append(" passes")
                .increase();
        for (CircuitTransform pass: this.passes) {
            long start = System.currentTimeMillis();
            long startId = DBSPNode.outerId;
            circuit = pass.apply(circuit);
            long endId = DBSPNode.outerId;
            long end = System.currentTimeMillis();
            Logger.INSTANCE.belowLevel(this, 1)
                    .append(pass.toString())
                    .append(" took ")
                    .append(end - start)
                    .append("ms, created ")
                    .append(String.format("%,d", endId - startId))
                    .append(" nodes")
                    .newline();
            if (this.getDebugLevel() >= 3) {
                String name = String.format("%02d-", dumped++) + pass.toString().replace(" ", "_") + ".png";
                ToDot.dump(this.compiler, name, details, "png", circuit);
            }
        }
        long finish = System.currentTimeMillis();
        Logger.INSTANCE.belowLevel(this, 1)
                .decrease()
                .append(this.toString())
                .append(" took ")
                .append(finish - begin)
                .append("ms.")
                .newline();
        return circuit;
    }

    @Override
    public String toString() {
        return this.id +
                " " +
                this.name +
                this.passes.size();
    }

    @Override
    public String getName() {
        return this.name;
    }
}
