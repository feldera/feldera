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

package org.dbsp.sqlCompiler.ir;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Base interface for all DBSP nodes.
 */
public abstract class DBSPNode
        implements IDBSPNode {
    static long crtId = 0;
    public final long id;

    /**
     * Original query Sql node that produced this node.
     */
    private final
    CalciteObject node;

    /** Controls the debugging for deterministic executions. */
    static boolean DEBUG_DETERMINISM = false;

    // If this is not null all created nodes are logged here.
    // Used for debugging the deterministic execution of the compiler.
    // The log is written when the "done()" method is called.
    @Nullable
    static List<DBSPNode> log;
    // If this list is not null each created node is compared with
    // the version from the previous log.
    @Nullable
    static List<String> previousLog;

    static final String logName = "node.log";

    static {
        startLog();
    }

    @Nullable
    static List<String> readLog() {
        File log = new File(logName);
        if (log.exists()) {
            try {
                return Files.readAllLines(log.toPath());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return null;
        }
    }

    static void saveLog() throws IOException {
        if (!DEBUG_DETERMINISM)
            return;
        if (log == null)
            return;
        File file = new File(logName);
        StringBuilder lines = new StringBuilder();
        for (DBSPNode node: log) {
            lines.append(toStringOneLine(node));
            lines.append("\n");
        }
        Utilities.writeFile(file.toPath(), lines.toString());
    }

    public static void startLog() {
        if (DEBUG_DETERMINISM) {
            log = new ArrayList<>();
            previousLog = readLog();
        }
    }

    public static void done() {
        try {
            DEBUG_DETERMINISM = false;
            // compare the current log with the previous one
            if (log != null && previousLog != null) {
                for (int i = 0; i < Math.min(log.size(), previousLog.size()); i++) {
                    DBSPNode node = log.get(i);
                    String previous = previousLog.get(i);
                    String str = toStringOneLine(node);
                    if (!str.equals(previous)) {
                        throw new RuntimeException("Node " + i +
                                " differs between runs: " + previous + " vs " + str);
                    }
                }
            }
            // save the log
            saveLog();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String toStringOneLine(DBSPNode node) {
        return node.getClass().getSimpleName() + " " + node.toString().replace("\n", "\\n");
    }

    protected DBSPNode(CalciteObject node) {
        this.node = node;
        this.id = crtId++;
        if (log != null)
            log.add(this);
    }

    /**
     * Do not call this method!
     * It is only used for testing.
     */
    public static void reset() {
        crtId = 0;
    }

    public CalciteObject getNode() { return this.node; }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        this.toString(stream);
        return builder.toString();
    }
}
