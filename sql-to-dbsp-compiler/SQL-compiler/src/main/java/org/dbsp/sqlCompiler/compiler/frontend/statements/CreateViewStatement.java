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

package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateLocalView;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** The representation of a CREATE VIEW AS ... DDL statement. */
public class CreateViewStatement extends CreateRelationStatement {
    /** Compiled and optimized query. */
    private final RelRoot compiled;
    public final SqlNode query;
    public final boolean local;

    public CreateViewStatement(SqlCreateLocalView node, String statement, String tableName,
                               boolean nameIsQuoted, @Nullable String comment,
                               List<RelColumnMetadata> columns, SqlNode query,
                               RelRoot compiled) {
        super(node, statement, tableName, nameIsQuoted, comment, columns);
        this.local = node.isLocal;
        this.query = query;
        this.compiled = compiled;
    }

    public RelNode getRelNode() {
        return Objects.requireNonNull(this.compiled).rel;
    }

    public RelRoot getRoot() {
        return this.compiled;
    }
}
