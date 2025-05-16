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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.util.Properties;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/** The representation of a CREATE VIEW AS ... DDL statement. */
public class CreateViewStatement extends CreateRelationStatement {
    /** Compiled and optimized query. */
    private final RelRoot compiled;
    public final SqlCreateView createView;
    final int emitFinal;
    public static final String EMIT_FINAL = "emit_final";

    public CreateViewStatement(ParsedStatement node, ProgramIdentifier tableName,
                               List<RelColumnMetadata> columns, SqlCreateView createView,
                               RelRoot compiled, int emitFinal, @Nullable Properties properties) {
        super(node, tableName, columns, properties);
        this.createView = createView;
        this.compiled = compiled;
        this.emitFinal = emitFinal;
    }

    public RelNode getRel() {
        return Objects.requireNonNull(this.compiled).rel;
    }

    public RelRoot getRoot() {
        return this.compiled;
    }

    public SqlCreateView.ViewKind getViewKind() {
        return this.createView.viewKind;
    }

    @Override
    public JsonNode asJson(boolean addColumnCaseSensitivity) {
        JsonNode node = super.asJson(addColumnCaseSensitivity);
        ObjectNode object = (ObjectNode) node;
        object.put("materialized",
                this.createView.viewKind == SqlCreateView.ViewKind.MATERIALIZED);
        return object;
    }

    /** Column number specified by "emit_final" annotation.  -1 if no such annotation */
    public int emitFinalColumn() {
        return this.emitFinal;
    }

    public void replaceColumns(List<RelColumnMetadata> columns) {
        this.columns.clear();
        this.columns.addAll(columns);
    }
}
