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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.util.Properties;

import javax.annotation.Nullable;
import java.util.List;

/** Describes a table as produced by a CREATE TABLE DDL statement. */
public class CreateTableStatement extends CreateRelationStatement {
    public final List<ForeignKey> foreignKeys;

    public CreateTableStatement(ParsedStatement node,
                                ProgramIdentifier tableName,
                                List<RelColumnMetadata> columns,
                                List<ForeignKey> foreignKeys,
                                @Nullable Properties properties) {
        super(node, tableName, columns, properties);
        this.foreignKeys = foreignKeys;
    }

    public boolean isMaterialized() {
        String mat = this.getPropertyValue("materialized");
        if (mat == null)
            return false;
        return mat.equalsIgnoreCase("true");
    }

    public boolean isAppendOnly() {
        String mat = this.getPropertyValue("append_only");
        if (mat == null)
            return false;
        return mat.equalsIgnoreCase("true");
    }

    @Override
    public JsonNode asJson(boolean addColumnCaseSensitivity) {
        JsonNode node = super.asJson(addColumnCaseSensitivity);
        ObjectNode object = (ObjectNode) node;
        object.put("materialized", this.isMaterialized());
        ArrayNode fk = object.putArray("foreign_keys");
        for (ForeignKey key: this.foreignKeys)
            fk.add(key.asJson());
        return object;
    }
}
