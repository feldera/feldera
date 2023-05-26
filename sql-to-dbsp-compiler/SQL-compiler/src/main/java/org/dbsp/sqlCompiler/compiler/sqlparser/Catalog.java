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

package org.dbsp.sqlCompiler.compiler.sqlparser;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains the catalog: a mapping from table names to table objects.
 */
public class Catalog extends AbstractSchema {
    public final String schemaName;
    private final Map<String, Table> tableMap;

    public Catalog(String schemaName) {
        this.schemaName = schemaName;
        this.tableMap = new HashMap<>();
    }

    public static String identifierToString(SqlIdentifier identifier) {
        if (!identifier.isSimple())
            throw new RuntimeException("Not a simple identifier " + identifier);
        return identifier.getSimple();
    }

    public void addTable(String name, Table table) {
        this.tableMap.put(name, table);
    }

    @Override
    public Map<String, Table> getTableMap() {
        return this.tableMap;
    }

    public void dropTable(String tableName) {
        this.tableMap.remove(tableName);
    }
}
