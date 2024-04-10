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

package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Maintains the catalog: a mapping from names to objects. */
// I am not sure this class is needed.
public class Catalog extends AbstractSchema {
    public final String schemaName;
    private final Map<String, Table> tableMap;
    private final Map<String, FrontEndStatement> definition;
    private final Multimap<String, Function> functionMap;
    private final Map<String, RelProtoDataType> typeMap;

    public Catalog(String schemaName) {
        this.schemaName = schemaName;
        this.tableMap = new HashMap<>();
        this.definition = new HashMap<>();
        this.typeMap = new HashMap<>();
        this.functionMap = ArrayListMultimap.create();
    }

    boolean addDefinition(String name, IErrorReporter reporter, FrontEndStatement statement) {
        if (this.definition.containsKey(name)) {
            reporter.reportError(statement.getPosition(), "Duplicate declaration",
                    Utilities.singleQuote(name) + " already defined");
            FrontEndStatement previous = this.definition.get(name);
            reporter.reportError(previous.getPosition(), "Duplicate declaration",
                    "Location of previous definition");
            return false;
        }
        this.definition.put(name, statement);
        return true;
    }

    public boolean addTable(String name, Table table, IErrorReporter reporter, FrontEndStatement statement) {
        this.tableMap.put(name, table);
        return this.addDefinition(name, reporter, statement);
    }

    @Override
    public Map<String, RelProtoDataType> getTypeMap() {
        return this.typeMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return this.tableMap;
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return this.functionMap;
    }

    public void dropTable(String tableName) {
        this.tableMap.remove(tableName);
    }

    public boolean addType(String name, IErrorReporter reporter, FrontEndStatement statement) {
        // Does not insert in the typeMap.
        return this.addDefinition(name, reporter, statement);
    }
}
