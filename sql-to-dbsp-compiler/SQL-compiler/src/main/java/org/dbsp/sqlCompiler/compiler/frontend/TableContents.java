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

package org.dbsp.sqlCompiler.compiler.frontend;

import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.CreateTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DropTableStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.RelStatement;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpandCasts;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This class keeps track of the contents of the tables as
 * it exists because of the execution of simple INSERT or DELETE statements.
 * It gives an instantaneous view of the table contents, after the execution
 * of a sequence of statements. */
public class TableContents implements ICompilerComponent {
    public final List<ProgramIdentifier> tablesCreated = new ArrayList<>();
    /** Remember the last statement that created each table. */
    final Map<ProgramIdentifier, CreateTableStatement> tableCreation = new HashMap<>();
    /** Keep track of the contents of each table. */
    final Map<ProgramIdentifier, DBSPZSetExpression> tableContents;
    final DBSPCompiler compiler;

    public TableContents(DBSPCompiler compiler) {
        this.compiler = compiler;
        this.tableContents = new HashMap<>();
    }

    public DBSPZSetExpression getTableContents(ProgramIdentifier tableName) {
        return Utilities.getExists(this.tableContents, tableName);
    }

    public TableData getTableData(ProgramIdentifier tableName) {
        return new TableData(tableName,
                this.getTableContents(tableName),
                this.getPrimaryKeyColumns(tableName));
    }

    public List<Integer> getPrimaryKeyColumns(ProgramIdentifier tableName) {
        CreateTableStatement stat = Utilities.getExists(this.tableCreation, tableName);
        return stat.getPrimaryKeyColumns();
    }

    /** "Execute" a DDL statement.
     * @return True on success. */
    public boolean execute(RelStatement statement) {
        if (statement.is(CreateTableStatement.class)) {
            CreateTableStatement create = statement.to(CreateTableStatement.class);
            Utilities.putNew(this.tableCreation, create.relationName, create);
            this.tablesCreated.add(create.relationName);
            Utilities.putNew(this.tableContents, create.relationName,
                    DBSPZSetExpression.emptyWithElementType(
                            create.getRowTypeAsTuple(this.compiler.getTypeCompiler())));
        } else if (statement.is(DropTableStatement.class)) {
            DropTableStatement drop = statement.to(DropTableStatement.class);
            this.tableCreation.remove(drop.tableName);
            this.tablesCreated.remove(drop.tableName);
            this.tableContents.remove(drop.tableName);
        }
        return true;
    }

    public CreateTableStatement getTableDefinition(ProgramIdentifier tableName) {
        return Utilities.getExists(this.tableCreation, tableName);
    }

    public void addToTable(ProgramIdentifier tableName, DBSPZSetExpression value, DBSPCompiler compiler) {
        DBSPZSetExpression table = this.tableContents.get(tableName);
        Simplify simplify = new Simplify(compiler);
        ExpandCasts expand = new ExpandCasts(compiler);
        var expanded = expand.apply(value);
        var simplified = simplify.apply(expanded);
        table.addUsingCast(simplified.to(DBSPZSetExpression.class));
    }

    public int getTableIndex(ProgramIdentifier tableName) {
        for (int i = 0; i < this.tablesCreated.size(); i++)
            if (this.tablesCreated.get(i).equals(tableName))
                return i;
        throw new InternalCompilerError("No table named " + tableName, CalciteObject.EMPTY);
    }

    @Override
    public DBSPCompiler compiler() {
        return this.compiler;
    }

    public int getTableCount() {
        return this.tablesCreated.size();
    }

    /** Clear the contents of all tables */
    public void clear() {
        for (Map.Entry<ProgramIdentifier, DBSPZSetExpression> entry: this.tableContents.entrySet()) {
            entry.setValue(DBSPZSetExpression.emptyWithElementType(entry.getValue().getElementType()));
        }
    }

    public void removeTable(ProgramIdentifier name) {
        this.tableContents.remove(name);
        this.tablesCreated.remove(name);
    }
}
