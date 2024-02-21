package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

public class Change implements IChange {
    public final DBSPZSetLiteral[] sets;

    public Change(DBSPZSetLiteral... sets) {
        this.sets = sets;
    }

    public Change(TableContents contents) {
        this.sets = new DBSPZSetLiteral[contents.getTableCount()];
        int index = 0;
        for (String table: contents.tablesCreated) {
            DBSPZSetLiteral data = contents.getTableContents(table);
            this.sets[index] = data;
            index++;
        }
    }

    public Change simplify() {
        Simplify simplify = new Simplify(new StderrErrorReporter());
        DBSPZSetLiteral[] simplified = Linq.map(this.sets,
                t -> simplify.apply(t).to(DBSPZSetLiteral.class), DBSPZSetLiteral.class);
        return new Change(simplified);
    }

    public static Change singleEmptyWithElementType(DBSPType elementType) {
        return new Change(DBSPZSetLiteral.emptyWithElementType(elementType));
    }

    /** Number of Z-sets in this change */
    public int setCount() {
        return this.sets.length;
    }

    public DBSPZSetLiteral getSet(int index) {
        return this.sets[index];
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (DBSPZSetLiteral zset: this.sets) {
            builder.append(zset);
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }
}
