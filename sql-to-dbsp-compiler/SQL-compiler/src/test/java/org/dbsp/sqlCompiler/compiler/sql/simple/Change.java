package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

/** A Change is a collection of Z-sets literals.
 * It represents an atomic change that is applied to a set of tables or views. */
public class Change {
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

    /** Create a Change for a single ZSet, representing an empty ZSet
     * with the specified element type. */
    public static Change singleEmptyWithElementType(DBSPType elementType) {
        return new Change(DBSPZSetLiteral.emptyWithElementType(elementType));
    }

    /** Number of Z-sets in this change */
    public int getSetCount() {
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

    /** Get the type of a set in the change */
    public DBSPType getSetType(int index) {
        return this.getSet(index).getType();
    }

    /** Get the type of an element of a set in the change */
    public DBSPType getSetElementType(int index) {
        return this.getSet(index).getElementType();
    }
}
