package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IHasId;
import org.dbsp.util.Linq;
import org.dbsp.util.Shuffle;

import java.util.List;

/** A Change is a collection of Z-sets literals.
 * It represents an atomic change that is applied to a set of tables or views. */
public class Change implements IHasId {
    public final DBSPZSetExpression[] sets;
    public final long id;
    static long crtId = 0;

    public Change(DBSPZSetExpression... sets) {
        this.sets = sets;
        this.id = crtId++;
    }

    public Change(TableContents contents) {
        this.sets = new DBSPZSetExpression[contents.getTableCount()];
        this.id = crtId++;
        int index = 0;
        for (ProgramIdentifier table: contents.tablesCreated) {
            DBSPZSetExpression data = contents.getTableContents(table);
            this.sets[index] = data;
            index++;
        }
    }

    /** Return a change that has the sets in this one shuffled */
    public Change shuffle(Shuffle shuffle) {
        if (shuffle.isIdentityPermutation())
            return this;
        List<DBSPZSetExpression> data = Linq.list(this.sets);
        data = shuffle.shuffle(data);
        DBSPZSetExpression[] shuffled = data.toArray(new DBSPZSetExpression[0]);
        return new Change(shuffled);
    }

    public Change simplify(DBSPCompiler compiler) {
        Simplify simplify = new Simplify(compiler);
        DBSPZSetExpression[] simplified = Linq.map(this.sets,
                t -> simplify.apply(t).to(DBSPZSetExpression.class), DBSPZSetExpression.class);
        return new Change(simplified);
    }

    /** Create a Change for a single ZSet, representing an empty ZSet
     * with the specified element type. */
    public static Change singleEmptyWithElementType(DBSPType elementType) {
        return new Change(DBSPZSetExpression.emptyWithElementType(elementType));
    }

    /** Number of Z-sets in this change */
    public int getSetCount() {
        return this.sets.length;
    }

    public DBSPZSetExpression getSet(int index) {
        return this.sets[index];
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (DBSPZSetExpression zset: this.sets) {
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

    public boolean compatible(Change outputs) {
        if (this.getSetCount() != outputs.getSetCount())
            return false;
        for (int i = 0; i < this.getSetCount(); i++)
            if (!this.getSet(i).getType().sameType(outputs.getSet(i).getType()))
                return false;
        return true;
    }

    @Override
    public long getId() {
        return this.id;
    }
}
