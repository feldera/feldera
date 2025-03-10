package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.type.IHasType;

/** A base class for Rust <a href="https://doc.rust-lang.org/reference/items.html">items</a>. */
public abstract class DBSPItem extends DBSPStatement implements IHasType {
    protected DBSPItem() {
        super(CalciteObject.EMPTY);
    }
    public abstract String getName();
    /** Get a path expression that references this item by name */
    public DBSPPathExpression getReference() {
        return new DBSPPathExpression(this.getType(), new DBSPPath(this.getName()));
    }
}
