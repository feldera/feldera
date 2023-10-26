package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;

/**
 * A base class for Rust <a href="https://doc.rust-lang.org/reference/items.html">items</a>.
 */
public abstract class DBSPItem extends DBSPStatement {
    protected DBSPItem() {
        super(CalciteObject.EMPTY);
    }
}
