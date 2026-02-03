package org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

/** Convert complex casts to simpler ones */
public class ExpandCasts extends Passes {
    public ExpandCasts(DBSPCompiler compiler) {
        super("ExpandCasts", compiler);
        this.add(new RepeatExpandSafeCasts(compiler));
        this.add(new RepeatExpandUnsafeCasts(compiler));
        this.add(new ExpandMetadataCasts(compiler));
    }

    public static void unsupported(DBSPExpression source, DBSPType type) {
        throw new UnsupportedException("Casting of value with type '" +
                source.getType().asSqlString() +
                "' to the target type '" + type.asSqlString() + "' not supported", source.getNode());
    }

    public static class RepeatExpandUnsafeCasts extends Repeat {
        public RepeatExpandUnsafeCasts(DBSPCompiler compiler) {
            super(compiler, new ExpandUnsafeCasts(compiler).circuitRewriter(true));
        }
    }

    public static class RepeatExpandSafeCasts extends Repeat {
        public RepeatExpandSafeCasts(DBSPCompiler compiler) {
            super(compiler, new ExpandSafeCasts(compiler).circuitRewriter(true));
        }
    }
}
