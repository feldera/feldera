package org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.function.UnaryOperator;

/** Convert complex casts to simpler ones */
public class ExpandCasts extends Passes {
    public ExpandCasts(DBSPCompiler compiler) {
        this(compiler, UnaryOperator.identity());
    }

    public ExpandCasts(DBSPCompiler compiler, UnaryOperator<CircuitTransform> select) {
        super("ExpandCasts", compiler);
        this.add(new RepeatExpandSafeCasts(compiler, select));
        this.add(new RepeatExpandUnsafeCasts(compiler, select));
        this.add(select.apply(new ExpandMetadataCasts(compiler)));
    }

    public static void unsupported(DBSPExpression source, DBSPType type) {
        throw new UnsupportedException("Casting of value with type '" +
                source.getType().asSqlString() +
                "' to the target type '" + type.asSqlString() + "' not supported", source.getNode());
    }

    public static class RepeatExpandUnsafeCasts extends Repeat {
        public RepeatExpandUnsafeCasts(DBSPCompiler compiler) {
            this(compiler, UnaryOperator.identity());
        }

        private RepeatExpandUnsafeCasts(DBSPCompiler compiler, UnaryOperator<CircuitTransform> select) {
            super(compiler, select.apply(new ExpandUnsafeCasts(compiler).circuitRewriter(true)));
        }
    }

    public static class RepeatExpandSafeCasts extends Repeat {
        public RepeatExpandSafeCasts(DBSPCompiler compiler) {
            this(compiler, UnaryOperator.identity());
        }

        private RepeatExpandSafeCasts(DBSPCompiler compiler, UnaryOperator<CircuitTransform> select) {
            super(compiler, select.apply(new ExpandSafeCasts(compiler).circuitRewriter(true)));
        }
    }
}
