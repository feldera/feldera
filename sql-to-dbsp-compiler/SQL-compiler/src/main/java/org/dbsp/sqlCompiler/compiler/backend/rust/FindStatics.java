package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/** Collects the lazy Rust static expressions needed by a generated scope. */
public final class FindStatics extends InnerVisitor {
    private final SortedMap<String, DBSPStaticExpression> expressions = new TreeMap<>();

    public FindStatics(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public VisitDecision preorder(DBSPType type) {
        return VisitDecision.STOP;
    }

    @Override
    public void postorder(DBSPStaticExpression expression) {
        this.expressions.put(expression.getName(), expression);
    }

    public List<DBSPStaticExpression> expressions() {
        return List.copyOf(this.expressions.values());
    }
}
