package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.IHasId;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Finds whether a node occurs twice in a tree.
 * Used for debugging. */
public class RepeatedExpressions extends InnerVisitor {
    /** If true check only expressions for duplicates */
    private final boolean onlyExpressions;
    private final Set<Long> visited;
    @Nullable
    private IDBSPInnerNode duplicate;
    private final List<IDBSPInnerNode> duplicateContext = new ArrayList<>();
    private final boolean reportError;

    public RepeatedExpressions(DBSPCompiler compiler, boolean expressions, boolean reportError) {
        super(compiler);
        this.onlyExpressions = expressions;
        this.visited = new HashSet<>();
        this.duplicate = null;
        this.reportError = reportError;
    }

    void printContext() {
        System.out.println(Linq.map(this.context, IHasId::getId));
    }

    public boolean hasDuplicate() {
        return this.duplicate != null;
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        // preorder so we can find the biggest such expression.
        if (this.duplicate != null)
            return VisitDecision.STOP;
        if (this.onlyExpressions && !node.isExpression())
            return VisitDecision.CONTINUE;
        if (this.visited.contains(node.getId())) {
            this.duplicate = node;
            this.duplicateContext.addAll(this.context);
        }
        this.visited.add(node.getId());
        return VisitDecision.CONTINUE;
    }

    @Nullable
    private IDBSPInnerNode root = null;

    @Override
    public void startVisit(IDBSPInnerNode node) {
        this.root = node;
        this.visited.clear();
        this.duplicate = null;
        this.duplicateContext.clear();
        super.startVisit(node);
    }

    @Override
    public void endVisit() {
        Objects.requireNonNull(this.root);
        if (this.duplicate != null && this.reportError) {
            StringBuilder builder = new StringBuilder();
            builder.append("Expression ")
                    .append(this.root)
                    .append(" ")
                    .append(this.root.getId())
                    .append(" contains multiple instances of ")
                    .append(this.duplicate)
                    .append(" ")
                    .append(this.duplicate.getId())
                    .append(" context:\n");
            Collections.reverse(this.duplicateContext);
            for (IDBSPInnerNode parent: this.duplicateContext) {
                builder.append(parent.getId())
                        .append(" ")
                        .append(parent)
                        .append("\n");
            }
            throw new RuntimeException(builder.toString());
        }
        super.endVisit();
    }
}
