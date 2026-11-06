package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

/** Metadata queries with Feldera-specific restrictions on expression lineage.
 *
 * <p>A window result depends on multiple input rows, so it cannot be described
 * by the row expressions returned by Calcite's expression-lineage metadata.
 * Calcite already returns unknown lineage for results of a relational Window,
 * but a window can also occur as a RexOver inside a Project expression.
 * Tracing that expression through a UNION enumerates combinations of column
 * origins and can retain millions of copied windows and their digest strings.
 * Reject these queries before dispatching to the metadata handlers, including
 * recursive queries made while tracing a projected column.  This only withholds
 * optional optimization metadata; it does not remove or change the window. */
public final class FelderaRelMetadataQuery extends RelMetadataQuery {
    public FelderaRelMetadataQuery(MetadataHandlerProvider provider) {
        super(provider);
    }

    @Override
    public @Nullable Set<RexNode> getExpressionLineage(RelNode rel, RexNode expression) {
        if (RexOver.containsOver(expression))
            return null;
        return super.getExpressionLineage(rel, expression);
    }
}
