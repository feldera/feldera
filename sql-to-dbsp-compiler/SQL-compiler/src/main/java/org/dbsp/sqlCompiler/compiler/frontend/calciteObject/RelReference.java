package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.*;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.ViewOrigins;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.Map;

/** A relational identity used by circuit provenance. Optimizer passes need its
 * identity, display text and diagnostic origin, not an executable Calcite plan.
 * Live references are used during SQL compilation; snapshots cross the S2 seam. */
public sealed interface RelReference {
    int id();
    int planId(Map<RelNode, Integer> remap);
    boolean isTableScan();
    String digest();
    String text();
    String sql();
    @Nullable String statefulSqlName();
    @Nullable ViewOrigins.ViewSourcePosition origin(DBSPCompiler compiler);

    record Live(RelNode node) implements RelReference {
        @Override public int id() { return this.node.getId(); }
        @Override public int planId(Map<RelNode, Integer> remap) {
            return Utilities.getExists(remap, this.node);
        }
        @Override public boolean isTableScan() { return this.node instanceof TableScan; }
        @Override public String digest() { return this.node.getDigest(); }
        @Override public String text() {
            // RelNode.toString includes JVM allocation ids. This text can appear
            // in generated Rust comments, including when SQL rendering fails.
            return RelOptUtil.toString(this.node).stripTrailing();
        }
        @Override public String sql() {
            try {
                return CalciteRelNode.convertToSql(this.node).toString();
            } catch (Throwable ex) {
                // Retain the front end's fallback when Calcite cannot render a node.
                return this.text();
            }
        }
        @Override public @Nullable ViewOrigins.ViewSourcePosition origin(DBSPCompiler compiler) {
            return compiler.viewOrigins.get(this.node);
        }
        @Override public @Nullable String statefulSqlName() {
            if (this.node instanceof Join || this.node instanceof Correlate)
                return "a JOIN";
            if (this.node instanceof Aggregate aggregate)
                return aggregate.getAggCallList().isEmpty() ? "a DISTINCT" : "an aggregate";
            if (this.node instanceof Window)
                return "a window function";
            if (this.node instanceof Sort sort)
                return (sort.fetch != null || sort.offset != null) ? "an ORDER BY with LIMIT" : null;
            if (this.node instanceof Intersect intersect)
                return intersect.all ? "an INTERSECT ALL" : "an INTERSECT";
            if (this.node instanceof Minus minus)
                return minus.all ? "an EXCEPT ALL" : "an EXCEPT";
            if (this.node instanceof Union union)
                return union.all ? null : "a UNION";
            return null;
        }
    }

    /** The plan id links to the separately carried Calcite-plan JSON. A missing
     * link is retained as -1 and fails if a caller tries to emit that dataflow. */
    record Snapshot(int id, int planId, boolean isTableScan, String digest, String text,
                    String sql, @Nullable String statefulSqlName,
                    @Nullable ViewOrigins.ViewSourcePosition viewOrigin) implements RelReference {
        @Override public int planId(Map<RelNode, Integer> remap) {
            if (this.planId < 0)
                throw new IllegalStateException("Relational provenance has no carried plan entry: " + this.id);
            return this.planId;
        }
        @Override public @Nullable ViewOrigins.ViewSourcePosition origin(DBSPCompiler compiler) {
            return this.viewOrigin;
        }
    }
}
