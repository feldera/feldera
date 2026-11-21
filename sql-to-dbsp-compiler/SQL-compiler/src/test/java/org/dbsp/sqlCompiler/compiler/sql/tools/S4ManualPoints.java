package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.dbsp.util.Utilities;

/** Source-reviewed declarations for children created inside composite apply methods.
 * These records describe observation boundaries, not resumable checkpoints. */
final class S4ManualPoints {
    private static final String OUTER = "org.dbsp.sqlCompiler.compiler.visitors.outer.";

    private S4ManualPoints() { }

    private static void point(ArrayNode points, String owner, int index, String name,
                              String implementation, String state, String condition) {
        var point = points.addObject();
        point.put("container", owner.substring(owner.lastIndexOf('.') + 1));
        point.put("index", index);
        point.put("pass", name);
        point.put("source", (OUTER + owner).replace('.', '/') + ".java");
        point.put("kind", implementation == null ? "in_place" : "transform");
        if (implementation == null) {
            point.putNull("implementation");
            point.putArray("fields");
        } else {
            try {
                var type = Class.forName(OUTER + implementation, false, S4ManualPoints.class.getClassLoader());
                point.put("implementation", type.getName());
                point.set("fields", S4Fields.declarations(type));
            } catch (ClassNotFoundException error) {
                throw new IllegalStateException("Manual pass implementation disappeared: " + implementation, error);
            }
        }
        point.put("local_and_shared_state", state);
        point.put("condition", condition);
        point.put("replay", "complete enclosing prefix with original object lifetimes");
        point.put("field_lifetimes_reviewed", false);
    }

    private static void mergeChildren(ArrayNode points) {
        var compiler = new org.dbsp.sqlCompiler.compiler.DBSPCompiler(
                new org.dbsp.sqlCompiler.compiler.CompilerOptions());
        var children = S4PassInventory.paths(
                new org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity.MergeGC(compiler));
        for (var child: children) {
            try {
                var type = Class.forName(child.path("implementation").asText(), false,
                        S4ManualPoints.class.getClassLoader());
                ((com.fasterxml.jackson.databind.node.ObjectNode) child).set("fields", S4Fields.declarations(type));
            } catch (ClassNotFoundException error) {
                throw new IllegalStateException("Constructed MergeGC child disappeared", error);
            }
        }
        ((com.fasterxml.jackson.databind.node.ObjectNode) points.get(points.size() - 1))
                .set("nested_transforms", children);
    }

    static ArrayNode declarations() {
        var points = Utilities.deterministicObjectMapper().createArrayNode();
        String owner = "monotonicity.MonotoneAnalyzer";
        point(points, owner, 0, "EnsureExpressionTrees", "CircuitRewriter",
                "Anonymous IRTransform captures the compiler and duplicates shared expression trees.", "always");
        point(points, owner, 1, "Graph", "Graph", "Graph object reused at points 3 and 9.", "always");
        point(points, owner, 2, "SeparateIntegrators", "monotonicity.SeparateIntegrators",
                "Reads the CircuitGraphs produced at point 1.", "incrementalize");
        point(points, owner, 3, "Graph", "Graph", "Rebuilds the shared graphs after separation.", "incrementalize");
        point(points, owner, 4, "OrderErrorConsumers", null,
                "Stable sort uses reachableFromError computed from outerGraph; the same set feeds InsertLimiters.", "always");
        point(points, owner, 5, "DeltaExpandOperators", "expansion.DeltaExpandOperators",
                "Produces a separate expanded circuit and an expansion map; original circuit stays live.", "always");
        point(points, owner, 6, "Monotonicity", "monotonicity.Monotonicity",
                "Analyzes the expanded circuit; info keys retain expanded operator identities.", "always");
        point(points, owner, 7, "InsertLimiters", "monotonicity.InsertLimiters",
                "Runs on original circuit with expanded circuit, monotonicity.info, expander.expansion and reachableFromError.", "always");
        point(points, owner, 8, "MergeGC", "monotonicity.MergeGC",
                "Nested passes share grouping, canonical-operator and removal state; retain their instances.", "always");
        mergeChildren(points);
        point(points, owner, 9, "Graph", "Graph", "Rebuilds shared graphs on the merged result.", "always");
        point(points, owner, 10, "CheckRetain", "monotonicity.CheckRetain",
                "Reads the graphs produced at point 9 with the same operator identities.", "always");

        owner = "LowerAsof";
        point(points, owner, 0, "Graph", "Graph", "Graph before lowering; leftGces, rightGces and original maps are empty.", "always");
        point(points, owner, 1, "LowerAsofInner", "LowerAsof$LowerAsofInner",
                "Populates leftGces, rightGces and insertion-ordered original maps shared with ordering and MoveGC.", "always");
        point(points, owner, 2, "Graph", "Graph", "New graph instance over lowered circuit; GC maps remain live.", "leftGces nonempty");
        point(points, owner, 3, "OrderGcDependencies", "LowerAsof$OrderGcDependencies",
                "Adds graph edges by original insertion order and leftGces/rightGces lookups, then resorts circuit.", "leftGces nonempty");
        point(points, owner, 4, "MoveGC", "LowerAsof$MoveGC",
                "Consumes the same leftGces, rightGces and original maps after resorting.", "leftGces nonempty");

        owner = "LinearPostprocessRetainKeys";
        point(points, owner, 0, "Graph", "Graph", "Graph used to collect ordered waterline dependency pairs in local toAdd list.", "always");
        point(points, owner, 1, "OrderWaterlineDependencies", null,
                "Adds edges from toAdd only after traversal, then resorts circuit against augmented graph.", "toAdd nonempty");
        point(points, owner, 2, "Graph", "Graph", "Rebuilds the same Graph object's CircuitGraphs after resorting.", "toAdd nonempty");
        point(points, owner, 3, "ReplaceLinear", "LinearPostprocessRetainKeys$ReplaceLinear",
                "Reads the graphs rebuilt at point 2.", "toAdd nonempty");
        return points;
    }
}
