package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.List;
import java.util.TreeMap;

/** Opt-in P0 rule census. Counts productions, not just attempted rule matches.
 * The JSON-lines journal is evidence only when its enclosing compiler run succeeds. */
final class OptimizerTrace implements RelOptListener {
    private final Path output;
    private final ObjectNode record = Utilities.deterministicObjectMapper().createObjectNode();
    private final Map<String, Long> attempts = new TreeMap<>();
    private final Map<String, Long> productions = new TreeMap<>();

    private OptimizerTrace(Path output, String step, int level, RelNode before) {
        this.output = output;
        this.record.put("schema_version", 1);
        this.record.put("step", step);
        this.record.put("optimization_level", level);
        this.record.set("before", shape(before));
    }

    @Nullable
    static OptimizerTrace create(String step, int level, RelNode before) {
        String path = System.getProperty("feldera.optimizer.trace", "");
        return path.isBlank() ? null : new OptimizerTrace(Path.of(path), step, level, before);
    }

    void configured(List<RelOptRule> candidates) {
        var rules = this.record.putArray("configured_rules");
        // HEP instructions do not register their rules in planner.getRules().
        // These are the step's candidates at this level; shape guards may skip them.
        candidates.stream().map(Object::toString).distinct().sorted().forEach(rules::add);
    }

    private static ObjectNode shape(RelNode root) {
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        Map<String, Long> kinds = new TreeMap<>();
        var correlates = result.putArray("correlates");
        var visited = Collections.newSetFromMap(new IdentityHashMap<RelNode, Boolean>());
        var pending = new ArrayDeque<RelNode>();
        pending.add(root);
        while (!pending.isEmpty()) {
            RelNode node = pending.removeFirst().stripped();
            if (!visited.add(node))
                continue;
            kinds.merge(node.getRelTypeName(), 1L, Long::sum);
            if (node instanceof Correlate correlate) {
                var item = correlates.addObject();
                item.put("join_type", correlate.getJoinType().name());
                item.put("required_columns", correlate.getRequiredColumns().toString());
                item.put("left", correlate.getLeft().stripped().getRelTypeName());
                item.put("right", correlate.getRight().stripped().getRelTypeName());
                item.put("row_type", correlate.getRowType().getFullTypeString());
            }
            pending.addAll(node.getInputs());
        }
        result.set("node_counts", mapper.valueToTree(kinds));
        return result;
    }

    void finish(RelNode after, boolean completed) {
        var mapper = Utilities.deterministicObjectMapper();
        this.record.put("completed", completed);
        this.record.set("after", shape(after));
        this.record.set("attempts", mapper.valueToTree(this.attempts));
        this.record.set("productions", mapper.valueToTree(this.productions));
        // One test JVM may have several compiler instances. Keep journal records intact.
        synchronized (OptimizerTrace.class) {
            try {
                Files.writeString(this.output, mapper.writeValueAsString(this.record) + "\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot write optimizer trace " + this.output, e);
            }
        }
    }

    @Override public void ruleAttempted(RuleAttemptedEvent event) {
        if (event.isBefore())
            this.attempts.merge(event.getRuleCall().getRule().toString(), 1L, Long::sum);
    }

    @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
        if (event.isBefore())
            this.productions.merge(event.getRuleCall().getRule().toString(), 1L, Long::sum);
    }

    @Override public void relEquivalenceFound(RelEquivalenceEvent event) {}
    @Override public void relDiscarded(RelDiscardedEvent event) {}
    @Override public void relChosen(RelChosenEvent event) {}
}
