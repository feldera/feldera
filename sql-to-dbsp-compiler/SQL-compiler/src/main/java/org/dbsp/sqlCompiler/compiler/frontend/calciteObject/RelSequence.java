package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Represents a sequence of RelNodes that execute one after another */
public class RelSequence extends CalciteRelNode {
    final List<CalciteRelNode> nodes;

    public RelSequence(List<CalciteRelNode> nodes) {
        Utilities.enforce(!nodes.isEmpty());
        List<CalciteRelNode> toUse = new ArrayList<>();
        CalciteRelNode previous = null;
        // Eliminate consecutive duplicates
        for (CalciteRelNode node : nodes) {
            Utilities.enforce(!node.isEmpty());
            if (previous != node)
                toUse.add(node);
            previous = node;
        }
        this.nodes = toUse;
    }

    @Override
    public IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap) {
        stream.append("{").increase()
                .appendJsonLabelAndColon("seq")
                .append("[").increase();
        boolean first = true;
        for (CalciteRelNode node : this.nodes) {
            if (!first)
                stream.append(",");
            first = false;
            node.asJson(stream, idRemap);
        }
        return stream.decrease().append("]").newline()
                .decrease().append("}");
    }

    @Override
    public CalciteRelNode remove(RelNode node) {
        List<CalciteRelNode> nodes = Linq.map(this.nodes, n -> n.remove(node));
        nodes = Linq.where(nodes, n -> !n.isEmpty());
        if (nodes.isEmpty())
            return CalciteEmptyRel.INSTANCE;
        return new RelSequence(nodes);
    }

    @Override
    public boolean contains(RelNode node) {
        return Linq.any(this.nodes, n -> n.contains(node));
    }

    @Override
    public CalciteRelNode after(CalciteRelNode first) {
        if (first.isEmpty()) {
            return this;
        } else if (first.is(IntermediateRel.class)) {
            IntermediateRel partial = first.to(IntermediateRel.class);
            if (this.contains(partial.relNode))
                return this;
            List<CalciteRelNode> nodes = new ArrayList<>();
            nodes.add(first);
            nodes.addAll(this.nodes);
            return new RelSequence(nodes);
        } else if (first.is(LastRel.class)) {
            LastRel last = first.to(LastRel.class);
            List<CalciteRelNode> nodes = new ArrayList<>();
            if (!this.contains(last.relNode))
                nodes.add(first);
            nodes.addAll(this.nodes);
            return new RelSequence(nodes);
        } else if (first.is(RelSequence.class)) {
            RelSequence seq = first.to(RelSequence.class);
            List<CalciteRelNode> nodes = new ArrayList<>(seq.nodes);
            // It is possible for the first to end in a prefix of this.
            for (CalciteRelNode node: this.nodes)
                if (!nodes.contains(node))
                    nodes.add(node);
            return new RelSequence(nodes);
        } else if (first.is(RelAnd.class)) {
            // Ignore the table scans
            return this;
        }
        throw new UnimplementedException("Lineage for " + this + ".after(" + first + ")");
    }

    @Override
    public CalciteRelNode intermediate() {
        List<CalciteRelNode> nodes = Linq.map(this.nodes, CalciteRelNode::intermediate);
        return new RelSequence(nodes);
    }

    @Override
    public String toString() {
        return "Sequence(" + String.join(",", Linq.map(this.nodes, CalciteObject::toString)) + ")";
    }
}
