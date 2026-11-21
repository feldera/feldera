package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Represents a set of {@link LastRel}, all of which are supposedly referring to table reads */
public class RelAnd extends CalciteRelNode {
    final Set<LastRel> nodes;
    private final boolean orderById;
    SourcePositionRange position = SourcePositionRange.INVALID;

    @Override
    public CalciteRelNode copy() {
        RelAnd result = new RelAnd(this.orderById);
        for (var last: this.nodes)
            result.add(last);
        return result.addSourcePositions(this.positions);
    }

    public RelAnd() {
        this(false);
    }

    public RelAnd(boolean orderById) {
        this.nodes = new HashSet<>();
        this.orderById = orderById;
    }

    private Collection<LastRel> positionOrder() {
        return this.orderById ? this.sort() : this.nodes;
    }

    @Override
    public List<SourcePositionRange> getSourcePositions() {
        List<SourcePositionRange> result = new ArrayList<>();
        for (LastRel lr: this.positionOrder()) {
            result.addAll(lr.getSourcePositions());
        }
        return result;
    }

    @Override
    public IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap) {
        if (this.nodes.size() == 1) {
            return this.nodes.iterator().next().asJson(stream, idRemap);
        }

        stream.append("{").increase()
                .appendJsonLabelAndColon("and")
                .append("[").increase();
        boolean first = true;
        for (CalciteRelNode node : this.sort()) {
            if (!first)
                stream.append(",");
            first = false;
            node.asJson(stream, idRemap);
        }
        return stream.decrease().append("]").newline()
                .decrease().append("}");
    }

    public void add(LastRel rel) {
        for (LastRel inside: this.nodes)
            if (inside.equals(rel))
                return;
        this.nodes.add(rel);
        if (rel.getPositionRange().isValid() && !this.position.isValid())
            this.position = rel.getPositionRange();
        Utilities.enforce(rel.relNode.isTableScan());
    }

    /** The table reads other than {@code node}; empty when {@code node} was the only one. */
    @Override
    public CalciteRelNode remove(RelReference node) {
        RelAnd result = new RelAnd(this.orderById);
        for (LastRel last : this.nodes)
            if (!last.contains(node))
                result.add(last);
        if (result.nodes.isEmpty())
            return CalciteEmptyRel.INSTANCE;
        return result.addSourcePositions(this.positions);
    }

    @Override
    public boolean contains(RelReference node) {
        return Linq.any(this.nodes, n -> n.contains(node));
    }

    @Override
    public CalciteRelNode after(CalciteRelNode after) {
        throw new UnimplementedException(this + " after " + after);
    }

    @Override
    public CalciteRelNode intermediate() {
        throw new UnimplementedException("intermediate " + this);
    }

    List<LastRel> sort() {
        List<LastRel> list = new ArrayList<>(this.nodes);
        list.sort(Comparator.comparingLong(LastRel::getId));
        return list;
    }

    /** Stable text for generated comments; debug toString retains allocation ids. */
    @Override
    public String toInternalString() {
        return "And(" + String.join(", ", Linq.map(this.sort(), CalciteObject::toInternalString)) + ")";
    }

    @Override
    public String toString() {
        // Sort for a deterministic order
        return this.getId() + " And(" + String.join(",", Linq.map(this.sort(), CalciteObject::toString)) + ")";
    }

    @Override
    public long getId() {
        return this.nodes.isEmpty() ? 0 : this.positionOrder().iterator().next().getId();
    }

    @Override
    public List<RelReference> getReferences() {
        return Linq.map(this.sort(), last -> last.relNode);
    }
}
