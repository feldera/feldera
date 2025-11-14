package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Represents a set of {@link LastRel}, all of which are supposedly referring to table reads */
public class RelAnd extends CalciteRelNode {
    final Set<LastRel> nodes;
    SourcePositionRange position = SourcePositionRange.INVALID;

    public RelAnd() {
        this.nodes = new HashSet<>();
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
    public SourcePositionRange getPositionRange() {
        return super.getPositionRange();
    }

    public void add(LastRel rel) {
        for (LastRel inside: this.nodes)
            if (inside.equals(rel))
                return;
        this.nodes.add(rel);
        if (rel.getPositionRange().isValid() && !this.position.isValid())
            this.position = rel.getPositionRange();
        Utilities.enforce(rel.relNode instanceof TableScan);
    }

    @Override
    public CalciteRelNode remove(RelNode node) {
        throw new UnimplementedException("remove " + node);
    }

    @Override
    public boolean contains(RelNode node) {
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

    @Override
    public String toString() {
        return this.getId() + " And(" + String.join(",", Linq.map(this.nodes, CalciteObject::toString)) + ")";
    }

    @Override
    public long getId() {
        return this.nodes.isEmpty() ? 0 : this.nodes.iterator().next().getId();
    }
}
