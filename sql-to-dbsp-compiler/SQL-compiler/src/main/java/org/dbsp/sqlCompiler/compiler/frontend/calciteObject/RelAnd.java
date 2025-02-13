package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Represents a set of {@link LastRel} */
public class RelAnd extends CalciteRelNode {
    final Set<LastRel> nodes;

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

    public void add(LastRel rel) {
        this.nodes.add(rel);
    }

    @Override
    public CalciteRelNode remove(RelNode node) {
        throw new UnimplementedException();
    }

    @Override
    public boolean contains(RelNode node) {
        return Linq.any(this.nodes, n -> n.contains(node));
    }

    @Override
    public CalciteRelNode after(CalciteRelNode after) {
        throw new UnimplementedException();
    }

    @Override
    public CalciteRelNode intermediate() {
        throw new UnimplementedException();
    }

    @Override
    public String toString() {
        return "And(" + String.join(",", Linq.map(this.nodes, CalciteObject::toString)) + ")";
    }
}
