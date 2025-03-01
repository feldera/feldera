package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Indicates that a {@link DBSPOperator} implements part of a specified {@link RelNode} */
public class IntermediateRel extends CalciteRelNode {
    final RelNode relNode;

    IntermediateRel(RelNode relNode) {
        this.relNode = relNode;
    }

    public LastRel getFinal() {
        return new LastRel(this.relNode);
    }

    @Override
    public String toString() {
        return "PartOf(" + this.relNode.getDigest() + ")";
    }

    @Override
    public IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap) {
        return stream.append("{").increase()
                .appendJsonLabelAndColon("partial")
                .append(Utilities.getExists(idRemap, this.relNode))
                .decrease().newline()
                .append("}");
    }

    @Override
    public CalciteRelNode remove(RelNode node) {
        if (this.relNode == node)
            return CalciteEmptyRel.INSTANCE;
        return this;
    }

    @Override
    public boolean contains(RelNode node) {
        return this.relNode == node;
    }

    @Override
    public CalciteRelNode after(CalciteRelNode first) {
        if (first.contains(this.relNode))
            first = first.remove(this.relNode);
        if (first.isEmpty())
            return this;
        else if (first.is(LastRel.class) || first.is(IntermediateRel.class)) {
            List<CalciteRelNode> nodes = new ArrayList<>();
            nodes.add(first);
            nodes.add(this);
            return new RelSequence(nodes);
        } else if (first.is(RelSequence.class)) {
            List<CalciteRelNode> nodes = new ArrayList<>(first.to(RelSequence.class).nodes);
            nodes.add(this);
            return new RelSequence(nodes);
        } else if (first.is(RelAnd.class)) {
            // Ignore table scans
            return this;
        }
        throw new UnimplementedException(this + " after " + first);
    }

    @Override
    public CalciteRelNode intermediate() {
        return this;
    }

    @Override
    public String toInternalString() {
        return this.relNode.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        IntermediateRel finalRel = (IntermediateRel) o;
        return this.relNode.equals(finalRel.relNode);
    }

    @Override
    public int hashCode() {
        return this.relNode.hashCode();
    }
}
