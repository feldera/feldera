package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Indicates the fact that the output of a {@link DBSPOperator} corresponds to the
 * result computed by a specified Calcite {@link RelNode} */
public class LastRel extends CalciteRelNode {
    public final RelNode relNode;

    public LastRel(RelNode relNode) {
        this.relNode = relNode;
    }

    @Override
    public String toString() {
        return "Last(" + this.relNode.getDigest() + ")";
    }

    @Override
    public IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap) {
        return stream.append("{").increase()
                .appendJsonLabelAndColon("final")
                .append(Utilities.getExists(idRemap, this.relNode))
                .decrease().newline()
                .append("}");
    }

    @Override
    public CalciteRelNode remove(RelNode node) {
        if (this.contains(node))
            return CalciteEmptyRel.INSTANCE;
        return this;
    }

    @Override
    public String toInternalString() {
        try {
            SqlNode node = CONVERTER.visitRoot(this.relNode).asStatement();
            return node.toString();
        } catch (Throwable ex) {
            // Sometimes Calcite crashes when converting rel to SQL
            return this.relNode.toString();
        }
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
            // Ignore the table scans
            return this;
        }
        throw new UnimplementedException(this + " after " + first);
    }

    @Override
    public CalciteRelNode intermediate() {
        return new IntermediateRel(this.relNode);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        LastRel finalRel = (LastRel) o;
        return this.relNode.equals(finalRel.relNode);
    }

    @Override
    public int hashCode() {
        return this.relNode.hashCode();
    }
}
