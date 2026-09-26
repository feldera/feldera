package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IIndentStream;

import java.util.List;
import java.util.Map;

/** Represents just a source position. */
public class PositionOnlyRel extends CalciteRelNode {
    public PositionOnlyRel(SourcePositionRange position) {
        super(position);
    }

    @Override
    public CalciteRelNode copy() {
        return new PositionOnlyRel(this.position);
    }

    /** True, as for {@link CalciteEmptyRel}: no relational operator produced this node,
     * so the nodes that combine with it keep their own contents. */
    @Override
    public boolean isEmpty() {
        return true;
    }

    /** Serialized like {@link CalciteEmptyRel}: the annotation has no {@link RelNode} to name. */
    @Override
    public IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap) {
        return stream.append("null");
    }

    @Override
    public CalciteRelNode remove(RelNode node) {
        return this;
    }

    @Override
    public boolean contains(RelNode node) {
        return false;
    }

    @Override
    public CalciteRelNode after(CalciteRelNode first) {
        return first.isEmpty() ? this : first;
    }

    @Override
    public CalciteRelNode intermediate() {
        return this;
    }

    @Override
    public List<RelNode> getRelNodes() {
        return List.of();
    }

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public String toString() {
        return "Position(" + this.position + ")";
    }
}
