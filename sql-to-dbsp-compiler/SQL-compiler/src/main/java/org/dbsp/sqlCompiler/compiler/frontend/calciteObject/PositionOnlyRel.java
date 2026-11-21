package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.util.IIndentStream;

import java.util.List;
import java.util.Map;

/** Represents just a source position. */
public class PositionOnlyRel extends CalciteRelNode {
    private final boolean sourceEmpty;

    public PositionOnlyRel(SourcePositionRange position) {
        super(position);
        this.sourceEmpty = true;
    }

    public PositionOnlyRel(SourcePositionRange position, List<SourcePositionRange> positions) {
        this(position, positions, true);
    }

    /** An imported node can retain the presence of provenance without retaining
     * live Calcite objects. Type factories use this distinction when interning. */
    public PositionOnlyRel(SourcePositionRange position, List<SourcePositionRange> positions, boolean sourceEmpty) {
        super(position);
        this.sourceEmpty = sourceEmpty;
        this.positions.clear();
        this.positions.addAll(positions);
    }

    @Override
    public CalciteRelNode copy() {
        return new PositionOnlyRel(this.position, this.positions, this.sourceEmpty);
    }

    /** Ordinary position wrappers are empty; imported wrappers preserve the original flag. */
    @Override
    public boolean isEmpty() {
        return this.sourceEmpty;
    }

    /** Serialized like {@link CalciteEmptyRel}: the annotation has no {@link RelNode} to name. */
    @Override
    public IIndentStream asJson(IIndentStream stream, Map<RelNode, Integer> idRemap) {
        return stream.append("null");
    }

    @Override
    public CalciteRelNode remove(RelReference node) {
        return this;
    }

    @Override
    public boolean contains(RelReference node) {
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
    public List<RelReference> getReferences() {
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
