package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rel.RelNode;
import org.dbsp.util.IIndentStream;

import java.util.Map;

/** Represents a CalciteObject that does not exist.
 * Similar to {@link CalciteObject#EMPTY}, but this is a subclass of {@link CalciteRelNode}. */
public class CalciteEmptyRel extends CalciteRelNode {
    private CalciteEmptyRel() {}

    public static final CalciteEmptyRel INSTANCE = new CalciteEmptyRel();

    @Override
    public boolean isEmpty() {
        return true;
    }

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
        return first;
    }

    @Override
    public CalciteRelNode intermediate() {
        return this;
    }
}
