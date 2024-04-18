package org.dbsp.sqlCompiler.compiler.frontend.calciteObject;

import org.apache.calcite.rex.RexNode;

public class CalciteRexNode extends CalciteObject {
    final RexNode rexNode;

    CalciteRexNode(RexNode rexNode) {
        this.rexNode = rexNode;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public String toString() {
        return this.rexNode.toString();
    }
}
