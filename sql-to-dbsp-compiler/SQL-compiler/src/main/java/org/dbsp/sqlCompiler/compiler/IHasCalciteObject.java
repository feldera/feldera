package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

public interface IHasCalciteObject {
    /** The Calcite objects from which this object has been created. */
    CalciteObject getNode();
}
