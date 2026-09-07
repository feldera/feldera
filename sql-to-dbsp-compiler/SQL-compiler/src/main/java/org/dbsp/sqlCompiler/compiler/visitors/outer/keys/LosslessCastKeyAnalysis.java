package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;

/** A key analysis in which a column that is a lossless cast of another column counts as a copy
 * of it, see {@link LosslessCastLineage}.  A key therefore survives such a cast, and a copy
 * this analysis reports may be wider than its source. */
public class LosslessCastKeyAnalysis extends KeyAnalysis {
    public LosslessCastKeyAnalysis(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    Lineage.InnerLineage interpreter() {
        return new LosslessCastLineage(this.compiler());
    }
}
