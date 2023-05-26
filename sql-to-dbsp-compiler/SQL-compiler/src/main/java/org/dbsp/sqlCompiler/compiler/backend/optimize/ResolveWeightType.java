package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.visitors.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;

/**
 * Replaces in the program the DBSPTypeWeight by the implementation
 * as indicated by the compiler.
 */
public class ResolveWeightType extends InnerRewriteVisitor {
    public ResolveWeightType(DBSPCompiler compiler) {
        super(compiler);
    }

    public boolean preorder(DBSPTypeWeight type) {
        this.map(type, this.getCompiler().getWeightTypeImplementation());
        return false;
    }
}
