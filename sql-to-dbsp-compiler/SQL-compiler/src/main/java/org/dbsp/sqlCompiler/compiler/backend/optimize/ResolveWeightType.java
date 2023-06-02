package org.dbsp.sqlCompiler.compiler.backend.optimize;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.visitors.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;

/**
 * Replaces in the program the DBSPTypeWeight by the implementation
 * as indicated by the compiler.
 */
public class ResolveWeightType extends InnerRewriteVisitor {
    final DBSPType weightTypeImplementation;

    public ResolveWeightType(IErrorReporter reporter, DBSPType weightTypeImplementation) {
        super(reporter);
        this.weightTypeImplementation = weightTypeImplementation;
    }

    public boolean preorder(DBSPTypeWeight type) {
        this.map(type, this.weightTypeImplementation);
        return false;
    }
}
