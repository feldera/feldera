package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.annotation.GlobalAggregate;
import org.dbsp.sqlCompiler.circuit.annotation.Region;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;

/** Find operators that can be in the same DBSP circuit region and add a {@link Region} annotation.
 * Regions have to be connected components that do not create cycles when collapsed to a single node,
 * so we check this property.  We group, for example, nodes with the same {@link GlobalAggregate}
 * annotation value.
 *
 * <p>WARNING: this visitor is marked as read-only visitor, but it DOES modify annotations on operators. */
public class TagRegions extends CircuitVisitor {
    public TagRegions(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPSimpleOperator operator) {
        GlobalAggregate ga = operator.annotations.first(GlobalAggregate.class);
        if (ga == null) {
            super.postorder(operator);
            return;
        }
        Region region = new Region("agg_" + ga.id);
        operator.addAnnotation(region, DBSPSimpleOperator.class);
    }
}
