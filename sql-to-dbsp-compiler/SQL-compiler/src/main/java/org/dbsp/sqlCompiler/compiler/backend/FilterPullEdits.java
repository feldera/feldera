package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;

import java.util.List;
import java.util.Map;

/** Install predicates lowered against individual producer closures by the Rust filter pass. */
final class FilterPullEdits extends CircuitCloneVisitor {
    enum Site { Map, MapIndex, Aggregate }

    private final Map<Long, Site> sites;

    FilterPullEdits(DBSPCompiler compiler, Map<Long, Site> sites) {
        super(compiler, false);
        this.sites = sites;
    }

    @Override
    public void postorder(DBSPFilterOperator filter) {
        var site = this.sites.get(filter.id);
        if (site != null) {
            var producer = this.mapped(filter.input()).simpleNode();
            var predicate = RustCircuitPass.pulledPredicate(this.compiler(), site, producer, filter.getClosureFunction());
            if (predicate != null) {
                var first = new DBSPFilterOperator(filter.getRelNode(), predicate, producer.inputs.get(0))
                        .copyAnnotations(filter);
                this.addOperator(first);
                this.map(filter, producer.withInputs(List.of(first.outputPort()), true).to(DBSPSimpleOperator.class));
                return;
            }
        }
        super.postorder(filter);
    }
}
