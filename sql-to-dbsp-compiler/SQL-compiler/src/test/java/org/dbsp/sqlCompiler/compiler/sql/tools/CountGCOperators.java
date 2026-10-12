package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainValuesOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Records the GC operators of a circuit, its star joins, and its inner and left joins. */
public final class CountGCOperators extends CircuitVisitor {
    /** One letter per GC operator: K for RetainKeys, V for RetainValues, N for RetainNValues. */
    private final List<String> kinds = new ArrayList<>();
    /** The kinds of star join operators, in circuit order. */
    public final List<String> starJoins = new ArrayList<>();
    /** The kinds of inner and left join operators, in circuit order. */
    public final List<String> joins = new ArrayList<>();

    public CountGCOperators(DBSPCompiler compiler) {
        super(compiler);
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
        this.kinds.add("K");
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainValuesOperator operator) {
        this.kinds.add("V");
    }

    @Override
    public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
        this.kinds.add("N");
    }

    @Override
    public void postorder(DBSPStarJoinOperator operator) {
        this.starJoins.add("StarJoin");
    }

    @Override
    public void postorder(DBSPStarJoinIndexOperator operator) {
        this.starJoins.add("StarJoinIndex");
    }

    @Override
    public void postorder(DBSPStarJoinFilterMapOperator operator) {
        this.starJoins.add("StarJoinFilterMap");
    }

    @Override
    public void postorder(DBSPJoinOperator operator) {
        this.joins.add("Join");
    }

    @Override
    public void postorder(DBSPJoinIndexOperator operator) {
        this.joins.add("JoinIndex");
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator operator) {
        this.joins.add("JoinFilterMap");
    }

    @Override
    public void postorder(DBSPLeftJoinOperator operator) {
        this.joins.add("LeftJoin");
    }

    @Override
    public void postorder(DBSPLeftJoinIndexOperator operator) {
        this.joins.add("LeftJoinIndex");
    }

    @Override
    public void postorder(DBSPLeftJoinFilterMapOperator operator) {
        this.joins.add("LeftJoinFilterMap");
    }

    /** The letters of the GC operators, sorted; "-" for none. */
    public String kinds() {
        if (this.kinds.isEmpty())
            return "-";
        List<String> sorted = new ArrayList<>(this.kinds);
        Collections.sort(sorted);
        return String.join("", sorted);
    }
}
