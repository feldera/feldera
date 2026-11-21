package org.dbsp.sqlCompiler.compiler;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperator;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteToDBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.ExternalFunction;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlUserDefinedAggregationFunction;
import org.dbsp.sqlCompiler.compiler.frontend.statements.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Experimental lowering host with no SQL frontend. The caller supplies resolved
 * objects and explicit context. Diagnostic interleaving and end-of-binding checks
 * are separate inputs still needed by the complete S1 replay contract. */
public final class S1LoweringCompiler extends DBSPCompiler {
    private final RexBuilder builder;
    private final CustomFunctions functions;
    private final CalciteToDBSPCompiler lowering;

    public S1LoweringCompiler(CompilerOptions options, RexBuilder builder, List<SqlOperator> operators) {
        super(options, true);
        this.builder = builder;
        var scalar = new HashMap<ProgramIdentifier, ExternalFunction>();
        var aggregate = new HashMap<ProgramIdentifier, SqlUserDefinedAggregationFunction>();
        for (var operator: operators) {
            if (operator instanceof ExternalFunction function) scalar.put(new ProgramIdentifier(function.getName()), function);
            if (operator instanceof SqlUserDefinedAggregationFunction function)
                aggregate.put(ProgramIdentifier.fromSqlId(function.description.name), function);
        }
        this.functions = new CustomFunctions() {
            @Override public ExternalFunction getUDF(ProgramIdentifier name) { return scalar.get(name); }
            @Override public SqlUserDefinedAggregationFunction getAggregate(ProgramIdentifier name) { return aggregate.get(name); }
        };
        this.lowering = new CalciteToDBSPCompiler(options, this, this.metadata);
    }

    @Override public RexBuilder getRexBuilder() { return this.builder; }
    @Override public CustomFunctions getCustomFunctions() { return this.functions; }
    @Override public TableContents getTableContents() { return this.lowering.getTableContents(); }
    @Override public void removeTable(ProgramIdentifier name) {
        super.removeTable(name);
        this.lowering.getTableContents().removeTable(name);
    }

    public DBSPCircuit lower(List<RelStatement> statements) {
        return this.lower(statements, index -> this.setErrorContext(statements.get(index).parsedStatement), () -> { });
    }

    public DBSPCircuit lower(List<RelStatement> statements, java.util.function.IntConsumer before, Runnable after) {
        var foreignKeys = new ArrayList<ForeignKey>();
        for (int index = 0; index < statements.size(); index++) {
            var statement = statements.get(index);
            before.accept(index);
            if (statement instanceof CreateViewStatement view) this.views.put(view.getName(), view);
            else if (statement instanceof CreateTableStatement table) foreignKeys.addAll(table.foreignKeys);
            else if (statement instanceof CreateIndexStatement declaration) {
                if (!this.validateCreateIndex(declaration)) return null;
                this.indexes.put(declaration.getName(), declaration);
            }
            this.lowering.compile(statement);
        }
        this.setErrorContext(SourcePositionRange.INVALID);
        after.run();
        var circuit = this.lowering.getFinalCircuit();
        if (circuit != null) this.validateForeignKeys(circuit, foreignKeys);
        return circuit;
    }
}
