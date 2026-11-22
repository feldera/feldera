package org.dbsp.sqlCompiler.compiler.visitors.outer.expandCasts;

import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CreateRuntimeErrorWrappers;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/** Expand the casts that may appear in the table metadata, e.g., DEFAULT column values */
public class ExpandMetadataCasts extends CircuitCloneVisitor {
    private final UnaryOperator<DBSPExpression> prepare;

    public ExpandMetadataCasts(DBSPCompiler compiler) {
        this(compiler, expression -> CreateRuntimeErrorWrappers.wrapCasts(compiler, expression));
    }

    public ExpandMetadataCasts(DBSPCompiler compiler, UnaryOperator<DBSPExpression> prepare) {
        super(compiler, false);
        this.prepare = prepare;
    }

    @Nullable
    DBSPExpression expand(@Nullable DBSPExpression expression) {
        if (expression == null)
            return null;
        return this.prepare.apply(expression);
    }

    @Nullable
    public TableMetadata processMetadata(TableMetadata metadata) {
        boolean changes = false;
        List<InputColumnMetadata> metas = new ArrayList<>();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            InputColumnMetadata cm = metadata.getColumnMetadata(i);
            DBSPExpression def = this.expand(cm.defaultValue);
            if (def != cm.defaultValue)
                changes = true;
            DBSPExpression lateness = this.expand(cm.lateness);
            if (lateness != cm.lateness)
                changes = true;
            if (changes) {
                cm = new InputColumnMetadata(cm.getNode(), cm.name, cm.type, cm.isPrimaryKey,
                        lateness, def, cm.defaultValuePosition);
            }
            metas.add(cm);
        }
        if (changes)
            return new TableMetadata(metadata.tableName, metas, metadata.getForeignKeys(), metadata.expectedSize,
                    metadata.materialized, metadata.isStreaming(), metadata.skipUnusedColumns);
        return null;
    }

    @Override
    public void postorder(DBSPInputMapWithWaterlineOperator operator) {
        TableMetadata tm = this.processMetadata(operator.metadata);
        if (tm != null) {
            this.map(operator, operator.withMetadata(tm), true);
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        TableMetadata tm = this.processMetadata(operator.metadata);
        if (tm != null) {
            this.map(operator, operator.withMetadata(tm));
        } else {
            super.postorder(operator);
        }
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        TableMetadata tm = this.processMetadata(operator.metadata);
        if (tm != null) {
            this.map(operator, operator.withMetadata(tm));
        } else {
            super.postorder(operator);
        }
    }
}
