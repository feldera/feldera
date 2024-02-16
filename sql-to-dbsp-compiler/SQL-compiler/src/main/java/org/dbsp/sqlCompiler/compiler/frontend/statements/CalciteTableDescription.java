package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;

/** A description of a table wrapping the attributes that Calcite needs
 * to compile SQL programs that refer to this table. */
public class CalciteTableDescription extends AbstractTable implements ScannableTable {
    IHasSchema schema;

    public CalciteTableDescription(IHasSchema schema) {
        this.schema = schema;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // We don't plan to use this method, but the optimizer requires this API
        throw new UnsupportedException(schema.getNode());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (RelColumnMetadata meta : schema.getColumns())
            builder.add(meta.field);
        return builder.build();
    }
}
