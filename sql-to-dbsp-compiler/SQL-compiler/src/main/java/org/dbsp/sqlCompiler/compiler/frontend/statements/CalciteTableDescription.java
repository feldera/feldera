package org.dbsp.sqlCompiler.compiler.frontend.statements;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlFragment;
import org.dbsp.util.Properties;

import java.util.ArrayList;
import java.util.List;

/** A description of a table wrapping the attributes that Calcite needs
 * to compile SQL programs that refer to this table. */
public class CalciteTableDescription extends AbstractTable implements ScannableTable {
    final IHasSchema schema;

    public CalciteTableDescription(IHasSchema schema) {
        this.schema = schema;
    }

    public ProgramIdentifier getName() {
        return this.schema.getName();
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override
            public @Nullable Double getRowCount() {
                Properties properties = CalciteTableDescription.this.schema.getProperties();
                if (properties == null)
                    return null;
                String expectedSize = properties.getPropertyValue("expected_size");
                if (expectedSize == null)
                    return null;
                try {
                    long size = Long.parseLong(expectedSize);
                    return (double) size;
                } catch (NumberFormatException ex) {
                    return null;
                }
            }

            @Override
            public @Nullable List<ImmutableBitSet> getKeys() {
                List<Integer> indexes = new ArrayList<>();
                int index = 0;
                for (RelColumnMetadata colum : CalciteTableDescription.this.schema.getColumns()) {
                    if (colum.isPrimaryKey)
                        indexes.add(index);
                    index++;
                }
                if (indexes.isEmpty())
                    return null;
                ImmutableBitSet keys = ImmutableBitSet.of(indexes);
                return ImmutableList.of(keys);
            }
        };
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // We don't plan to use this method, but the optimizer requires this API
        throw new UnsupportedException(this.schema.getNode());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (RelColumnMetadata meta : this.schema.getColumns())
            builder.add(meta.field);
        return builder.build();
    }

    @Override
    public String toString() {
        return this.schema.toString();
    }
}
