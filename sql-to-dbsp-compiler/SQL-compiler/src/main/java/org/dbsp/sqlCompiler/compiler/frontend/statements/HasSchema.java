package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;

import java.util.ArrayList;
import java.util.List;

/** An object that has a schema */
public class HasSchema implements IHasSchema {
    final CalciteObject node;
    final String name;
    final List<RelColumnMetadata> columns;
    final boolean nameIsQuoted;

    public HasSchema(CalciteObject node, String name, boolean nameIsQuoted, RelDataType rowType) {
        this.node = node;
        this.name = name;
        this.columns = new ArrayList<>();
        this.nameIsQuoted = nameIsQuoted;
        for (RelDataTypeField field: rowType.getFieldList()) {
            RelColumnMetadata meta = new RelColumnMetadata(
                    CalciteObject.create(field.getType()),
                    field,
                    false,
            true,
                    null,
                    null);
            this.columns.add(meta);
        }
    }

    @Override
    public CalciteObject getNode() {
        return this.node;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean nameIsQuoted() {
        return this.nameIsQuoted;
    }

    @Override
    public List<RelColumnMetadata> getColumns() {
        return this.columns;
    }
}
