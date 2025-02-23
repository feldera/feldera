package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.RelColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.util.Properties;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** An object that has a schema */
public class HasSchema implements IHasSchema {
    final CalciteObject node;
    final ProgramIdentifier name;
    final List<RelColumnMetadata> columns;

    public HasSchema(CalciteObject node, ProgramIdentifier name, RelDataType rowType) {
        this.node = node;
        this.name = name;
        this.columns = new ArrayList<>();
        for (RelDataTypeField field: rowType.getFieldList()) {
            RelColumnMetadata meta = new RelColumnMetadata(
                    CalciteObject.create(field.getType()),
                    field, false, true,
                    null, null, null, null);
            this.columns.add(meta);
        }
    }

    @Override
    public CalciteObject getNode() {
        return this.node;
    }

    @Override
    public ProgramIdentifier getName() {
        return this.name;
    }

    @Override
    public List<RelColumnMetadata> getColumns() {
        return this.columns;
    }

    @Nullable @Override
    public Properties getProperties() {
        return null;
    }
}
