package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.util.Linq;
import org.dbsp.util.Properties;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.NULL;

/** Wraps an Avro Schema object into an IHasSchema interface */
public class AvroSchemaWrapper implements IHasSchema {
    private final RelDataTypeFactory typeFactory;
    final String name;
    final org.apache.avro.Schema schema;

    public AvroSchemaWrapper(RelDataTypeFactory typeFactory,
            org.apache.avro.Schema schema) {
        this.typeFactory = typeFactory;
        this.name = schema.getName();
        this.schema = schema;
    }

    @Override
    public CalciteObject getNode() {
        return CalciteObject.EMPTY;
    }

    @Override
    public ProgramIdentifier getName() {
        return new ProgramIdentifier(this.name);
    }

    @Override
    public List<RelColumnMetadata> getColumns() {
        RelDataType rowType = this.convertRecord(this.schema);
        return Linq.map(rowType.getFieldList(), f -> new RelColumnMetadata(
                CalciteObject.EMPTY, f, false, ProgramIdentifier.needsQuotes(f.getName()),
                null, null, null, null));
    }

    @Nullable @Override
    public Properties getProperties() {
        return null;
    }

    private RelDataType convertRecord(Schema recordSchema) {
        List<RelDataTypeField> fields = new ArrayList<>();
        int index = 0;
        for (Schema.Field field : recordSchema.getFields()) {
            Schema fieldSchema = field.schema();
            RelDataType type = this.convertType(fieldSchema);
            fields.add(new RelDataTypeFieldImpl(field.name(), index, type));
            index++;
        }
        return this.typeFactory.createStructType(fields);
    }

    private RelDataType convertType(Schema sType) {
        Schema.Type type = sType.getType();
        if (type == Schema.Type.NULL) {
            return this.typeFactory.createSqlType(NULL);
        } else if (type == Schema.Type.STRING) {
            return this.typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else if (type == Schema.Type.INT) {
            return this.typeFactory.createSqlType(SqlTypeName.INTEGER);
        } else if (type == Schema.Type.BOOLEAN) {
            return this.typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        } else if (type == Schema.Type.BYTES) {
            return this.typeFactory.createSqlType(SqlTypeName.BINARY);
        } else if (type == Schema.Type.LONG) {
            return this.typeFactory.createSqlType(SqlTypeName.BIGINT);
        } else if (type == Schema.Type.DOUBLE) {
            return this.typeFactory.createSqlType(SqlTypeName.DOUBLE);
        } else if (type == Schema.Type.FLOAT) {
            return this.typeFactory.createSqlType(SqlTypeName.REAL);
        } else if (type == Schema.Type.ARRAY) {
            return this.typeFactory.createArrayType(
                    this.convertType(sType.getElementType()), -1);
        } else if (type == Schema.Type.RECORD) {
            return convertRecord(sType);
        } else if (type == Schema.Type.MAP) {
            return this.typeFactory.createMapType(
                    // key is always a string
                    this.typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    this.convertType(sType.getValueType()));
        } else if (type == Schema.Type.FIXED) {
            return this.typeFactory.createSqlType(SqlTypeName.VARBINARY, sType.getFixedSize());
        } else if (type == Schema.Type.UNION) {
            // We only support the combination "NULL" with some other type:
            List<RelDataType> fields = Linq.map(sType.getTypes(), this::convertType);
            if (fields.size() != 2)
                throw new UnimplementedException("General 'UNION' fields in Avro not implemented: " + sType);
            if (fields.get(0).getSqlTypeName() == NULL) {
                return this.typeFactory.createTypeWithNullability(fields.get(1), true);
            } else if (fields.get(1).getSqlTypeName() == NULL) {
                return this.typeFactory.createTypeWithNullability(fields.get(0), true);
            }
        }
        throw new UnimplementedException("Avro type '" + sType + "' not yet implemented");
    }
}
