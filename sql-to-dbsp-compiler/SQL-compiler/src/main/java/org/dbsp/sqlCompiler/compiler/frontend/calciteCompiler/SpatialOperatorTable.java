package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.model.ClassNameFilter;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.AccumOperation;
import org.apache.calcite.runtime.CollectOperation;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.runtime.UnionOperation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlSpatialTypeFunctions;
import org.apache.calcite.sql.util.SqlOperatorTables;
import java.util.List;
import java.util.Objects;

/** Calcite's spatial functions, e.g., ST_POINT and ST_DISTANCE.
 *
 * <p>Calcite discovers these functions by reflection over its own classes, the same
 * mechanism that loads user-defined functions named in a model file.  Since Calcite
 * 1.43 that mechanism consults a filter that rejects every class unless the
 * calcite.model.classes.allowed system property lists it, so
 * {@link SqlOperatorTables#spatialInstance} throws a SecurityException.  We build the
 * same table with a filter that accepts Calcite's own classes; the class names below
 * are constants, not user input. */
public class SpatialOperatorTable {
    private SpatialOperatorTable() {}

    private static final ClassNameFilter TRUSTED = ClassNameFilter.of("", "org.apache.calcite.");

    /** Reflection over all spatial functions is expensive, so the table is built once */
    private static final SqlOperatorTable INSTANCE = create();

    public static SqlOperatorTable instance() {
        return INSTANCE;
    }

    private static SqlOperatorTable create() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        SchemaPlus schema = rootSchema.plus();

        ModelHandler.addFunctions(TRUSTED, schema, null,
                SpatialTypeFunctions.class.getName(), "*", true);
        ModelHandler.addFunctions(TRUSTED, schema, null,
                SqlSpatialTypeFunctions.class.getName(), "*", true);

        schema.add("ST_UNION", Objects.requireNonNull(AggregateFunctionImpl.create(UnionOperation.class)));
        schema.add("ST_ACCUM", Objects.requireNonNull(AggregateFunctionImpl.create(AccumOperation.class)));
        schema.add("ST_COLLECT", Objects.requireNonNull(AggregateFunctionImpl.create(CollectOperation.class)));

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema, List.of(), new JavaTypeFactoryImpl(), CalciteConnectionConfigImpl.DEFAULT);
        return SqlOperatorTables.of(catalogReader.getOperatorList());
    }
}
