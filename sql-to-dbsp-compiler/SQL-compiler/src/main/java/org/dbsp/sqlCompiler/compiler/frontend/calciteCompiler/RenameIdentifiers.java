package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateFunctionDeclaration;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlExtendedColumnDeclaration;
import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** A shuttle which renames the identifiers in a SQL program to anonymize them. */
public class RenameIdentifiers extends SqlShuttle {
    static class RenameMap {
        final Map<String, String> nameMap;
        final String prefix;
        int id;

        RenameMap(String prefix) {
            this.nameMap = new HashMap<>();
            this.prefix = prefix;
            this.id = 0;
        }

        String rename(String id) {
            if (this.nameMap.containsKey(id))
                return this.nameMap.get(id);
            String newName = this.prefix + this.id;
            this.id++;
            this.nameMap.put(id, newName);
            return newName;
        }
    }

    final RenameMap nameMap;

    public RenameIdentifiers() {
        this.nameMap = new RenameMap("ID_");
    }

    @Override
    public @Nullable SqlNode visit(SqlIdentifier id) {
        if (id.isStar()) {
            return super.visit(id);
        }
        List<String> names = Linq.map(id.names, this.nameMap::rename);
        return new SqlIdentifier(names, SqlParserPos.ZERO);
    }

    @Nullable SqlNodeList rewriteProperties(@Nullable SqlNodeList properties) {
        // This has some knowledge of the semantics of some properties.
        // It strips out the connectors altogether
        // It renames the property for emit_final.
        if (properties == null)
            return properties;
        List<SqlNode> result = new ArrayList<>();
        int index = 0;
        boolean skipValue = false;
        boolean renameValue = false;
        for (SqlNode node: properties) {
            SqlLiteral lit = (SqlLiteral) node;
            String litValue = lit.getValueAs(String.class);
            if (index % 2 == 0) {
                // property name
                if (litValue.equalsIgnoreCase("connectors")) {
                    skipValue = true;
                } else {
                    result.add(lit);
                }
                if (litValue.equalsIgnoreCase("emit_final")) {
                    renameValue = true;
                }
            } else {
                if (skipValue) {
                    skipValue = false;
                } else if (renameValue) {
                    // We have to convert to uppercase
                    String renamed = this.nameMap.rename(litValue.toLowerCase(Locale.ENGLISH));
                    SqlLiteral literal = SqlLiteral.createCharString(renamed, lit.getParserPosition());
                    result.add(literal);
                    renameValue = false;
                } else {
                    result.add(lit);
                }
            }
            index++;
        }
        if (result.isEmpty())
            return null;
        return new SqlNodeList(result, properties.getParserPosition());
    }

    SqlIdentifier renameIdentifier(SqlIdentifier id) {
        return (SqlIdentifier) Objects.requireNonNull(this.visit(id));
    }

    List<SqlIdentifier> renameIdentifiers(List<SqlIdentifier> list) {
        return Linq.map(list, this::renameIdentifier);
    }

    SqlTypeNameSpec processSpec(SqlTypeNameSpec type) {
        if (type instanceof SqlBasicTypeNameSpec)
            return type;
        if (type instanceof SqlMapTypeNameSpec map) {
            SqlNode key = super.visit(map.getKeyType());
            SqlNode value = super.visit(map.getValType());
            if (key != map.getKeyType() || value != map.getValType()) {
                return new SqlMapTypeNameSpec(
                        (SqlDataTypeSpec) Objects.requireNonNull(key),
                        (SqlDataTypeSpec) Objects.requireNonNull(value),
                        map.getParserPos());
            }
            return type;
        }
        if (type instanceof SqlCollectionTypeNameSpec coll) {
            SqlTypeNameSpec elementType = coll.getElementTypeName();
            SqlTypeNameSpec renamed = this.processSpec(elementType);
            // There is no way to extract the collection type name! It's a private field, and there is
            // no accessor!
            SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(CalciteSqlDialect.DEFAULT));
            coll.unparse(writer, 0, 0);
            String str = writer.toString();
            boolean array = str.toLowerCase().endsWith("array");
            SqlTypeName collectionName = array ? SqlTypeName.ARRAY : SqlTypeName.MULTISET;
            if (renamed != elementType) {
                return new SqlCollectionTypeNameSpec(renamed, true, collectionName, coll.getParserPos());
            }
            return type;
        }
        if (type instanceof SqlUserDefinedTypeNameSpec spec) {
            SqlIdentifier id = spec.getTypeName();
            if (id.isSimple() && SqlToRelCompiler.DEFAULT_TYPE_ALIASES.containsKey(
                    id.getSimple().toUpperCase(Locale.ENGLISH)))
                return type;
            SqlIdentifier renamed = this.renameIdentifier(id);
            return new SqlUserDefinedTypeNameSpec(renamed, spec.getParserPos());
        }
        throw new InternalCompilerError("Unexpected type in SQL program " + type);
    }

    @Override
    public @Nullable SqlNode visit(SqlDataTypeSpec spec) {
        SqlTypeNameSpec type = spec.getTypeNameSpec();
        SqlTypeNameSpec renamed = this.processSpec(type);
        if (renamed != type) {
            return new SqlDataTypeSpec(renamed, spec.getTimeZone(), spec.getNullable(), spec.getParserPosition());
        }
        return spec;
    }

    @Override
    public @Nullable SqlNode visit(SqlCall call) {
        if (call instanceof SqlExtendedColumnDeclaration ecd) {
            SqlIdentifier name = this.renameIdentifier(ecd.name);
            var foreignKeyTables = this.renameIdentifiers(ecd.foreignKeyTables);
            var foreignKeyColumns = this.renameIdentifiers(ecd.foreignKeyColumns);
            var type = Objects.requireNonNull(this.visit(ecd.dataType));
            return new SqlExtendedColumnDeclaration(ecd.getParserPosition(),
                    name, (SqlDataTypeSpec) type, ecd.expression, ecd.strategy,
                    foreignKeyTables, foreignKeyColumns, ecd.primaryKey,
                    ecd.lateness, ecd.watermark, ecd.defaultValue, ecd.interned);
        } else if (call instanceof SqlCreateView cv) {
            SqlIdentifier name = this.renameIdentifier(cv.name);
            SqlNodeList columnList = cv.columnList == null ? null : (SqlNodeList) super.visit(cv.columnList);
            SqlNode query = Objects.requireNonNull(super.visitNode(cv.query));
            SqlNodeList properties = this.rewriteProperties(cv.viewProperties);
            return new SqlCreateView(cv.getParserPosition(), cv.getReplace(), cv.viewKind, name,
                    columnList, properties, query);
        } else if (call instanceof org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable) {
            org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable tbl =
                    (org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable) super.visit(call);
            SqlNodeList properties = Objects.requireNonNull(tbl).tableProperties;
            properties = this.rewriteProperties(properties);
            return new org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateTable(
                    tbl.getParserPosition(), tbl.getReplace(), tbl.ifNotExists,
                    tbl.name, tbl.columnsOrForeignKeys, properties);
        } else if (call instanceof SqlCreateFunctionDeclaration decl) {
            SqlCreateFunctionDeclaration rewrittenCall = (SqlCreateFunctionDeclaration) Objects.requireNonNull(super.visit(call));
            SqlIdentifier name = decl.getName();
            if (name.isSimple() && name.getSimple().toLowerCase(Locale.ENGLISH)
                    .startsWith(ExternalFunction.JSONSTRING_AS)) {
                String type = name.getSimple().substring(ExternalFunction.JSONSTRING_AS.length());
                String newName = ExternalFunction.JSONSTRING_AS + this.nameMap.rename(type);
                return new SqlCreateFunctionDeclaration(decl.getParserPosition(), decl.getReplace(), decl.ifNotExists,
                        new SqlIdentifier(newName, name.getParserPosition()), rewrittenCall.getParameters(),
                        rewrittenCall.getReturnType(), rewrittenCall.getBody());
            }
        } else if (call instanceof SqlBasicCall) {
            SqlBasicCall basic = (SqlBasicCall) Objects.requireNonNull(super.visit(call));
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlUnresolvedFunction uf) {
                String name = uf.getName();
                if (name.toLowerCase(Locale.ENGLISH).startsWith(ExternalFunction.JSONSTRING_AS)) {
                    String type = name.substring(ExternalFunction.JSONSTRING_AS.length());
                    String newName = ExternalFunction.JSONSTRING_AS + this.nameMap.rename(type);
                    SqlIdentifier id = new SqlIdentifier(newName, SqlParserPos.ZERO);
                    SqlUnresolvedFunction newOp = new SqlUnresolvedFunction(id, uf.getReturnTypeInference(),
                            uf.getOperandTypeInference(), uf.getOperandTypeChecker(), null,
                            uf.getFunctionType());
                    return new SqlBasicCall(newOp, basic.getOperandList(), basic.getParserPosition());
                }
            }
        }
        return super.visit(call);
    }
}
