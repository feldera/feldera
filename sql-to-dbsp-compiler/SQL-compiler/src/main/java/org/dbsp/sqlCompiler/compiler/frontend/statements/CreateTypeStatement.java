package org.dbsp.sqlCompiler.compiler.frontend.statements;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ParsedStatement;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateType;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlAttributeDefinition;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class CreateTypeStatement extends RelStatement {
    public final CalciteObject nameNode;
    public final ProgramIdentifier typeName;
    /** A null list retains the unsupported ROW-alias form, distinct from an
     * explicitly declared record with no attributes. */
    @Nullable public final List<Attribute> attributes;
    /** Calcite expands named struct references in this type. Attributes retain
     * their declared names so lowering can recover nominal struct references. */
    public final RelDataType relDataType;

    public record Attribute(ProgramIdentifier name, @Nullable ProgramIdentifier namedType,
                            @Nullable Boolean nullable) { }

    public CreateTypeStatement(ParsedStatement node, SqlCreateType createType,
                               ProgramIdentifier typeName, RelDataType relDataType) {
        this(node, CalciteObject.create(createType.name), typeName, relDataType, attributes(createType));
    }

    public CreateTypeStatement(ParsedStatement node, CalciteObject nameNode, ProgramIdentifier typeName,
                               RelDataType relDataType, @Nullable List<Attribute> attributes) {
        super(node);
        this.nameNode = nameNode;
        this.typeName = typeName;
        this.relDataType = relDataType;
        this.attributes = attributes == null ? null : List.copyOf(attributes);
    }

    @Nullable private static List<Attribute> attributes(SqlCreateType type) {
        if (type.attributeDefs == null) return null;
        var attributes = new ArrayList<Attribute>();
        for (var node: type.attributeDefs) {
            var definition = (SqlAttributeDefinition) node;
            var nameSpec = definition.dataType.getTypeNameSpec();
            var namedType = nameSpec instanceof SqlUserDefinedTypeNameSpec
                    ? ProgramIdentifier.fromSqlId(nameSpec.getTypeName()) : null;
            attributes.add(new Attribute(ProgramIdentifier.fromSqlId(definition.name), namedType,
                    definition.dataType.getNullable()));
        }
        return attributes;
    }
}
