package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.util.Linq;

import java.util.List;

/** Description of a user-defined aggregate function */
public class AggregateFunctionDescription implements AggregateFunction {
    public final SqlIdentifier name;
    public final CalciteObject node;
    public final RelDataType returnType;
    public final List<RelDataTypeField> parameterList;
    public final boolean linear;
    public final SourcePositionRange position;
    public final SqlReturnTypeInference returnInference;
    public final SqlOperandTypeInference operandInference;

    static SqlOperandTypeInference createTypeInference(List<RelDataTypeField> parameters) {
        return InferTypes.explicit(Linq.map(parameters, RelDataTypeField::getType));
    }

    public AggregateFunctionDescription(SqlIdentifier name, RelDataType returnType,
                                        List<RelDataTypeField> parameters, boolean linear) {
        this.name = name;
        this.node = CalciteObject.create(name);
        this.position = new SourcePositionRange(name.getParserPosition());
        this.returnType = returnType;
        this.parameterList = parameters;
        this.linear = linear;
        this.returnInference = ReturnTypes.explicit(returnType);
        this.operandInference = createTypeInference(parameters);
    }

    public String getName() {
        return this.name.getSimple();
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
        return this.returnType;
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return Linq.map(this.parameterList, p -> new FunctionParameter() {
            @Override
            public int getOrdinal() {
                return p.getIndex();
            }

            @Override
            public String getName() {
                return p.getName();
            }

            @Override
            public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
                return p.getType();
            }

            @Override
            public boolean isOptional() {
                return false;
            }
        });
    }
}
