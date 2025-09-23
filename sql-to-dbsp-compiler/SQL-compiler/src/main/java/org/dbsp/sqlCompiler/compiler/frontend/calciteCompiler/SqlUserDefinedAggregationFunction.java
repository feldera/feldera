package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Linq;

import java.util.List;

/**
 * Calcite only supports user-defined aggregates defined in Java.
 * This is a user-defined aggregation function declaration.
 */
public class SqlUserDefinedAggregationFunction extends SqlUserDefinedAggFunction {
    final SqlOperandMetadata operandTypeChecker;
    public final AggregateFunctionDescription description;

    public SqlUserDefinedAggregationFunction(AggregateFunctionDescription function) {
        super(function.name, SqlKind.OTHER,
                function.returnInference,
                function.operandInference, null,
                function, false, false, Optionality.FORBIDDEN);
        SqlOperandTypeChecker checker = ExternalFunction.createTypeChecker(
                function.name.getSimple(), function.parameterList);
        this.description = function;
        this.operandTypeChecker = new SqlOperandMetadata() {
            @Override
            public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
                return Linq.map(function.parameterList, RelDataTypeField::getType);
            }

            @Override
            public List<String> paramNames() {
                return Linq.map(function.parameterList, RelDataTypeField::getName);
            }

            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
                return checker.checkOperandTypes(callBinding, throwOnFailure);
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
                return checker.getOperandCountRange();
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
                return checker.getAllowedSignatures(op, opName);
            }
        };
    }

    public boolean isLinear() {
        return this.description.linear;
    }

    @Override
    public @Nullable SqlOperandTypeInference getOperandTypeInference() {
        return super.getOperandTypeInference();
    }

    @Override
    public @Nullable SqlOperandMetadata getOperandTypeChecker() {
        return this.operandTypeChecker;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return this.operandTypeChecker.getOperandCountRange();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return this.operandTypeChecker.checkOperandTypes(callBinding, throwOnFailure);
    }
}
