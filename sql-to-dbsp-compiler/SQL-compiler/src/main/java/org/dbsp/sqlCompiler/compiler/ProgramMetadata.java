package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToSqlVisitor;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.statements.DeclareViewStatement;
import org.dbsp.sqlCompiler.compiler.frontend.statements.IHasSchema;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPNullLiteral;
import org.dbsp.util.IJson;
import org.dbsp.util.Utilities;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Set;

/** Represents metadata about the compiled program.
 * Contains a description of all input tables and all views,
 * and the values of all variables set by SQL SET statements. */
public class ProgramMetadata implements IJson {
    final LinkedHashMap<ProgramIdentifier, IHasSchema> inputTables;
    final LinkedHashMap<ProgramIdentifier, IHasSchema> outputViews;
    /** The variables created by SET statements with their values. */
    final LinkedHashMap<String, DBSPExpression> variables;

    public ProgramMetadata() {
        this.inputTables = new LinkedHashMap<>();
        this.outputViews = new LinkedHashMap<>();
        this.variables = new LinkedHashMap<>();
    }

    private static String canonicalVariableName(String variable) {
        return variable.toLowerCase(Locale.ENGLISH);
    }

    static final Set<String> reserved = Set.of(DBSPCompiler.WARNINGS_ARE_ERRORS.toLowerCase(Locale.ENGLISH));

    private boolean known(String variable) {
        if (reserved.contains(variable))
            return true;
        if (variable.startsWith("feldera_ignore_warning_"))
            return true;
        return false;
    }

    public void set(String variable, DBSPExpression value,
                    SourcePositionRange range, DBSPCompiler compiler) {
        variable = canonicalVariableName(variable);
        if (this.variables.containsKey(variable)) {
            DBSPExpression expression = this.variables.get(variable);
            String str = ToSqlVisitor.convert(compiler, expression);
            compiler.reportWarning(range,
                    "Overwriting value", "Variable " + Utilities.singleQuote(variable) +
                            " is already set to " + str);
        }
        if (!this.known(variable)) {
            compiler.reportWarning(range,
                    "Unknown setting", "Variable " + Utilities.singleQuote(variable) +
                            " does not control any known settings");
        }
        this.variables.put(variable, value);
    }

    public boolean hasValue(String variable) {
        variable = canonicalVariableName(variable);
        return this.variables.containsKey(variable);
    }

    /** Return true if the value of the supplied variable is falsy
     * (OFF, false, NULL, or zero).  The variable must have a value. */
    public boolean isFalsy(String variable) {
        variable = canonicalVariableName(variable);
        DBSPExpression expression = Utilities.getExists(this.variables, variable);
        if (expression.is(DBSPIntLiteral.class)) {
            BigInteger value = expression.to(DBSPIntLiteral.class).getValue();
            if (value == null) return true;
            return value.equals(BigInteger.ZERO);
        } else if (expression.is(DBSPBoolLiteral.class)) {
            Boolean value = expression.to(DBSPBoolLiteral.class).value;
            if (value == null) return true;
            return !value;
        } else {
            return expression.is(DBSPNullLiteral.class);
        }
    }

    public ObjectNode asJson() {
        ObjectMapper mapper = Utilities.deterministicObjectMapper();
        ArrayNode inputs = mapper.createArrayNode();
        for (IHasSchema input: this.inputTables.values()) {
            if (input.is(DeclareViewStatement.class))
                continue;
            inputs.add(input.asJson(false));
        }
        ArrayNode outputs = mapper.createArrayNode();
        for (IHasSchema output: this.outputViews.values())
            outputs.add(output.asJson(false));
        ObjectNode ios = mapper.createObjectNode();
        ios.set("inputs", inputs);
        ios.set("outputs", outputs);
        return ios;
    }

    public IHasSchema getTableDescription(ProgramIdentifier name) {
        return Utilities.getExists(this.inputTables, name);
    }

    public boolean hasTable(ProgramIdentifier name) {
        return this.inputTables.containsKey(name);
    }

    public boolean hasView(ProgramIdentifier name) {
        return this.outputViews.containsKey(name);
    }

    public IHasSchema getViewDescription(ProgramIdentifier name) {
        return Utilities.getExists(this.outputViews, name);
    }

    public void addTable(IHasSchema description) {
        this.inputTables.put(description.getName(), description);
    }

    public void removeTable(ProgramIdentifier name) {
        this.inputTables.remove(name);
    }

    public void addView(IHasSchema description) {
        this.outputViews.put(description.getName(), description);
    }

    public static ProgramMetadata fromJson(JsonNode node, RelDataTypeFactory typeFactory) {
        ProgramMetadata result = new ProgramMetadata();
        var it = Utilities.getProperty(node, "inputs").elements();
        while (it.hasNext()) {
            JsonNode tbl = it.next();
            IHasSchema sch = IHasSchema.AbstractIHasSchema.fromJson(tbl, typeFactory);
            result.addTable(sch);
        }
        it = Utilities.getProperty(node, "outputs").elements();
        while (it.hasNext()) {
            JsonNode tbl = it.next();
            IHasSchema sch = IHasSchema.AbstractIHasSchema.fromJson(tbl, typeFactory);
            result.addView(sch);
        }
        return result;
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        IJson.toJsonStream(this.asJson(), visitor.stream);
    }
}
