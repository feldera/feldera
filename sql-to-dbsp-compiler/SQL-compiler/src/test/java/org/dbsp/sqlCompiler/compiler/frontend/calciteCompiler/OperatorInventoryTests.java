package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CalciteFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.FunctionDocumentation;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SpatialOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.dbsp.util.Utilities;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.ArrayList;

/** Optional inventory of the actual validator table, including STANDARD and spatial operators. */
public class OperatorInventoryTests {
    @Test
    public void exportValidatorOperators() throws IOException {
        String output = System.getProperty("feldera.operator.inventory", "");
        if (output.isBlank())
            return;
        var compiler = new DBSPCompiler(new CompilerOptions());
        var operators = compiler.getSqlToRelCompiler().createOperatorTable().getOperatorList();
        var standard = SqlStdOperatorTable.instance().getOperatorList();
        var spatial = SpatialOperatorTable.instance().getOperatorList();
        var documented = new ArrayList<FunctionDocumentation.FunctionDescription>();
        documented.addAll(CalciteFunctions.INSTANCE.getDescriptions());
        documented.addAll(new CustomFunctions().getDescriptions());
        var mapper = Utilities.deterministicObjectMapper();
        var root = mapper.createObjectNode();
        root.put("schema_version", 1);
        root.put("lowering_support_verified", false);
        var rows = root.putArray("operators");
        operators.stream().sorted(Comparator.comparing(op -> op.getName() + "/" + op.getKind()
                + "/" + op.getSyntax() + "/" + op.getClass().getName())).forEach(op -> {
            var row = rows.addObject();
            row.put("name", op.getName());
            row.put("kind", op.getKind().name());
            row.put("syntax", op.getSyntax().name());
            row.put("implementation", op.getClass().getName());
            row.put("deterministic", op.isDeterministic());
            row.put("dynamic", op.isDynamicFunction());
            row.put("aggregate", op.isAggregator());
            row.put("standard_instance", standard.stream().anyMatch(candidate -> candidate == op));
            row.put("spatial_instance", spatial.stream().anyMatch(candidate -> candidate == op));
            var documentation = row.putArray("documentation_by_name");
            documented.stream().filter(description -> description.functionName().equalsIgnoreCase(op.getName()))
                    .map(FunctionDocumentation.FunctionDescription::documentation).distinct().sorted()
                    .forEach(documentation::add);
        });
        Files.writeString(Path.of(output), mapper.writeValueAsString(root), StandardOpenOption.CREATE_NEW);
    }
}
