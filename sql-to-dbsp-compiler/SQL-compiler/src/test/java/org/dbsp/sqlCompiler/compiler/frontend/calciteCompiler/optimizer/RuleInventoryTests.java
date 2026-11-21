package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Utilities;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolve journal rule descriptions to their actual implementation classes. */
public class RuleInventoryTests {
    /** Exercise the current shape guards without applying any optimizer rules. */
    private static Map<String, RelNode> shapes(RelBuilder builder) {
        Map<String, RelNode> shapes = new LinkedHashMap<>();
        RelNode values = builder.values(new String[] {"i"}, 1).build();
        shapes.put("values", values);
        RelNode joins = values;
        for (int index = 0; index < 3; index++)
            joins = LogicalJoin.create(joins, values, List.of(),
                    builder.literal(true), Set.of(), JoinRelType.INNER);
        shapes.put("three_inner_joins", joins);
        var factory = builder.getTypeFactory();
        var row = factory.builder().add("a",
                factory.createArrayType(factory.createSqlType(SqlTypeName.INTEGER), -1)).build();
        var arrays = builder.values(row).build();
        shapes.put("uncollect", Uncollect.create(arrays.getTraitSet(), arrays, false, List.of()));
        return shapes;
    }

    @Test
    public void exportConfiguredRules() throws IOException {
        String output = System.getProperty("feldera.rule.inventory", "");
        if (output.isBlank())
            return;
        var compiler = new DBSPCompiler(new CompilerOptions());
        var builder = RelBuilder.create(Frameworks.newConfigBuilder().build());
        var mapper = Utilities.deterministicObjectMapper();
        var root = mapper.createObjectNode();
        root.put("schema_version", 1);
        var steps = root.putArray("steps");
        for (int level = 0; level <= 2; level++) {
            for (var shape: shapes(builder).entrySet()) {
                var optimizer = new CalciteOptimizer(level, builder, compiler);
                for (int index = 0; index < optimizer.steps.size(); index++) {
                    var step = optimizer.steps.get(index);
                    var row = steps.addObject();
                    row.put("optimization_level", level);
                    row.put("shape", shape.getKey());
                    row.put("index", index);
                    row.put("step", step.getName());
                    row.put("implementation", step.getClass().getName());
                    row.put("abort_on_error", step.abortOnError());
                    var rules = row.putArray("rules");
                    if (step instanceof CalciteOptimizer.HepOptimizerStep hep) {
                        row.put("kind", "hep");
                        // Conditional steps register rules only when constructing
                        // their program, rather than in the optimizer constructor.
                        hep.getProgram(shape.getValue(), level);
                        for (var rule: hep.configuredRules(level)) {
                            var entry = rules.addObject();
                            entry.put("description", rule.toString());
                            entry.put("implementation", rule.getClass().getName());
                        }
                    } else {
                        row.put("kind", "direct");
                    }
                }
            }
        }
        Files.writeString(Path.of(output), mapper.writeValueAsString(root), StandardOpenOption.CREATE_NEW);
    }
}
