package org.dbsp.sqllogictest.executors;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.TableData;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqllogictest.Main;
import org.dbsp.util.Linq;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class for the DbspJdbcExecutor.
 * Generate on demand the input functions.
 * We do this so that we can build the circuit first in a deterministic
 * way for debugging.
 */
class InputFunctionGenerator {
    final DBSPCompiler compiler;
    final InputGenerator inputGenerator;
    @Nullable
    private DBSPFunction inputFunction = null;

    InputFunctionGenerator(DBSPCompiler compiler, InputGenerator inputGenerator) {
        this.compiler = compiler;
        this.inputGenerator = inputGenerator;
    }

    DBSPFunction createInputFunction() throws IOException, SQLException {
        TableData[] inputSets = this.inputGenerator.getInputs();
        DBSPExpression[] fields = new DBSPExpression[inputSets.length];
        int totalSize = 0;
        Set<ProgramIdentifier> seen = new HashSet<>();
        for (int i = 0; i < inputSets.length; i++) {
            totalSize += inputSets[i].data().size();
            fields[i] = inputSets[i].data();
            if (seen.contains(inputSets[i].name()))
                throw new RuntimeException("Table " + inputSets[i].name() + " already in input");
            seen.add(inputSets[i].name());
        }

        if (totalSize > 10) {
            // If the data is large write, it to a set of CSV files and read it at runtime.
            for (int i = 0; i < inputSets.length; i++) {
                String fileName = (Main.getAbsoluteRustDirectory() + inputSets[i].name()) + ".csv";
                File file = new File(fileName);
                ToCsvVisitor.toCsv(compiler, file, inputSets[i].data());
                fields[i] = new DBSPApplyExpression(CalciteObject.EMPTY, "read_csv",
                        inputSets[i].data().getType(),
                        new DBSPStrLiteral("./src/" + inputSets[i].name() + ".csv"));
            }
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction(CalciteObject.EMPTY, "input", new ArrayList<>(),
                result.getType(), result, Linq.list());
    }

    void generate() throws IOException, SQLException {
        this.inputFunction = this.createInputFunction();
    }

    public DBSPFunction getInputFunction() throws IOException, SQLException {
        if (this.inputFunction == null)
            this.generate();
        return this.inputFunction;
    }
}
