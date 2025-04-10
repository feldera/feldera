package org.dbsp.sqllogictest.executors;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqllogictest.Main;
import org.dbsp.util.Linq;
import org.dbsp.util.TableValue;

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
        TableValue[] inputSets = this.inputGenerator.getInputs();
        DBSPExpression[] fields = new DBSPExpression[inputSets.length];
        int totalSize = 0;
        Set<ProgramIdentifier> seen = new HashSet<>();
        for (int i = 0; i < inputSets.length; i++) {
            totalSize += inputSets[i].contents.size();
            fields[i] = inputSets[i].contents;
            if (seen.contains(inputSets[i].tableName))
                throw new RuntimeException("Table " + inputSets[i].tableName + " already in input");
            seen.add(inputSets[i].tableName);
        }

        if (totalSize > 10) {
            // If the data is large write, it to a set of CSV files and read it at runtime.
            for (int i = 0; i < inputSets.length; i++) {
                String fileName = (Main.getAbsoluteRustDirectory() + inputSets[i].tableName) + ".csv";
                File file = new File(fileName);
                ToCsvVisitor.toCsv(compiler, file, inputSets[i].contents);
                fields[i] = new DBSPApplyExpression(CalciteObject.EMPTY, "read_csv",
                        inputSets[i].contents.getType(),
                        new DBSPStrLiteral("./src/" + inputSets[i].tableName + ".csv"));
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
