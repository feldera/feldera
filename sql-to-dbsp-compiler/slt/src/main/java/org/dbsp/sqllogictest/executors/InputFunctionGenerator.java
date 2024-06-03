package org.dbsp.sqllogictest.executors;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqllogictest.Main;
import org.dbsp.util.Linq;
import org.dbsp.util.TableValue;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/**
 * Helper class for the DbspJdbcExecutor.
 * Generate on demand the input functions.
 * We do this so that we can build the circuit first in a deterministic
 * way for debugging.
 */
class InputFunctionGenerator {
    final DBSPCompiler compiler;
    final InputGenerator inputGenerator;
    final String connectionString;
    @Nullable
    private DBSPFunction inputFunction = null;

    InputFunctionGenerator(DBSPCompiler compiler, InputGenerator inputGenerator, String connectionString) {
        this.compiler = compiler;
        this.inputGenerator = inputGenerator;
        this.connectionString = connectionString;
    }

    private DBSPExpression generateReadDbCall(TableValue tableValue) {
        // Generates a read_table(<conn>, <table_name>, <mapper from |AnyRow| -> Tuple type>) invocation
        DBSPTypeUser sqliteRowType = new DBSPTypeUser(CalciteObject.EMPTY, USER, "AnyRow", false);
        DBSPVariablePath rowVariable = new DBSPVariablePath("row", sqliteRowType.ref());
        DBSPTypeTuple tupleType = tableValue.contents.elementType.to(DBSPTypeTuple.class);
        final List<DBSPExpression> rowGets = new ArrayList<>(tupleType.tupFields.length);
        for (int i = 0; i < tupleType.tupFields.length; i++) {
            DBSPApplyMethodExpression rowGet =
                    new DBSPApplyMethodExpression("get",
                            tupleType.tupFields[i],
                            rowVariable.deref(), new DBSPUSizeLiteral(i));
            rowGets.add(rowGet);
        }
        DBSPTupleExpression tuple = new DBSPTupleExpression(rowGets, false);
        DBSPClosureExpression mapClosure = new DBSPClosureExpression(CalciteObject.EMPTY, tuple,
                rowVariable.asParameter());
        return new DBSPApplyExpression("read_db", tableValue.contents.getType(),
                new DBSPStrLiteral(this.connectionString), new DBSPStrLiteral(tableValue.tableName),
                mapClosure);
    }

    DBSPFunction createInputFunction() throws IOException, SQLException {
        TableValue[] inputSets = this.inputGenerator.getInputs();
        DBSPExpression[] fields = new DBSPExpression[inputSets.length];
        int totalSize = 0;
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < inputSets.length; i++) {
            totalSize += inputSets[i].contents.size();
            fields[i] = inputSets[i].contents;
            if (seen.contains(inputSets[i].tableName))
                throw new RuntimeException("Table " + inputSets[i].tableName + " already in input");
            seen.add(inputSets[i].tableName);
        }

        if (totalSize > 10) {
            if (this.connectionString.equals("csv")) {
                // If the data is large write, it to a set of CSV files and read it at runtime.
                for (int i = 0; i < inputSets.length; i++) {
                    String fileName = (Main.rustDirectory + inputSets[i].tableName) + ".csv";
                    File file = new File(fileName);
                    ToCsvVisitor.toCsv(compiler, file, inputSets[i].contents);
                    fields[i] = new DBSPApplyExpression(CalciteObject.EMPTY, "read_csv",
                            inputSets[i].contents.getType(),
                            new DBSPStrLiteral(fileName));
                }
            } else {
                // read from DB
                for (int i = 0; i < inputSets.length; i++) {
                    fields[i] = generateReadDbCall(inputSets[i]);
                }
            }
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction("input", new ArrayList<>(),
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
