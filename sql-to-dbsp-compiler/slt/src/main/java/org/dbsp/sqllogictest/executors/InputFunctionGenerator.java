package org.dbsp.sqllogictest.executors;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.path.DBSPPath;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqllogictest.Main;
import org.dbsp.util.Linq;
import org.dbsp.util.TableValue;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/**
 * Helper class for the DbspJdbcExecutor.
 * Generate on demand the input functions.
 * We do this so we can build the circuit first in a deterministic
 * way for debugging.
 */
class InputFunctionGenerator {
    final DBSPCompiler compiler;
    final InputGenerator inputGenerator;
    final String connectionString;
    @Nullable
    private DBSPFunction streamInputFunction = null;
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
        DBSPTypeTuple tupleType = tableValue.contents.zsetType.elementType.to(DBSPTypeTuple.class);
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
        return new DBSPApplyExpression("read_db", tableValue.contents.zsetType,
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

    /**
     * Example generated code for the function body:
     * let mut vec = Vec::new();
     * vec.push((data.0, zset!(), zset!(), zset!()));
     * vec.push((zset!(), data.1, zset!(), zset!()));
     * vec.push((zset!(), zset!(), data.2, zset!()));
     * vec.push((zset!(), zset!(), zset!(), data.3));
     * vec
     */
    DBSPFunction createStreamInputFunction(
            DBSPFunction inputGeneratingFunction) {
        DBSPTypeRawTuple inputType = Objects.requireNonNull(inputGeneratingFunction.returnType).to(DBSPTypeRawTuple.class);
        DBSPType returnType = new DBSPTypeVec(inputType, false);
        DBSPVariablePath vec = returnType.var("vec");
        DBSPLetStatement input = new DBSPLetStatement("data", inputGeneratingFunction.call());
        List<DBSPStatement> statements = new ArrayList<>();
        statements.add(input);
        DBSPLetStatement let = new DBSPLetStatement(vec.variable,
                new DBSPPath("Vec", "new").toExpression().call(), true);
        statements.add(let);
        if (this.compiler.options.languageOptions.incrementalize) {
            for (int i = 0; i < inputType.tupFields.length; i++) {
                DBSPExpression field = input.getVarReference().field(i);
                DBSPExpression elems = new DBSPApplyExpression("to_elements",
                        DBSPTypeAny.getDefault(), field.borrow());

                DBSPVariablePath e = DBSPTypeAny.getDefault().var("e");
                DBSPExpression[] fields = new DBSPExpression[inputType.tupFields.length];
                for (int j = 0; j < inputType.tupFields.length; j++) {
                    DBSPType fieldType = inputType.tupFields[j];
                    if (i == j) {
                        fields[j] = e.applyClone();
                    } else {
                        fields[j] = new DBSPApplyExpression("zset!", fieldType);
                    }
                }
                DBSPExpression projected = new DBSPRawTupleExpression(fields);
                DBSPExpression lambda = projected.closure(e.asParameter());
                DBSPExpression iter = new DBSPApplyMethodExpression(
                        "iter", DBSPTypeAny.getDefault(), elems);
                DBSPExpression map = new DBSPApplyMethodExpression(
                        "map", DBSPTypeAny.getDefault(), iter, lambda);
                DBSPExpression expr = new DBSPApplyMethodExpression(
                        "extend", new DBSPTypeVoid(), vec, map);
                DBSPStatement statement = new DBSPExpressionStatement(expr);
                statements.add(statement);
            }
            if (inputType.tupFields.length == 0) {
                // This case will cause no invocation of the circuit, but we need
                // at least one.
                DBSPExpression expr = new DBSPApplyMethodExpression(
                        "push", new DBSPTypeVoid(), vec, new DBSPRawTupleExpression());
                DBSPStatement statement = new DBSPExpressionStatement(expr);
                statements.add(statement);
            }
        } else {
            DBSPExpression expr = new DBSPApplyMethodExpression(
                    "push", new DBSPTypeVoid(), vec, input.getVarReference());
            DBSPStatement statement = new DBSPExpressionStatement(expr);
            statements.add(statement);
        }
        DBSPBlockExpression block = new DBSPBlockExpression(statements, vec);
        return new DBSPFunction("stream_input", Linq.list(), returnType, block, Linq.list());
    }

    void generate() throws IOException, SQLException {
        this.inputFunction = this.createInputFunction();
        this.streamInputFunction = this.createStreamInputFunction(this.inputFunction);
    }

    public DBSPFunction getStreamInputFunction() throws IOException, SQLException {
        if (this.streamInputFunction == null)
            this.generate();
        return this.streamInputFunction;
    }

    public DBSPFunction getInputFunction() throws IOException, SQLException {
        if (this.inputFunction == null)
            this.generate();
        return this.inputFunction;
    }
}
