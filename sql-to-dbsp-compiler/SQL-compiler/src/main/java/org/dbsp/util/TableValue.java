package org.dbsp.util;

import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeUser;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

public class TableValue {
    public final String tableName;
    public final DBSPZSetLiteral contents;

    public TableValue(String tableName, DBSPZSetLiteral contents) {
        this.tableName = tableName;
        this.contents = contents;
    }

    public DBSPExpression generateReadDbCall(String connectionString) {
        // Generates a read_table(<conn>, <table_name>, <mapper from |AnyRow| -> Tup type>) invocation
        DBSPTypeUser sqliteRowType = new DBSPTypeUser(CalciteObject.EMPTY, USER, "AnyRow", false);
        DBSPVariablePath rowVariable = new DBSPVariablePath("row", sqliteRowType.ref());
        DBSPTypeTuple tupleType = this.contents.elementType.to(DBSPTypeTuple.class);
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
        return new DBSPApplyExpression("read_db", this.contents.getType(),
                new DBSPStrLiteral(connectionString), new DBSPStrLiteral(this.tableName),
                mapClosure);
    }

    static final class HasDecimalOrDate extends InnerVisitor {
        public boolean found = false;

        public HasDecimalOrDate() {
            super(new StderrErrorReporter());
        }

        @Override
        public VisitDecision preorder(DBSPCastExpression node) {
            this.found = true;
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPTypeDate node) {
            this.found = true;
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPTypeTimestamp node) {
            this.found = true;
            return VisitDecision.STOP;
        }

        @Override
        public VisitDecision preorder(DBSPTypeDecimal node) {
            this.found = true;
            return VisitDecision.STOP;
        }
    }

    /**
     * Generate a Rust function which produces the inputs specified by 'tables'.
     * @param name    Name for the created function.
     * @param tables  Values that produce the input
     * @param directory  Directory where temporary files can be written
     * @param connectionString  Connection string that specified whether a database should be used for the data.
     *                          If the value is 'csv' temporary files may be used.
     */
    public static DBSPFunction createInputFunction(String name,
            TableValue[] tables, String directory, String connectionString) throws IOException {
        DBSPExpression[] fields = new DBSPExpression[tables.length];
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < tables.length; i++) {
            fields[i] = tables[i].contents;
            if (seen.contains(tables[i].tableName))
                throw new RuntimeException("Table " + tables[i].tableName + " already in input");
            seen.add(tables[i].tableName);

            if (tables[i].contents.size() < 100)
                continue;
            HasDecimalOrDate hd = new HasDecimalOrDate();
            hd.apply(tables[i].contents);
            if (hd.found)
                // Decimal values are not currently correctly serialized and deserialized.
                // Illegal Date values are deserialized incorrectly.
                continue;

            if (connectionString.equals("csv")) {
                // If the data is large, write it to a CSV file and read it at runtime.
                String fileName = (Paths.get(directory, tables[i].tableName)) + ".csv";
                File file = new File(fileName);
                file.deleteOnExit();
                ToCsvVisitor.toCsv(new StderrErrorReporter(), file, tables[i].contents);
                fields[i] = new DBSPApplyExpression(CalciteObject.EMPTY, "read_csv",
                        tables[i].contents.getType(),
                        new DBSPStrLiteral(fileName));
            } else {
                fields[i] = tables[i].generateReadDbCall(connectionString);
            }
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction(name, new ArrayList<>(),
                result.getType(), result, Linq.list());
    }
}
