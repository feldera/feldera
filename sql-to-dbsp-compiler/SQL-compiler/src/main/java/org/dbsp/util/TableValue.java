package org.dbsp.util;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/** Represents the contents of a table as produced by INSERT and REMOVE statements */
public class TableValue {
    public final ProgramIdentifier tableName;
    public final DBSPZSetExpression contents;

    public TableValue(ProgramIdentifier tableName, DBSPZSetExpression contents) {
        this.tableName = tableName;
        this.contents = contents;
    }

    static final class HasDecimalOrDate extends InnerVisitor {
        public boolean found = false;

        public HasDecimalOrDate(DBSPCompiler compiler) {
            super(compiler);
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
     */
    public static DBSPFunction createInputFunction(DBSPCompiler compiler, String name,
            TableValue[] tables, String directory) throws IOException {
        DBSPExpression[] fields = new DBSPExpression[tables.length];
        Set<ProgramIdentifier> seen = new HashSet<>();
        for (int i = 0; i < tables.length; i++) {
            fields[i] = tables[i].contents;
            if (seen.contains(tables[i].tableName))
                throw new RuntimeException("Table " + tables[i].tableName + " already in input");
            seen.add(tables[i].tableName);

            if (tables[i].contents.size() < 100)
                continue;
            HasDecimalOrDate hd = new HasDecimalOrDate(compiler);
            hd.apply(tables[i].contents);
            if (hd.found)
                // Decimal values are not currently correctly serialized and deserialized.
                // Illegal Date values are deserialized incorrectly.
                continue;

            // If the data is large, write it to a CSV file and read it at runtime.
            String fileName = (Paths.get(directory, tables[i].tableName.name())) + ".csv";
            File file = new File(fileName);
            file.deleteOnExit();
            ToCsvVisitor.toCsv(compiler, file, tables[i].contents);
            fields[i] = new DBSPApplyExpression(CalciteObject.EMPTY, "read_csv",
                    tables[i].contents.getType(),
                    new DBSPStrLiteral(fileName, false));
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction(name, new ArrayList<>(),
                result.getType(), result, Linq.list());
    }
}
