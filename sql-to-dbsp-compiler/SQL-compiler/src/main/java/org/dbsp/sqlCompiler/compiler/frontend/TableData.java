package org.dbsp.sqlCompiler.compiler.frontend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.CreateRuntimeErrorWrappers;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPCastExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStrLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Linq;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** The data in a table.
 * @param name  Table name.
 * @param data  Actual data.
 * @param primaryKeys  List of column indexes which are part of the primary key.
 * */
public record TableData(ProgramIdentifier name, DBSPZSetExpression data, List<Integer> primaryKeys) {
    public TableData(ProgramIdentifier name, DBSPZSetExpression data) {
        this(name, data, List.of());
    }

    public TableData(String name, DBSPZSetExpression data) {
        this(new ProgramIdentifier(name), data, List.of());
    }

    public DBSPType getType() {
        return this.data.getType();
    }

    public boolean hasPrimaryKey() {
        return !this.primaryKeys.isEmpty();
    }

    public TableData transform(Function<IDBSPInnerNode, IDBSPInnerNode> transform) {
        DBSPZSetExpression result = transform.apply(this.data).to(DBSPZSetExpression.class);
        return new TableData(this.name, result, this.primaryKeys);
    }

    public TableData negate() {
        return new TableData(this.name, this.data.negate(), this.primaryKeys);
    }

    public TableData minus(DBSPZSetExpression data) {
        return new TableData(this.name, this.data.minus(data), this.primaryKeys);
    }

    public TableData add(TableData set) {
        DBSPZSetExpression result = this.data.clone();
        result.append(set.data);
        return new TableData(this.name, result, this.primaryKeys);
    }

    public DBSPClosureExpression keyFunction() {
        if (!this.hasPrimaryKey())
            throw new InternalCompilerError("No primary key");
        DBSPVariablePath var = this.data.elementType.ref().var();
        List<DBSPExpression> fields = Linq.map(this.primaryKeys, k -> var.deref().field(k).applyCloneIfNeeded());
        return new DBSPTupleExpression(fields, false).closure(var);
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
                                                   TableData[] tables, String directory) throws IOException {
        DBSPExpression[] fields = new DBSPExpression[tables.length];
        Set<ProgramIdentifier> seen = new HashSet<>();
        for (int i = 0; i < tables.length; i++) {
            fields[i] = tables[i].data;
            fields[i] = CreateRuntimeErrorWrappers.process(compiler, fields[i]);
            if (seen.contains(tables[i].name))
                throw new RuntimeException("Table " + tables[i].name + " already in input");
            seen.add(tables[i].name);

            if (tables[i].data.size() < 100)
                continue;
            HasDecimalOrDate hd = new HasDecimalOrDate(compiler);
            hd.apply(tables[i].data);
            if (hd.found)
                // Decimal values are not currently correctly serialized and deserialized.
                // Illegal Date values are deserialized incorrectly.
                continue;

            // If the data is large, write it to a CSV file and read it at runtime.
            String fileName = (Paths.get(directory, tables[i].name.name())) + ".csv";
            File file = new File(fileName);
            file.deleteOnExit();
            ToCsvVisitor.toCsv(compiler, file, tables[i].data);
            fields[i] = new DBSPApplyExpression(CalciteObject.EMPTY, "read_csv",
                    tables[i].data.getType(),
                    new DBSPStrLiteral(fileName, false));
        }
        DBSPRawTupleExpression result = new DBSPRawTupleExpression(fields);
        return new DBSPFunction(CalciteObject.EMPTY, name, new ArrayList<>(),
                result.getType(), result, Linq.list());
    }
}
