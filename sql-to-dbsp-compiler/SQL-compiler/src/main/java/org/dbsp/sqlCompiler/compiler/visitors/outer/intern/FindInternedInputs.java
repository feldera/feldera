package org.dbsp.sqlCompiler.compiler.visitors.outer.intern;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.util.Utilities;

import java.util.Locale;
import java.util.Map;

/** Discover fields in inputs that should be interned */
public class FindInternedInputs extends CircuitVisitor {
    final Map<DBSPSourceTableOperator, Intern.InternedColumnList> internedInputs;

    public FindInternedInputs(DBSPCompiler compiler, Map<DBSPSourceTableOperator, Intern.InternedColumnList> internedInputs) {
        super(compiler);
        this.internedInputs = internedInputs;
    }

    public void postorder(DBSPSourceTableOperator operator) {
        if (operator.tableName.name().toLowerCase(Locale.ENGLISH).startsWith("feldera"))
            return;
        Intern.InternedColumnList list = new Intern.InternedColumnList();
        int index = 0;
        for (var column: operator.metadata.getColumns()) {
            if (column.interned &&
                    column.type.code == DBSPTypeCode.STRING) {
                list.add(index);
            }
            index++;
        }
        if (!list.isEmpty()) {
            Utilities.putNew(this.internedInputs, operator, list);
        }
    }
}
