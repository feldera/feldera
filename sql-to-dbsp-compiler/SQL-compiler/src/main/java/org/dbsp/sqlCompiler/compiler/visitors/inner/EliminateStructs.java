package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;

import java.util.ArrayList;
import java.util.List;

/** Convert every occurrence of a TypeStruct to a TypeTuple */
public class EliminateStructs extends InnerRewriteVisitor {
    public EliminateStructs(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public VisitDecision preorder(DBSPTypeStruct type) {
        this.push(type);
        List<DBSPType> fields = new ArrayList<>();
        for (DBSPTypeStruct.Field f: type.fields.values()) {
            DBSPType fType = this.transform(f.type);
            fields.add(fType);
        }
        this.pop(type);
        DBSPType result = new DBSPTypeTuple(type.getNode(), type.mayBeNull, fields);
        this.map(type, result);
        return VisitDecision.STOP;
    }
}
