package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.commons.math3.util.Pair;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Analyzes the fields of each stream that correspond to primary
 * keys or foreign keys in input streams. */
public class KeyPropagation extends CircuitVisitor {
    static class PrimaryKeyField {
        final DBSPSourceTableOperator table;
        final int tableFieldIndex;

        PrimaryKeyField(DBSPSourceTableOperator table, int tableFieldIndex) {
            this.table = table;
            this.tableFieldIndex = tableFieldIndex;
        }

        @Override
        public String toString() {
            return this.table.tableName + ":" + this.tableFieldIndex;
        }
    }

    /** Field belongs to a foreign key pointing to some primary key */
    static class ForeignKeyField extends PrimaryKeyField {
        final DBSPSourceTableOperator fkTable;
        final int fkTableFieldIndex;

        ForeignKeyField(
                DBSPSourceTableOperator source,
                int tableKeyIndex,
                DBSPSourceTableOperator fkTable,
                int fkTableFieldIndex) {
            // Super points to the actual key
            super(source, tableKeyIndex);
            this.fkTable = fkTable;
            this.fkTableFieldIndex = fkTableFieldIndex;
        }

        @Override
        public String toString() {
            return this.fkTable.tableName + ":" + this.fkTableFieldIndex + "->" + super.toString();
        }
    }

    /** Properties of one output field */
    static class FieldProperties {
        /** Primary key that this is part of, or null */
        @Nullable PrimaryKeyField keyField;
        /** Foreign keys that this is part of */
        final List<ForeignKeyField> fkFields;

        FieldProperties() {
            this.keyField = null;
            this.fkFields = new ArrayList<>();
        }

        void setPrimaryKey(PrimaryKeyField kf) {
            this.keyField = kf;
        }

        void addForeignKey(ForeignKeyField fk) {
            this.fkFields.add(fk);
        }

        boolean isEmpty() {
            return this.keyField == null && this.fkFields.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (this.keyField != null) {
                builder.append(this.keyField);
            }
            for (ForeignKeyField fk: this.fkFields) {
                builder.append(" ").append(fk);
            }
            if (this.isEmpty()) {
                builder.append("-");
            }
            return builder.toString();
        }
    }

    static class StreamDescription {
        /** One property for each output field */
        final List<FieldProperties> properties;

        StreamDescription() {
            this.properties = new ArrayList<>();
        }

        void addProperties(FieldProperties fp) {
            this.properties.add(fp);
        }

        @Override
        public String toString() {
            return this.properties.toString();
        }

        public FieldProperties get(int columnIndex) {
            return this.properties.get(columnIndex);
        }

        public boolean hasSomeKey() {
            return Linq.any(this.properties, p -> !p.isEmpty());
        }
    }

    /** Maps each operator to a Tuple type that contains the key descriptions */
    final Map<DBSPOperator, StreamDescription> keys;

    public KeyPropagation(IErrorReporter errorReporter) {
        super(errorReporter);
        this.keys = new HashMap<>();
    }

    void processMap(DBSPUnaryOperator node) {
        StreamDescription inputKeys = this.keys.get(node.input());
        if (inputKeys == null) {
            super.postorder(node);
            return;
        }

        Projection projection = new Projection(this.errorReporter, true);
        projection.apply(node.getFunction());
        if (!projection.hasIoMap()) {
            super.postorder(node);
            return;
        }

        List<Pair<Integer, Integer>> ioMap = projection.getIoMap();
        StreamDescription result = new StreamDescription();
        for (Pair<Integer, Integer> source : ioMap) {
            assert source.getFirst() == 0;
            int sourceColumnIndex = source.getSecond();
            FieldProperties fieldProperties = inputKeys.get(sourceColumnIndex);
            result.addProperties(fieldProperties);
        }
        if (result.hasSomeKey()) {
            this.map(node, result);
        }
        super.postorder(node);
    }

    @Override
    public void postorder(DBSPMapIndexOperator node) {
        this.processMap(node);
    }

    @Override
    public void postorder(DBSPMapOperator node) {
        this.processMap(node);
    }

    @Override
    public void postorder(DBSPSourceTableOperator operator) {
        StreamDescription description = new StreamDescription();
        int index = 0;
        boolean found = false;
        for (InputColumnMetadata col: operator.metadata.getColumns()) {
            FieldProperties prop = new FieldProperties();
            if (col.isPrimaryKey) {
                prop.setPrimaryKey(new PrimaryKeyField(operator, index));
                found = true;
            }
            description.addProperties(prop);
            index++;
        }
        for (ForeignKey fk: operator.metadata.getForeignKeys()) {
            assert fk.thisTable.tableName.getString().equals(operator.tableName);
            DBSPSourceTableOperator other = this.getCircuit().getInput(fk.otherTable.tableName.getString());
            if (other == null)
                // This can happen for foreign keys that refer to tables that are not in the program.
                // This is a warning, but still a legal SQL program.
                continue;
            for (int i = 0; i < fk.thisTable.columns.size(); i++) {
                String thisColumn = fk.thisTable.columns.get(i).getString();
                String otherColumn = fk.otherTable.columns.get(i).getString();
                int thisColumnIndex = operator.metadata.getColumnIndex(thisColumn);
                int otherColumnIndex = other.metadata.getColumnIndex(otherColumn);
                ForeignKeyField fkf = new ForeignKeyField(other, otherColumnIndex, operator, thisColumnIndex);
                description.get(thisColumnIndex).addForeignKey(fkf);
                found = true;
            }
        }
        if (found) {
            this.map(operator, description);
        }
        super.postorder(operator);
    }

    void map(DBSPOperator operator, StreamDescription keys) {
        Utilities.putNew(this.keys, operator, keys);
        Logger.INSTANCE.belowLevel(this, 1)
                .append(operator.getIdString())
                .append(" ")
                .append(operator.operation)
                .append(" ")
                .append(keys.toString())
                .newline();
    }

    void copy(DBSPUnaryOperator unary) {
        StreamDescription inputKeys = this.keys.get(unary.input());
        if (inputKeys != null)
            this.map(unary, inputKeys);
        super.postorder(unary);
    }

    @Override
    public void postorder(DBSPFilterOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPNoopOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPDelayedIntegralOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPIntegrateOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPNegateOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPDistinctOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPStreamDistinctOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPViewOperator source) {
        this.copy(source);
    }

    @Override
    public void postorder(DBSPSinkOperator source) {
        this.copy(source);
    }
}
