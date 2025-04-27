package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDelayedIntegralOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinFilterMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPUnaryOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.InputColumnMetadata;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ForeignKey;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Projection;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    static final class ForeignKeyField extends PrimaryKeyField {
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
    static final class FieldProperties {
        /** Primary key that this is part of, or null */
        @Nullable PrimaryKeyField keyField;
        /** Foreign keys that this is part of */
        final List<ForeignKeyField> fkFields;

        FieldProperties() {
            this.keyField = null;
            this.fkFields = new ArrayList<>();
        }

        FieldProperties(@Nullable PrimaryKeyField pk, List<ForeignKeyField> fk) {
            this.keyField = pk;
            this.fkFields = fk;
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

        FieldProperties getForeignKeys() {
            return new FieldProperties(null, this.fkFields);
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
        /** One property for each output field.  For streams that represent
         * indexed Z-sets the fields are flattened. */
        final List<FieldProperties> properties;

        StreamDescription() {
            this.properties = new ArrayList<>();
        }

        protected StreamDescription(List<FieldProperties> fields) {
            this.properties = fields;
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

        StreamDescription prefix(int count) {
            return new StreamDescription(this.properties.subList(0, count));
        }

        public StreamDescription tail(int index) {
            return new StreamDescription(this.properties.subList(index, this.properties.size()));
        }
    }

    /**
     * Description of joins that join a primary key with a foreign key
     *
     * @param join      Join operator.
     * @param table     Table whose primary key is joined on
     * @param leftIsKey If true the LHS of the join uses the key
     */
    public record JoinDescription(DBSPSimpleOperator join, DBSPSourceTableOperator table, boolean leftIsKey) {
        @Override
        public String toString() {
            return "JoinDescription{" +
                    "join=" + join +
                    ", table=" + table +
                    ", leftIsKey=" + leftIsKey +
                    '}';
        }
    }

    /** Maps each operator to a StreamDescription for its output */
    final Map<DBSPSimpleOperator, StreamDescription> keys;
    /** Maps each join that operates on a primary/foreign key to its description */
    public final Map<DBSPSimpleOperator, JoinDescription> joins;

    public KeyPropagation(DBSPCompiler compiler) {
        super(compiler);
        this.keys = new HashMap<>();
        this.joins = new HashMap<>();
    }

    void processMap(DBSPUnaryOperator node) {
        if (!node.input().isSimpleNode()) {
            super.postorder(node);
            return;
        }
        StreamDescription inputKeys = this.keys.get(node.input().simpleNode());
        if (inputKeys == null) {
            super.postorder(node);
            return;
        }

        Projection projection = new Projection(this.compiler(), true);
        projection.apply(node.getFunction());
        if (!projection.hasIoMap()) {
            super.postorder(node);
            return;
        }

        Projection.IOMap ioMap = projection.getIoMap();
        StreamDescription result = new StreamDescription();
        for (int sourceColumnIndex: ioMap.getFieldsOfInput(0)) {
            FieldProperties fieldProperties = inputKeys.get(sourceColumnIndex);
            result.addProperties(fieldProperties);
        }
        if (result.hasSomeKey()) {
            this.map(node, result);
        }
        super.postorder(node);
    }

    /** Source table whose keys are fully included in the specified stream, or
     * null if no such source table exists. */
    @Nullable
    DBSPSourceTableOperator hasKeys(StreamDescription description) {
        Set<Integer> keyFieldsFound = new HashSet<>();
        DBSPSourceTableOperator source = null;
        for (FieldProperties field: description.properties) {
            if (field.keyField != null) {
                if (source == null)
                    source = field.keyField.table;
                else
                    // There is no way a stream could contain keys from multiple tables
                    Utilities.enforce((source == field.keyField.table));
                keyFieldsFound.add(field.keyField.tableFieldIndex);
            }
        }

        if (source == null)
            return null;

        // If the number of fields found is the same as the key size
        // we must have found all of them
        if (keyFieldsFound.size() == source.metadata.getPrimaryKeys().size())
            return source;
        return null;
    }

    /** Check if desc contains a foreign key that contains all fields of the key
     * of table source.
     * @param source  Table whose key we are looking for.
     * @param desc    Description of a stream where we look for foreign key fields.
     * @return        The table whose key is joined, or null of no such table exists. */
    @Nullable
    DBSPSourceTableOperator checkForeignKeys(DBSPSourceTableOperator source, StreamDescription desc) {
        Set<Integer> keyFieldsFound = new HashSet<>();
        for (FieldProperties field: desc.properties) {
            for (ForeignKeyField fk: field.fkFields) {
                if (fk.table != source)
                    continue;
                keyFieldsFound.add(fk.tableFieldIndex);
            }
        }
        // If the number of fields found is the same as the key size
        // we must have found all of them
        if (keyFieldsFound.size() == source.metadata.getPrimaryKeys().size())
            return source;
        return null;
    }

    /** Check if the desc0 contains all the fields of a key,
     * and desc1 contains all the fields of a foreign key pointing to it. */
    @Nullable
    DBSPSourceTableOperator checkForeign(StreamDescription desc0, StreamDescription desc1) {
        DBSPSourceTableOperator keys = this.hasKeys(desc0);
        if (keys == null) {
            return null;
        }

        return this.checkForeignKeys(keys, desc1);
    }

    /** Add foreign key information for a join.
     *
     * @param operator        Join that is being processed.
     * @param table           Table whose primary key is being joined
     * @param foreignKeyIndex Description of the index part of the foreign key input
     * @param foreignKey      Description of the data part of the foreign key input
     * @param keyOnLeft       True if the key is the left input of the join
     */
    void mapJoin(DBSPSimpleOperator operator, DBSPSourceTableOperator table,
                 StreamDescription foreignKeyIndex, StreamDescription foreignKey,
                 boolean keyOnLeft) {
        JoinDescription desc = new JoinDescription(operator, table, keyOnLeft);
        Logger.INSTANCE.belowLevel(this, 1)
                        .appendSupplier(desc::toString)
                        .newline();
        Utilities.putNew(this.joins, operator, desc);

        // In addition, the foreign key information is propagated through joins
        // (but not the primary key information).
        Projection projection = new Projection(this.compiler(), true);
        projection.apply(operator.getFunction());
        if (projection.hasIoMap()) {
            Projection.IOMap ioMap = projection.getIoMap();
            StreamDescription result = new StreamDescription();
            for (Projection.InputAndFieldIndex source : ioMap.fields()) {
                int input = source.inputIndex();
                int sourceColumnIndex = source.fieldIndex();
                FieldProperties props = switch (input) {
                    case 0 -> foreignKeyIndex.get(sourceColumnIndex);
                    case 1 -> keyOnLeft ? new FieldProperties() : foreignKey.get(sourceColumnIndex);
                    case 2 -> keyOnLeft ? foreignKey.get(sourceColumnIndex) : new FieldProperties();
                    default -> throw new IllegalStateException("Unexpected value: " + input);
                };
                result.addProperties(props.getForeignKeys());
            }
            if (result.hasSomeKey()) {
                this.map(operator, result);
            }
        }

    }

    void processJoin(DBSPJoinBaseOperator join) {
        if (!join.left().isSimpleNode() || !join.right().isSimpleNode()) {
            super.postorder(join);
            return;
        }
        StreamDescription left = this.keys.get(join.left().simpleNode());
        StreamDescription right = this.keys.get(join.right().simpleNode());
        if (left == null || right == null) {
            super.postorder(join);
            return;
        }

        int indexFields = join.left()
                .getOutputIndexedZSetType().getKeyTypeTuple()
                .tupFields.length;
        StreamDescription leftIndex = left.prefix(indexFields);
        StreamDescription rightIndex = right.prefix(indexFields);
        if (!leftIndex.hasSomeKey() || !rightIndex.hasSomeKey()) {
            super.postorder(join);
            return;
        }

        // See if the key is on the left and the fk on the right
        DBSPSourceTableOperator table = this.checkForeign(leftIndex, rightIndex);
        if (table != null) {
            this.mapJoin(join, table, rightIndex, right.tail(indexFields), true);
        } else {
            // See if the key is on the right and the fk on the left
            table = this.checkForeign(rightIndex, leftIndex);
            if (table != null) {
                this.mapJoin(join, table, leftIndex, left.tail(indexFields), false);
            }
        }

        super.postorder(join);
    }

    @Override
    public void postorder(DBSPJoinOperator node) {
        this.processJoin(node);
    }

    @Override
    public void postorder(DBSPStreamJoinOperator node) {
        this.processJoin(node);
    }

    @Override
    public void postorder(DBSPJoinFilterMapOperator node) {
        // The analysis assumes that there is no map function in the join
        Utilities.enforce(node.map == null);
        this.processJoin(node);
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
            Utilities.enforce(fk.thisTable.tableName.toIdentifier().equals(operator.tableName));
            DBSPSourceTableOperator other = this.getCircuit().getInput(fk.otherTable.tableName.toIdentifier());
            if (other == null)
                // This can happen for foreign keys that refer to tables that are not in the program.
                // This is a warning, but still a legal SQL program.
                continue;
            for (int i = 0; i < fk.thisTable.columnNames.size(); i++) {
                ProgramIdentifier thisColumn = fk.thisTable.columnNames.get(i).toIdentifier();
                ProgramIdentifier otherColumn = fk.otherTable.columnNames.get(i).toIdentifier();
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

    void map(DBSPSimpleOperator operator, StreamDescription keys) {
        Utilities.putNew(this.keys, operator, keys);
        Logger.INSTANCE.belowLevel(this, 1)
                .appendSupplier(operator::getIdString)
                .append(" ")
                .appendSupplier(() -> operator.operation)
                .append(" ")
                .appendSupplier(keys::toString)
                .newline();
    }

    void copy(DBSPUnaryOperator unary) {
        if (!unary.input().isSimpleNode()) {
            super.postorder(unary);
            return;
        }
        StreamDescription inputKeys = this.keys.get(unary.input().simpleNode());
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
