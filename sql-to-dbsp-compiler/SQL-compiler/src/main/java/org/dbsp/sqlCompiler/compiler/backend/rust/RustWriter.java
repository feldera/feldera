package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

/** Base class for writing code to Rust files */
public abstract class RustWriter extends BaseCodeGenerator {
    public RustWriter() {}

    /** Various visitors gather here information about the program prior to generating code. */
    public static class StructuresUsed {
        /** The set of all tuple sizes used in the program. */
        final Set<Integer> tupleSizesUsed = new HashSet<>();
        /** The set of all semigroup sizes used. */
        final Set<Integer> semigroupSizesUsed = new HashSet<>();

        int getMaxTupleSize() {
            int max = 0;
            for (int s: this.tupleSizesUsed)
                if (s > max)
                    max = s;
            return max;
        }
    }

    /** Visitor which discovers some data structures used.
     * Stores the result in the "used" structure. */
    static class FindResources extends InnerVisitor {
        final StructuresUsed used;

        public FindResources(DBSPCompiler compiler, StructuresUsed used) {
            super(compiler);
            this.used = used;
        }

        @Override
        public void postorder(DBSPTypeTuple type) {
            this.used.tupleSizesUsed.add(type.size());
        }

        @Override
        public void postorder(DBSPTypeStruct type) {
            this.used.tupleSizesUsed.add(type.fields.size());
        }

        @Override
        public void postorder(DBSPTypeSemigroup type) {
            this.used.semigroupSizesUsed.add(type.semigroupSize());
        }
    }

    /** Preamble used when generating Rust code. */
    String rustPreamble() {
        return STANDARD_PREAMBLE;
    }

    /** Generate TupN[T0, T1, ...] */
    String tup(int count) {
        StringBuilder builder = new StringBuilder();
        builder.append(DBSPTypeCode.TUPLE.rustName)
                .append(count)
                .append("<");
        for (int j = 0; j < count; j++) {
            if (j > 0)
                builder.append(", ");
            builder.append("T")
                    .append(j);
        }
        builder.append(">");
        return builder.toString();
    }

    void generateStructures(StructuresUsed used, IndentStream stream) {
        /*
        #[derive(Clone)]
        pub struct Semigroup2<T0, T1, TS0, TS1>(PhantomData<(T0, T1, TS0, TS1)>);

        impl<T0, T1, TS0, TS1> Semigroup<Tuple2<T0, T1>> for Semigroup2<T0, T1, TS0, TS1>
        where
            TS0: Semigroup<T0>,
            TS1: Semigroup<T1>,
        {
            fn combine(left: &Tuple2<T0, T1>, right: &Tuple2<T0, T1>) -> Tuple2<T0, T1> {
                Tuple2::new(
                    TS0::combine(&left.0, &right.0),
                    TS1::combine(&left.1, &right.1),
                )
            }
        }
         */
        for (int i: used.semigroupSizesUsed) {
            Integer[] indexes = new Integer[i];
            IntStream.range(0, i).forEach(ix -> indexes[ix] = ix);
            String[] ts = Linq.map(indexes, ix -> "T" + ix, String.class);
            String[] tts = Linq.map(indexes, ix -> "TS" + ix, String.class);

            stream.append("#[derive(Clone)]").newline()
                    .append("pub struct Semigroup")
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append(">(PhantomData<(")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append(")>);")
                    .newline()
                    .newline();

            stream.append("impl<")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append("> Semigroup")
                    .append("<")
                    .append(DBSPTypeCode.TUPLE.rustName)
                    .append(i)
                    .append("<")
                    .intercalate(", ", indexes, ix -> "T" + ix)
                    .append(">> for Semigroup")
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append(">")
                    .newline()
                    .append("where").increase()
                    .join(",\n", indexes, ix -> "TS" + ix + ": Semigroup<T" + ix + ">")
                    .newline().decrease()
                    .append("{").increase()
                    .append("fn combine(left: &")
                    .append(DBSPTypeCode.TUPLE.rustName)
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .append(">, right:&")
                    .append(DBSPTypeCode.TUPLE.rustName)
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .append(">) -> ")
                    .append(DBSPTypeCode.TUPLE.rustName)
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .append("> {").increase()
                    .append(DBSPTypeCode.TUPLE.rustName)
                    .append(i)
                    .append("::new(").increase()
                    .join("\n", indexes, ix -> "TS" + ix + "::combine(&left." + ix + ", &right." + ix + "),")
                    .newline().decrease()
                    .append(")").newline()
                    .decrease()
                    .append("}").newline()
                    .decrease()
                    .append("}").newline();
        }

        if (!used.tupleSizesUsed.isEmpty()) {
            stream.append("declare_tuples! {").increase();
            for (int i : used.tupleSizesUsed) {
                if (i <= 10)
                    // These are already pre-declared
                    continue;
                stream.append(this.tup(i));
                stream.append(",\n");
            }
            stream.decrease().append("}\n\n");

            for (int i : used.tupleSizesUsed) {
                if (i <= 10)
                    // These are already pre-declared
                    continue;
                stream.append("feldera_types::deserialize_without_context!(");
                stream.append(DBSPTypeCode.TUPLE.rustName)
                        .append(i);
                for (int j = 0; j < i; j++) {
                    stream.append(", ");
                    stream.append("T")
                            .append(j);
                }
                stream.append(");\n");
            }
        }
        stream.append("\n");
    }

    String generateUdfInclude() {
        IndentStream stream = new IndentStream(new StringBuilder());
        String stubs = Utilities.getBaseName(DBSPCompiler.STUBS_FILE_NAME);
        stream.append("mod ")
                .append(stubs)
                .append(";")
                .newline()
                .append("mod ")
                .append(Utilities.getBaseName(DBSPCompiler.UDF_FILE_NAME))
                .append(";")
                .newline()
                .append("use crate::")
                .append(stubs)
                .append("::*;")
                .newline();
        return stream.toString();
    }

    public StructuresUsed analyze(DBSPCompiler compiler) {
        StructuresUsed used = new StructuresUsed();
        FindResources findResources = new FindResources(compiler, used);
        CircuitRewriter findCircuitResources = findResources.getCircuitVisitor(true);

        for (IDBSPNode node : this.toWrite) {
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                inner.accept(findResources);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                // Find the resources used to generate the correct Rust preamble
                findCircuitResources.apply(outer);
            }
        }
        return used;
    }
}
