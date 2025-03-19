package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeSemigroup;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.Linq;
import org.dbsp.util.ProgramAndTester;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/** This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a compilable Rust file. */
public class RustFileWriter {
    final List<IDBSPNode> toWrite;
    final PrintStream outputStream;
    boolean slt = false;

    /** Various visitors gather here information about the program prior to generating code. */
    static class StructuresUsed {
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
    final StructuresUsed used = new StructuresUsed();

    /** Visitor which discovers some data structures used.
     * Stores the result in the "used" structure. */
    class FindResources extends InnerVisitor {
        public FindResources(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPTypeTuple type) {
            RustFileWriter.this.used.tupleSizesUsed.add(type.size());
        }

        @Override
        public void postorder(DBSPTypeStruct type) {
            RustFileWriter.this.used.tupleSizesUsed.add(type.fields.size());
        }

        @Override
        public void postorder(DBSPTypeSemigroup type) {
            RustFileWriter.this.used.semigroupSizesUsed.add(type.semigroupSize());
        }
    }

    /** Preamble used for all compilations. */
    static final String commonPreamble =
            """
            // Automatically-generated file
            #![allow(dead_code)]
            #![allow(non_snake_case)]
            #![allow(non_camel_case_types)]
            #![allow(unused_imports)]
            #![allow(unused_parens)]
            #![allow(unused_variables)]
            #![allow(unused_mut)]
            #![allow(unconditional_panic)]
            """;

    /** Preamble used when generating Rust code. */
    @SuppressWarnings("SpellCheckingInspection")
    String rustPreamble() {
        String preamble = """
                use dbsp::{
                    algebra::{ZSet, MulByRef, F32, F64, Semigroup, SemigroupValue, ZRingValue,
                         UnimplementedSemigroup, DefaultSemigroup, HasZero, AddByRef, NegByRef,
                         AddAssignByRef,
                    },
                    circuit::{checkpointer::Checkpoint, Circuit, CircuitConfig, Stream},
                    operator::{
                        Generator,
                        Fold,
                        group::WithCustomOrd,
                        time_series::{RelRange, RelOffset, OrdPartitionedIndexedZSet},
                        MaxSemigroup,
                        MinSemigroup,
                        CmpFunc,
                    },
                    OrdIndexedZSet, OrdZSet,
                    TypedBox,
                    utils::*,
                    zset,
                    indexed_zset,
                    DBWeight,
                    DBData,
                    DBSPHandle,
                    Error,
                    Runtime,
                    NumEntries,
                    MapHandle, ZSetHandle, OutputHandle,
                    dynamic::{DynData,DynDataTyped},
                };
                use rust_decimal_macros::dec;
                use feldera_types::program_schema::SqlIdentifier;
                use dbsp_adapters::Catalog;
                use feldera_types::{deserialize_table_record, serialize_table_record};
                use size_of::*;
                use ::serde::{Deserialize,Serialize};
                use compare::{Compare, Extract};
                use std::{
                    cell::LazyCell,
                    collections::BTreeMap,
                    convert::identity,
                    ops::Neg,
                    fmt::{Debug, Formatter, Result as FmtResult},
                    path::Path,
                    marker::PhantomData,
                    sync::{Arc, LazyLock},
                };
                use core::cmp::Ordering;
                use rust_decimal::Decimal;
                use dbsp::declare_tuples;
                use feldera_sqllib::{
                    *,
                    array::*,
                    casts::*,
                    binary::*,
                    decimal::*,
                    geopoint::*,
                    timestamp::*,
                    interval::*,
                    map::*,
                    string::*,
                    operators::*,
                    aggregates::*,
                    uuid::*,
                    variant::*,
                };

                #[cfg(not(target_env = "msvc"))]
                #[global_allocator]
                static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

                #[allow(non_upper_case_globals)]
                #[export_name = "malloc_conf"]
                pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\\0";
                #[cfg(test)]
                use readers::*;
                """;
        if (this.slt) {
            preamble += """
                #[cfg(test)]
                use sltsqlvalue::*;
                """;
        }
        return preamble;
    }


    public RustFileWriter(PrintStream outputStream) {
        this.toWrite = new ArrayList<>();
        this.outputStream = outputStream;
    }

    /** Special support for running the SLT tests */
    public RustFileWriter forSlt() {
        this.slt = true;
        return this;
    }

    public RustFileWriter(String outputFile)
            throws IOException {
        this(new PrintStream(outputFile, StandardCharsets.UTF_8));
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

        stream.append("declare_tuples! {").increase();
        for (int i: used.tupleSizesUsed) {
            if (i <= 10)
                // These are already pre-declared
                continue;
            stream.append(this.tup(i));
            stream.append(",\n");
        }
        stream.decrease().append("}\n\n");

        for (int i: used.tupleSizesUsed) {
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
        stream.append("\n");

        if (this.slt) {
            stream.append("#[cfg(test)]").newline()
                    .append("sltsqlvalue::to_sql_row_impl! {").increase();
            for (int i : used.tupleSizesUsed) {
                if (i <= 10)
                    // These are already pre-declared
                    continue;
                stream.append(this.tup(i));
                stream.append(",\n");
            }
            stream.decrease().append("}\n\n");
        }
    }

    String generatePreamble(StructuresUsed used) {
        IndentStream stream = new IndentStreamBuilder();
        stream.append(commonPreamble);
        long max = this.used.getMaxTupleSize();
        if (max > 120) {
            // this is just a guess
            stream.append("#![recursion_limit = \"")
                    .append(max * 2)
                    .append("\"]")
                    .newline();
        }

        stream.append("""
            #[cfg(test)]
            use hashing::*;""")
                .newline();
        stream.append(this.rustPreamble())
                .newline();
        this.generateStructures(used, stream);

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

    public void add(ProgramAndTester pt) {
        if (pt.program() != null)
            this.add(pt.program());
        this.add(pt.tester());
    }

    public void add(DBSPCircuit circuit) {
        this.toWrite.add(circuit);
    }

    public void add(DBSPFunction function) {
        this.toWrite.add(function);
    }

    public void write(DBSPCompiler compiler) {
        List<IDBSPNode> objects = new ArrayList<>();
        FindResources findResources = new FindResources(compiler);
        CircuitRewriter findCircuitResources = findResources.getCircuitVisitor(true);

        for (IDBSPNode node: this.toWrite) {
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                inner.accept(findResources);
                objects.add(inner);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                // Find the resources used to generate the correct Rust preamble
                outer = findCircuitResources.apply(outer);
                objects.add(outer);
            }
        }
        // Emit code
        this.outputStream.println(generatePreamble(used));
        for (IDBSPNode node: objects) {
            String str;
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                str = ToRustInnerVisitor.toRustString(compiler, inner, false);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                str = ToRustVisitor.toRustString(compiler, outer);
            }
            this.outputStream.println(str);
            this.outputStream.println();
        }
    }

    public void writeAndClose(DBSPCompiler compiler) {
        this.write(compiler);
        this.outputStream.close();
    }
}
