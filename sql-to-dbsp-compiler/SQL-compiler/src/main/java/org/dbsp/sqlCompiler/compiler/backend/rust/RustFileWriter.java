package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.inner.SanitizeNames;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.compiler.ICompilerComponent;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.BetaReduction;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeSemigroup;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeWeight;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.ProgramAndTester;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.IntStream;

/**
 * This class helps generate Rust code.
 * It is given a set of circuit and functions and generates a compilable Rust file.
 */
public class RustFileWriter implements ICompilerComponent {
    final List<IDBSPNode> toWrite;
    final PrintStream outputStream;
    boolean emitHandles = false;

    /**
     * Various visitors gather here information about the program prior to generating code.
     */
    static class StructuresUsed {
        /**
         * The set of all tuple sizes used in the program.
         */
        final Set<Integer> tupleSizesUsed = new HashSet<>();
        /**
         * The set of all semigroup sizes used.
         */
        final Set<Integer> semigroupSizesUsed = new HashSet<>();
    }
    final StructuresUsed used = new StructuresUsed();

    /**
     * Visitor which discovers some data structures used.
     * Stores the result in the "used" structure.
     */
    class FindResources extends InnerVisitor {
        public FindResources(IErrorReporter reporter) {
            super(reporter);
        }

        @Override
        public void postorder(DBSPTypeTuple type) {
            RustFileWriter.this.used.tupleSizesUsed.add(type.size());
        }

        @Override
        public void postorder(DBSPTypeSemigroup type) {
            RustFileWriter.this.used.semigroupSizesUsed.add(type.semigroupSize());
        }
    }

    /**
     * If this is called with 'true' the emitted Rust code will use handles
     * instead of explicitly-typed ZSets.
     */
    public void emitCodeWithHandle(boolean emit) {
        this.emitHandles = emit;
    }

    /**
     * Preamble used for all compilations.
     */
    static final String commonPreamble =
            "// Automatically-generated file\n" +
            "#![allow(dead_code)]\n" +
            "#![allow(non_snake_case)]\n" +
            "#![allow(unused_imports)]\n" +
            "#![allow(unused_parens)]\n" +
            "#![allow(unused_variables)]\n" +
            "#![allow(unused_mut)]\n\n" +
            "#![allow(non_camel_case_types)]\n\n" +
            "#[cfg(test)]\n" +
            "use hashing::*;\n";  // comparison functions

    /**
     * Preamble used when generating Rust code.
     */
    @SuppressWarnings("SpellCheckingInspection")
    static final String rustPreamble =
            "use paste::paste;\n" +
            "use derive_more::{Add,Sub,Neg,From,Into,AddAssign};\n" +
            "use dbsp::{\n" +
            "    algebra::{ZSet, MulByRef, F32, F64, Semigroup, SemigroupValue, ZRingValue,\n" +
            "         UnimplementedSemigroup, DefaultSemigroup, HasZero, AddByRef, NegByRef,\n" +
            "         AddAssignByRef, Decimal,\n" +
            "    },\n" +
            "    circuit::{Circuit, Stream},\n" +
            "    operator::{\n" +
            "        Generator,\n" +
            "        FilterMap,\n" +
            "        Fold,\n" +
            "        time_series::{RelRange, RelOffset, OrdPartitionedIndexedZSet},\n" +
            "        MaxSemigroup,\n" +
            "        MinSemigroup,\n" +
            "    },\n" +
            "    trace::ord::{OrdIndexedZSet, OrdZSet},\n" +
            "    zset,\n" +
            "    indexed_zset,\n" +
            "    DBWeight,\n" +
            "    DBData,\n" +
            "    DBSPHandle,\n" +
            "    Runtime,\n" +
            "};\n" +
            "use dbsp_adapters::Catalog;\n" +
            "use size_of::*;\n" +
            "use bincode::{Decode, Encode};\n" +
            "use ::serde::{Deserialize,Serialize};\n" +
            "use compare::{Compare, Extract};\n" +
            "use std::{\n" +
            "    convert::identity,\n" +
            "    ops::{Add, Neg, AddAssign},\n" +
            "    fmt::{Debug, Formatter, Result as FmtResult},\n" +
            "    cell::RefCell,\n" +
            "    path::Path,\n" +
            "    rc::Rc,\n" +
            "    marker::PhantomData,\n" +
            "    str::FromStr,\n" +
            "};\n" +
            "use tuple::declare_tuples;\n" +
            "use sqllib::{\n" +
            "    *,\n" +
            "    casts::*,\n" +
            "    geopoint::*,\n" +
            "    timestamp::*,\n" +
            "    interval::*,\n" +
            "    string::*,\n" +
            "    operators::*,\n" +
            "};\n" +
            "#[cfg(test)]\n" +
            "use sqlvalue::*;\n" +
            "#[cfg(test)]\n" +
            "use readers::*;\n" +
            "#[cfg(test)]\n" +
            "use sqlx::{AnyConnection, any::AnyRow, Row};\n";

    /**
     * Preamble used when generating jit code.
     */
    static final String jitPreamble = "#[cfg(feature = \"jit\")]\n" +
            "use dataflow_jit::{\n" +
            "    sql_graph::SqlGraph,\n" +
            "    ir::{\n" +
            "        GraphExt,\n" +
            "        NodeId,\n" +
            "        Constant,\n" +
            "        literal::{\n" +
            "            StreamCollection::Set,\n" +
            "            NullableConstant,\n" +
            "            RowLiteral,\n" +
            "            NullableConstant::{Nullable, NonNull},\n" +
            "        },\n" +
            "    },\n" +
            "    facade::{DbspCircuit,Demands},\n" +
            "    codegen::CodegenConfig,\n" +
            "};\n" +
            "#[cfg(test)]\n" +
            "use sqllib::{\n" +
            "    *,\n" +
            "    casts::*,\n" +
            "    geopoint::*,\n" +
            "    timestamp::*,\n" +
            "    interval::*,\n" +
            "};\n" +
            "#[cfg(test)]\n" +
            "use chrono::{NaiveDate, NaiveDateTime};\n" +
            "#[cfg(test)]\n" +
            "use dbsp::algebra::Decimal;\n" +
            "#[cfg(test)]\n" +
            "use std::str::FromStr;\n"
            ;

    final DBSPCompiler compiler;

    public RustFileWriter(DBSPCompiler compiler, PrintStream outputStream) {
        this.compiler = compiler;
        this.toWrite = new ArrayList<>();
        this.outputStream = outputStream;
    }

    public RustFileWriter(DBSPCompiler compiler, String outputFile)
            throws FileNotFoundException, UnsupportedEncodingException {
        this(compiler, new PrintStream(outputFile, "UTF-8"));
    }

    @Override
    public DBSPCompiler getCompiler() {
        return this.compiler;
    }

    void generateStructures(StructuresUsed used, IndentStream stream) {
        /*
        #[derive(Clone)]
        pub struct Semigroup2<T0, T1, TS0, TS1>(PhantomData<(T0, T1, TS0, TS1)>);

        impl<T0, T1, TS0, TS1> Semigroup<(T0, T1)> for Semigroup2<T0, T1, TS0, TS1>
        where
            TS0: Semigroup<T0>,
            TS1: Semigroup<T1>,
        {
            fn combine(left: &(T0, T1), right: &(T0, T1)) -> (T0, T1) {
                (
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
                    .append("<(")
                    .intercalate(", ", indexes, ix -> "T" + ix)
                    .append(")> for Semigroup")
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
                    .append("fn combine(left: &(")
                    .intercalate(", ", ts)
                    .append("), right:&(")
                    .intercalate(", ", ts)
                    .append(")) -> (")
                    .intercalate(", ", ts)
                    .append(") {").increase()
                    .append("(").increase()
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
            if (i == 0)
                continue;
            stream.append("Tuple")
                    .append(i)
                    .append("<");
            for (int j = 0; j < i; j++) {
                if (j > 0)
                    stream.append(", ");
                stream.append("T")
                        .append(j);
            }
            stream.append(">,\n");
        }
        stream.decrease().append("}\n\n");
    }

    public String generatePreamble(StructuresUsed used) {
        IndentStream stream = new IndentStream(new StringBuilder());
        stream.append(commonPreamble);
        if (this.compiler.options.ioOptions.jit) {
            stream.append(jitPreamble);
        } else {
            stream.append(rustPreamble)
                    .newline();
            stream.append("type ")
                    .append(new DBSPTypeWeight().getRustString())
                    .append(" = ")
                    .append(this.getCompiler().getWeightTypeImplementation().toString())
                    .append(";")
                    .newline();
            this.generateStructures(used, stream);
        }
        return stream.toString();
    }

    public void add(ProgramAndTester pt) {
        if (pt.program != null)
            this.add(pt.program);
        this.add(pt.tester);
    }

    public void add(DBSPCircuit circuit) {
        this.toWrite.add(circuit);
    }

    public void add(DBSPFunction function) {
        this.toWrite.add(function);
    }

    public void write() {
        // Lower the circuits
        CircuitRewriter reducer = new BetaReduction(this.compiler).getCircuitVisitor();
        List<IDBSPNode> lowered = new ArrayList<>();
        FindResources findResources = new FindResources(this.compiler);
        CircuitRewriter findCircuitResources = findResources.getCircuitVisitor();
        LowerCircuitVisitor lower = new LowerCircuitVisitor(this.compiler);
        SanitizeNames sanitizer = new SanitizeNames(this.compiler);

        for (IDBSPNode node: this.toWrite) {
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                inner.accept(findResources);
                lowered.add(inner);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                // Lowering implements aggregates and inlines some calls.
                outer = lower.apply(outer);
                // Beta reduction is beneficial after implementing aggregates.
                outer = reducer.apply(outer);
                // Sanitize structure names
                outer = sanitizer.apply(outer);
                // Find the resources used to generate the correct Rust preamble
                outer = findCircuitResources.apply(outer);
                lowered.add(outer);
            }
        }
        // Emit code
        this.outputStream.println(generatePreamble(used));
        for (IDBSPNode node: lowered) {
            String str;
            IDBSPInnerNode inner = node.as(IDBSPInnerNode.class);
            if (inner != null) {
                str = ToRustInnerVisitor.toRustString(this.compiler, inner, false);
            } else {
                DBSPCircuit outer = node.to(DBSPCircuit.class);
                if (this.emitHandles)
                    str = ToRustHandleVisitor.toRustString(this.compiler, outer, outer.name);
                else
                    str = ToRustVisitor.toRustString(this.getCompiler(), outer);
            }
            this.outputStream.println(str);
        }
    }

    public void writeAndClose() {
        Logger.INSTANCE.setLoggingLevel(FindResources.class, 3);
        this.write();
        this.outputStream.close();
    }
}
