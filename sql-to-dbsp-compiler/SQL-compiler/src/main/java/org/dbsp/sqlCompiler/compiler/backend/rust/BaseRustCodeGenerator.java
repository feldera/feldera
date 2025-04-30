package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.util.IIndentStream;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Base class for generating Rust code */
public abstract class BaseRustCodeGenerator implements ICodeGenerator {
    static int crdId = 0;
    final int id;
    /** List of nodes to generate code for */
    protected final List<IDBSPNode> toWrite;
    /** List of crate names that are dependencies for this one */
    protected final List<String> dependencies;
    /** Stream where code is generated; set by {@link BaseRustCodeGenerator#setOutputBuilder} */
    @Nullable
    protected IIndentStream outputBuilder = null;
    boolean generateUdfInclude = true;
    boolean generateMalloc = true;
    boolean generateTuples = true;

    protected BaseRustCodeGenerator() {
        this.id = crdId++;
        this.toWrite = new ArrayList<>();
        this.dependencies = new ArrayList<>();
    }

    public BaseRustCodeGenerator withGenerateTuples(boolean generate) {
        this.generateTuples = generate;
        return this;
    }

    public BaseRustCodeGenerator withUdf(boolean udf) {
        this.generateUdfInclude = udf;
        return this;
    }

    public BaseRustCodeGenerator withMalloc(boolean malloc) {
        this.generateMalloc = malloc;
        return this;
    }

    protected String dbspCircuit(boolean topLevel) {
        return topLevel ? "RootCircuit" : "ChildCircuit<RootCircuit>";
    }

    @Override
    public void setOutputBuilder(IIndentStream stream) {
        this.outputBuilder = stream;
    }

    public IIndentStream builder() {
        return Objects.requireNonNull(this.outputBuilder);
    }

    @Override
    public void add(IDBSPNode node) {
        this.toWrite.add(node);
    }

    @Override
    public void addDependency(String crate) {
        this.dependencies.add(crate);
    }

    /** Preamble used for all compilations. */
    public static final String COMMON_PREAMBLE = """
            // Automatically-generated file
            #![allow(dead_code)]
            #![allow(non_snake_case)]
            #![allow(non_camel_case_types)]
            #![allow(unused_imports)]
            #![allow(unused_parens)]
            #![allow(unused_variables)]
            #![allow(unused_mut)]
            #![allow(unconditional_panic)]
            #![allow(non_upper_case_globals)]
            """;

    public static final String ALLOC_PREAMBLE = """
            #[cfg(not(target_env = "msvc"))]
            #[global_allocator]
            static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
            #[allow(non_upper_case_globals)]
            #[export_name = "malloc_conf"]
            pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\\0";""";

    public static final String STANDARD_PREAMBLE = """
            use dbsp::{
                algebra::{ZSet, MulByRef, F32, F64, Semigroup, SemigroupValue, ZRingValue,
                     UnimplementedSemigroup, DefaultSemigroup, HasZero, AddByRef, NegByRef,
                     AddAssignByRef,
                },
                circuit::{checkpointer::Checkpoint, ChildCircuit, Circuit, CircuitConfig, RootCircuit, Stream},
                operator::{
                    dynamic::aggregate::{Max, Min, MinSome1, Postprocess},
                    Generator,
                    Fold,
                    group::WithCustomOrd,
                    time_series::{RelRange, RelOffset, OrdPartitionedIndexedZSet, OrdPartitionedOverStream},
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
            use dbsp_adapters::Catalog;
            use feldera_types::{
                program_schema::SqlIdentifier,
                deserialize_table_record, serialize_table_record,
            };
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
            use feldera_sqllib::*;""";
}
