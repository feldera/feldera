package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.ir.IDBSPNode;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseCodeGenerator implements ICodeGenerator {
    final List<IDBSPNode> toWrite;
    final List<String> dependencies;
    @Nullable
    protected PrintStream outputStream = null;

    protected BaseCodeGenerator() {
        this.toWrite = new ArrayList<>();
        this.dependencies = new ArrayList<>();
    }

    @Override
    public void setPrintStream(PrintStream stream) {
        this.outputStream = stream;
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
    static final String COMMON_PREAMBLE =
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

    static final String STANDARD_PREAMBLE =
            """
            use dbsp::{
                algebra::{ZSet, MulByRef, F32, F64, Semigroup, SemigroupValue, ZRingValue,
                     UnimplementedSemigroup, DefaultSemigroup, HasZero, AddByRef, NegByRef,
                     AddAssignByRef,
                },
                circuit::{checkpointer::Checkpoint, Circuit, CircuitConfig, RootCircuit, Stream},
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
}
