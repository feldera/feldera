// Automatically-generated file
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(unused_imports)]
#![allow(unused_parens)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unconditional_panic)]

#![allow(non_camel_case_types)]
#[cfg(test)]
use hashing::*;use dbsp::{
    algebra::{ZSet, MulByRef, F32, F64, Semigroup, SemigroupValue, ZRingValue,
         UnimplementedSemigroup, DefaultSemigroup, HasZero, AddByRef, NegByRef,
         AddAssignByRef,
    },
    circuit::{checkpointer::Checkpoint, Circuit, CircuitConfig, Stream},
    operator::{
        Generator,
        FilterMap,
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
};
use core::cmp::Ordering;
use rust_decimal::Decimal;
use dbsp::declare_tuples;
use json::*;
use sqllib::{
    *,
    array::*,
    casts::*,
    binary::*,
    geopoint::*,
    timestamp::*,
    interval::*,
    string::*,
    operators::*,
    aggregates::*,
    variant::*,
};
use sltsqlvalue::*;
#[cfg(test)]
use readers::*;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

declare_tuples! {
}


sltsqlvalue::to_sql_row_impl! {
}


pub fn circuit(cconf: CircuitConfig) -> Result<(DBSPHandle, Catalog), Error> {

    let (circuit, streams) = Runtime::init_circuit(cconf, |circuit| {
        let mut catalog = Catalog::new();
        // CREATE TABLE `T` (`K` BIGINT NOT NULL PRIMARY KEY)
        #[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
        struct struct_1 {
            #[serde(rename = "K")]
            field0: i64,
        }
        impl From<struct_1> for Tup1<i64> {
            fn from(table: struct_1) -> Self {
                Tup1::new(table.field0.into(), )
            }
        }
        impl From<Tup1<i64>> for struct_1 {
            fn from(tuple: Tup1<i64>) -> Self {
                Self {
                    field0: tuple.0.into(),
                }
            }
        }
        deserialize_table_record!(struct_1["T", 1] {
            (field0, "K", false, i64, None)
        });
        serialize_table_record!(struct_1[1]{
            field0["K"]: i64
        });
        #[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
        struct struct_1_key {
            #[serde(rename = "K")]
            field0: i64,
        }
        impl From<struct_1_key> for Tup1<i64> {
            fn from(table: struct_1_key) -> Self {
                Tup1::new(table.field0.into(), )
            }
        }
        impl From<Tup1<i64>> for struct_1_key {
            fn from(tuple: Tup1<i64>) -> Self {
                Self {
                    field0: tuple.0.into(),
                }
            }
        }
        deserialize_table_record!(struct_1_key["struct_1_key", 1] {
            (field0, "K", false, i64, None)
        });
        serialize_table_record!(struct_1_key[1]{
            field0["K"]: i64
        });
        #[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
        struct struct_1_upsert {
            #[serde(rename = "K")]
            field0: i64,
        }
        impl From<struct_1_upsert> for Tup1<i64> {
            fn from(table: struct_1_upsert) -> Self {
                Tup1::new(table.field0.into(), )
            }
        }
        impl From<Tup1<i64>> for struct_1_upsert {
            fn from(tuple: Tup1<i64>) -> Self {
                Self {
                    field0: tuple.0.into(),
                }
            }
        }
        deserialize_table_record!(struct_1_upsert["struct_1_upsert", 1] {
            (field0, "K", false, i64, None)
        });
        serialize_table_record!(struct_1_upsert[1]{
            field0["K"]: i64
        });
        // DBSPSourceMapOperator 493(34)
        // CREATE TABLE `T` (`K` BIGINT NOT NULL PRIMARY KEY)
        let (stream493, handle493) = circuit.add_input_map::<Tup1<i64>, Tup1<i64>, Tup1<i64>, _>(
            Box::new(|updated: &mut Tup1<i64>, changes: &Tup1<i64>| {}));
        catalog.register_input_map::<Tup1<i64>, struct_1_key, Tup1<i64>, struct_1, Tup1<i64>, struct_1_upsert, _, _>(stream493.clone(), handle493, move |t_8: &Tup1<i64>, | ->
        Tup1<i64> {
            Tup1::new((*t_8).0)
        }, move |t_9: &Tup1<i64>, | ->
        Tup1<i64> {
            Tup1::new((*t_9).0)
        }, r#"{"name":"T","case_sensitive":false,"fields":[{"name":"K","case_sensitive":false,"columntype":{"nullable":false,"type":"BIGINT"}}],"primary_key":["K"],"materialized":false,"foreign_keys":[]}"#);

        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPMapIndexOperator 768(202)
        let stream768: Stream<_, IndexedWSet<Tup0, Tup1<i64>>> = stream493.map_index(move |t_7: (&Tup1<i64>, &Tup1<i64>, ), | ->
        (Tup0, Tup1<i64>, ) {
            (Tup0::new(), Tup1::new((*t_7.1).clone().0), )
        });
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPAggregateLinearPostprocessOperator 774(121)
        let stream774: Stream<_, IndexedWSet<Tup0, Tup1<i64>>> = stream768.aggregate_linear_postprocess(move |t_2: &Tup1<i64>, | ->
        Tup1<i64> {
            Tup1::new(1i64)
        }, move |t_3: Tup1<i64>, | ->
        Tup1<i64> {
            Tup1::new(t_3.0)
        });
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPMapOperator 776(212)
        let stream776: Stream<_, WSet<Tup1<i64>>> = stream774.map(move |t_4: (&Tup0, &Tup1<i64>, ), | ->
        Tup1<i64> {
            Tup1::new((*t_4.1).0)
        });
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPMapOperator 781(217)
        let stream781: Stream<_, WSet<Tup1<i64>>> = stream774.map(move |t_4: (&Tup0, &Tup1<i64>, ), | ->
        Tup1<i64> {
            Tup1::new(0i64)
        });
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPNegateOperator 786(220)
        let stream786: Stream<_, WSet<Tup1<i64>>> = stream781.neg();
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        let stream157 = circuit.add_source(Generator::new(|| if Runtime::worker_index() == 0 {zset!(
            Tup1::new(0i64) => 1,
        )} else {zset!(
        )}));
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPDifferentiateOperator 222(157)
        let stream222: Stream<_, WSet<Tup1<i64>>> = stream157.differentiate();
        // rel#26:LogicalAggregate.(input=LogicalTableScan#1,group={},C=COUNT())
        // DBSPSumOperator 788(226)
        let stream788: Stream<_, WSet<Tup1<i64>>> = stream222.sum([&stream786, &stream776]);
        // CREATE VIEW `V` AS
        // SELECT COUNT(*) AS `C`
        // FROM `schema`.`T` AS `T`
        // DBSPSinkOperator 790(174)
        #[derive(Clone, Debug, Eq, PartialEq, Default, serde::Serialize, serde::Deserialize)]
        struct struct_2 {
            #[serde(rename = "C")]
            field0: i64,
        }
        impl From<struct_2> for Tup1<i64> {
            fn from(table: struct_2) -> Self {
                Tup1::new(table.field0.into(), )
            }
        }
        impl From<Tup1<i64>> for struct_2 {
            fn from(tuple: Tup1<i64>) -> Self {
                Self {
                    field0: tuple.0.into(),
                }
            }
        }
        deserialize_table_record!(struct_2["V", 1] {
            (field0, "C", false, i64, None)
        });
        serialize_table_record!(struct_2[1]{
            field0["C"]: i64
        });
        catalog.register_output_zset::<_, struct_2>(stream788.clone(), r#"{"name":"V","case_sensitive":false,"fields":[{"name":"C","case_sensitive":false,"columntype":{"nullable":false,"type":"BIGINT"}}],"materialized":false}"#);

        Ok(catalog)
    })?;
    Ok((circuit, streams))
}


