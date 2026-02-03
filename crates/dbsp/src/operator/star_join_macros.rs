#[macro_export]
macro_rules! generate_all_join_orderings {
    ($($f:tt)::+, [$($fixed:tt)*], $first:expr, $($rest:expr),+ $(,)?) => {
        $crate::generate_all_join_orderings!(@collect $($f)::+, [$($fixed)*], $first, []; []; $($rest),+)
    };
    (@collect $($f:tt)::+, [$($fixed:tt)*], $first:expr,
        [$($out:expr,)*]; [$($prefix:expr,)*];
        $head:expr $(, $tail:expr)*
    ) => {
        $crate::generate_all_join_orderings!(
            @collect
            $($f)::+,
            [$($fixed)*],
            $first,
            [$($out,)* $($f)::+!($($fixed)* $($prefix,)* $first, $head $(, $tail)*),];
            [$($prefix,)* $head,];
            $($tail),*
        )
    };
    (@collect $($f:tt)::+, [$($fixed:tt)*], $first:expr,
        [$($out:expr,)*]; [$($prefix:expr,)*];
    ) => {
        [$($out,)* $($f)::+!($($fixed)* $($prefix,)* $first)]
    };
}

#[macro_export]
macro_rules! count_tts {
    () => {
        0usize
    };
    ($head:tt $(, $tail:tt)*) => {
        1usize + $crate::count_tts!($($tail),*)
    };
}

#[macro_export]
macro_rules! build_star_join_index_func {
    ($join_func:ident, $prefix_cursor:ident, $trace_cursors:ident, $($vals:expr),+ $(,)?) => {{
        use $crate::{dynamic::{Erase, DowncastTrait}, trace::Cursor};

        let mut ok: Box<DynData> = Box::<OK>::default().erase_box();
        let mut ov: Box<DynData> = Box::<OV>::default().erase_box();
        let join_func = $join_func.clone();

        Box::new(move |$prefix_cursor, $trace_cursors, cb| {
            for (k, v) in join_func(
                unsafe { $prefix_cursor.key().downcast() },
                $($vals),+
            ) {
                *unsafe { ok.downcast_mut() } = k;
                *unsafe { ov.downcast_mut() } = v;
                cb(ok.as_mut(), ov.as_mut());
            }
        })
    }};
}

#[macro_export]
macro_rules! build_star_join_flatmap_func {
    ($join_func:ident, $prefix_cursor:ident, $trace_cursors:ident, $($vals:expr),+ $(,)?) => {{
        use $crate::dynamic::{Erase, DowncastTrait};
        use $crate::trace::Cursor;

        let mut ov: Box<DynData> = Box::<OV>::default().erase_box();
        let join_func = $join_func.clone();

        Box::new(move |$prefix_cursor, $trace_cursors, cb| {
            for v in join_func(
                unsafe { $prefix_cursor.key().downcast() },
                $($vals),+
            ) {
                *unsafe { ov.downcast_mut() } = v;
                cb(ov.as_mut(), ().erase_mut());
            }
        })
    }};
}

#[macro_export]
macro_rules! build_star_join_func {
    ($join_func:ident, $prefix_cursor:ident, $trace_cursors:ident, $($vals:expr),+ $(,)?) => {{
        use $crate::dynamic::{Erase, DowncastTrait};
        use $crate::trace::Cursor;

        let mut ov: Box<DynData> = Box::<OV>::default().erase_box();
        let join_func = $join_func.clone();

        Box::new(move |$prefix_cursor, $trace_cursors, cb| {
            let v = join_func(
                unsafe { $prefix_cursor.key().downcast() },
                $($vals),+
            );
            *unsafe { ov.downcast_mut() } = v;
            cb(ov.as_mut(), ().erase_mut());
        })
    }};
}

#[macro_export]
macro_rules! star_join_index_funcs {
    (
        $join_func:ident,
        $prefix_cursor:ident,
        $trace_cursors:ident,
        [$($trace_idx:tt),+ $(,)?]
    ) => {
        $crate::generate_all_join_orderings!(
            $crate::build_star_join_index_func,
            [$join_func, $prefix_cursor, $trace_cursors,],
            unsafe { $prefix_cursor.val().downcast() },
            $(unsafe { $trace_cursors[$trace_idx].val().downcast() }),+
        )
    };
}

#[macro_export]
macro_rules! star_join_flatmap_funcs {
    (
        $join_func:ident,
        $prefix_cursor:ident,
        $trace_cursors:ident,
        [$($trace_idx:tt),+ $(,)?]
    ) => {
        $crate::generate_all_join_orderings!(
            $crate::build_star_join_flatmap_func,
            [$join_func, $prefix_cursor, $trace_cursors,],
            unsafe { $prefix_cursor.val().downcast() },
            $(unsafe { $trace_cursors[$trace_idx].val().downcast() }),+
        )
    };
}

#[macro_export]
macro_rules! star_join_funcs {
    (
        $join_func:ident,
        $prefix_cursor:ident,
        $trace_cursors:ident,
        [$($trace_idx:tt),+ $(,)?]
    ) => {
        $crate::generate_all_join_orderings!(
            $crate::build_star_join_func,
            [$join_func, $prefix_cursor, $trace_cursors,],
            unsafe { $prefix_cursor.val().downcast() },
            $(unsafe { $trace_cursors[$trace_idx].val().downcast() }),+
        )
    };
}

#[macro_export]
macro_rules! inner_star_join_index_body {
    (
        $stream1:expr,
        [$($stream:expr),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ok_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            NestedCircuit,
            dynamic::DynData,
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<NestedCircuit, MonoIndexedZSet, DynData, DynData>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_index_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ok_ty, $ov_ty>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_inner_star_join_index_mono(
                &join_factories,
                &[$($stream.inner()),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! inner_star_join_index_root_body {
    (
        $stream1:expr,
        [$($stream:expr),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ok_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            RootCircuit,
            dynamic::DynData,
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynData>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_index_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ok_ty, $ov_ty>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_inner_star_join_index_mono(
                &join_factories,
                &[$($stream.inner()),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! star_join_index_body {
    (
        $stream1:expr,
        [$(($stream:expr, $saturate:expr)),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ok_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            RootCircuit,
            dynamic::DynData,
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynData>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_index_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ok_ty, $ov_ty>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_star_join_index_mono(
                &join_factories,
                &[$(($stream.inner(), $saturate)),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! inner_star_join_flatmap_body {
    (
        $stream1:expr,
        [$($stream:expr),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            NestedCircuit,
            dynamic::{DynData, DynUnit},
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<NestedCircuit, MonoIndexedZSet, DynData, DynUnit>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_flatmap_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ov_ty, ()>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_inner_star_join_mono(
                &join_factories,
                &[$($stream.inner()),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! inner_star_join_flatmap_root_body {
    (
        $stream1:expr,
        [$($stream:expr),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            RootCircuit,
            dynamic::{DynData, DynUnit},
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynUnit>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_flatmap_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ov_ty, ()>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_inner_star_join_mono(
                &join_factories,
                &[$($stream.inner()),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! star_join_flatmap_body {
    (
        $stream1:expr,
        [$(($stream:expr, $saturate:expr)),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            RootCircuit,
            dynamic::{DynData, DynUnit},
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynUnit>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_flatmap_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ov_ty, ()>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_star_join_mono(
                &join_factories,
                &[$(($stream.inner(), $saturate)),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! inner_star_join_root_body {
    (
        $stream1:expr,
        [$($stream:expr),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            RootCircuit,
            dynamic::{DynData, DynUnit},
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynUnit>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ov_ty, ()>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_inner_star_join_mono(
                &join_factories,
                &[$($stream.inner()),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! inner_star_join_body {
    (
        $stream1:expr,
        [$($stream:expr),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            NestedCircuit,
            dynamic::{DynData, DynUnit},
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };


        let join_funcs: [StarJoinFunc<NestedCircuit, MonoIndexedZSet, DynData, DynUnit>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ov_ty, ()>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_inner_star_join_mono(
                &join_factories,
                &[$($stream.inner()),+],
                &join_funcs,
            )
            .typed()
    }};
}

#[macro_export]
macro_rules! star_join_body {
    (
        $stream1:expr,
        [$(($stream:expr, $saturate:expr)),+ $(,)?],
        $join_func:ident,
        [$($val_ty:ty),+ $(,)?],
        [$($trace_idx:tt),+ $(,)?],
        $k_ty:ty,
        $ov_ty:ty
    ) => {{
        use $crate::{
            RootCircuit,
            dynamic::{DynData, DynUnit},
            operator::dynamic::{MonoIndexedZSet, multijoin::{StarJoinFunc, StarJoinFactories}},
            trace::BatchReaderFactories,
        };

        let join_funcs: [StarJoinFunc<RootCircuit, MonoIndexedZSet, DynData, DynUnit>;
            $crate::count_tts!($($trace_idx),+) + 1
        ] = $crate::star_join_funcs!(
            $join_func,
            prefix_cursor,
            trace_cursors,
            [$($trace_idx),+]
        );

        let mut join_factories = StarJoinFactories::new::<$ov_ty, ()>();
        $(
            join_factories.add_input_factories(
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
                BatchReaderFactories::new::<$k_ty, $val_ty, ZWeight>(),
            );
        )+

        $stream1
            .inner()
            .dyn_star_join_mono(
                &join_factories,
                &[$(($stream.inner(), $saturate)),+],
                &join_funcs,
            )
            .typed()
    }};
}

/// Generate a `inner_star_joinN`/`inner_star_joinN_nested` function.
///
/// Call this macro for every number N in order to generate an N-way inner star join
/// operator for both RootCircuit and NestedCircuit.
///
/// The operator computes an incremental join of multiple streams on the same key using a
/// user-provided join function that returns exactly one output value for each input tuple.
///
/// Example generated function signature:
///
/// ```text
///     pub fn inner_star_join4<K, V1, V2, V3, V4, OV>(
///         stream1: &Stream<crate::RootCircuit, OrdIndexedZSet<K, V1>>,
///         stream2: &Stream<crate::RootCircuit, OrdIndexedZSet<K, V2>>,
///         stream3: &Stream<crate::RootCircuit, OrdIndexedZSet<K, V3>>,
///         stream4: &Stream<crate::RootCircuit, OrdIndexedZSet<K, V4>>,
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> OV + Clone + 'static,
///     ) -> Stream<RootCircuit, OrdZSet<OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OV: DBData;
///
///     pub fn inner_star_join4_nested<K, V1, V2, V3, V4, OV>(
///         stream1: &Stream<crate::NestedCircuit, OrdIndexedZSet<K, V1>>,
///         stream2: &Stream<crate::NestedCircuit, OrdIndexedZSet<K, V2>>,
///         stream3: &Stream<crate::NestedCircuit, OrdIndexedZSet<K, V3>>,
///         stream4: &Stream<crate::NestedCircuit, OrdIndexedZSet<K, V4>>,
///     ) -> Stream<NestedCircuit, OrdZSet<OV>>
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> OV + Clone + 'static,
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OV: DBData;
/// ```
#[macro_export]
macro_rules! define_inner_star_join {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                #[allow(unused)]
                pub fn [<inner_star_join $n>]<K, V1, #(V~I,)* OV>(
                    stream1: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        stream~I: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> OV + Clone + 'static,
                ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OV: $crate::DBData,
                {
                    $crate::inner_star_join_root_body!(
                        stream1,
                        [#(stream~I,)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OV
                    )
                }

                #[allow(unused)]
                pub fn [<inner_star_join $n _nested>]<K, V1, #(V~I,)* OV>(
                    stream1: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        stream~I: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> OV + Clone + 'static,
                ) -> $crate::Stream<$crate::NestedCircuit, $crate::OrdZSet<OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OV: $crate::DBData,
                {
                    $crate::inner_star_join_body!(
                        stream1,
                        [#(stream~I,)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OV
                    )
                }
            }
        });
    };
}

/// Generate a `inner_star_join_indexN`/`inner_star_join_indexN_nested` function.
///
/// Call this macro for every number N in order to generate an N-way inner star join
/// index operator for both RootCircuit and NestedCircuit.
///
/// The operator computes an incremental join of multiple streams on the same key using a
/// user-provided join function that can return 0 or more output key-value pairs for each input tuple.
///
/// Example generated function signature:
///
/// ```text
///     pub fn inner_star_join_index4<K, V1, V2, V3, V4, OK, OV, It>(
///         stream1: &Stream<RootCircuit, OrdIndexedZSet<K, V1>>,
///         stream2: &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
///         stream3: &Stream<RootCircuit, OrdIndexedZSet<K, V3>>,
///         stream4: &Stream<RootCircuit, OrdIndexedZSet<K, V4>>,
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> It + Clone + 'static,
///     ) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OK: DBData,
///         OV: DBData,
///         It: IntoIterator<Item = (OK, OV)> + 'static;
///
///     pub fn inner_star_join_index4_nested<K, V1, V2, V3, V4, OK, OV, It>(
///         stream1: &Stream<NestedCircuit, OrdIndexedZSet<K, V1>>,
///         stream2: &Stream<NestedCircuit, OrdIndexedZSet<K, V2>>,
///         stream3: &Stream<NestedCircuit, OrdIndexedZSet<K, V3>>,
///         stream4: &Stream<NestedCircuit, OrdIndexedZSet<K, V4>>,
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> It + Clone + 'static,
///     ) -> Stream<NestedCircuit, OrdIndexedZSet<OK, OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OK: DBData,
///         OV: DBData,
///         It: IntoIterator<Item = (OK, OV)> + 'static;
/// ```
#[macro_export]
macro_rules! define_inner_star_join_index {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                #[allow(unused)]
                pub fn [<inner_star_join_index $n>]<K, V1, #(V~I,)* OK, OV, It>(
                    stream1: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        stream~I: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<OK, OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OK: $crate::DBData,
                    OV: $crate::DBData,
                    It: IntoIterator<Item = (OK, OV)> + 'static,
                {
                    $crate::inner_star_join_index_root_body!(
                        stream1,
                        [#(stream~I,)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OK,
                        OV
                    )
                }

                #[allow(unused)]
                pub fn [<inner_star_join_index $n _nested>]<K, V1, #(V~I,)* OK, OV, It>(
                    stream1: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        stream~I: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                ) -> $crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<OK, OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OK: $crate::DBData,
                    OV: $crate::DBData,
                    It: IntoIterator<Item = (OK, OV)> + 'static,
                {
                    $crate::inner_star_join_index_body!(
                        stream1,
                        [#(stream~I,)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OK,
                        OV
                    )
                }
            }

        });
    };
}

/// Generate a `inner_star_join_flatmapN`/`inner_star_join_flatmapN_nested` function.
///
/// Call this macro for every number N in order to generate an N-way inner star join
/// flatmap operator for both RootCircuit and NestedCircuit.
///
/// The operator computes an incremental join of multiple streams on the same key using a
/// user-provided join function that can return 0 or more output values for each input tuple.
///
/// Example generated function signature:
///
/// ```text
///     pub fn inner_star_join_flatmap4<K, V1, V2, V3, V4, OV, It>(
///         stream1: &Stream<RootCircuit, OrdIndexedZSet<K, V1>>,
///         stream2: &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
///         stream3: &Stream<RootCircuit, OrdIndexedZSet<K, V3>>,
///         stream4: &Stream<RootCircuit, OrdIndexedZSet<K, V4>>,
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> It + Clone + 'static,
///     ) -> Stream<RootCircuit, OrdZSet<OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OV: DBData,
///         It: IntoIterator<Item = OV> + 'static;
///
///     pub fn inner_star_join_flatmap4_nested<K, V1, V2, V3, V4, OV, It>(
///         stream1: &Stream<NestedCircuit, OrdIndexedZSet<K, V1>>,
///         stream2: &Stream<NestedCircuit, OrdIndexedZSet<K, V2>>,
///         stream3: &Stream<NestedCircuit, OrdIndexedZSet<K, V3>>,
///         stream4: &Stream<NestedCircuit, OrdIndexedZSet<K, V4>>,
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> It + Clone + 'static,
///     ) -> Stream<NestedCircuit, OrdZSet<OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OV: DBData,
///         It: IntoIterator<Item = OV> + 'static;
/// ```
#[macro_export]
macro_rules! define_inner_star_join_flatmap {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                #[allow(unused)]
                pub fn [<inner_star_join_flatmap $n>]<K, V1, #(V~I,)* OV, It>(
                    stream1: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        stream~I: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OV: $crate::DBData,
                    It: IntoIterator<Item = OV> + 'static,
                {
                    $crate::inner_star_join_flatmap_root_body!(
                        stream1,
                        [#(stream~I,)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OV
                    )
                }

                #[allow(unused)]
                pub fn [<inner_star_join_flatmap $n _nested>]<K, V1, #(V~I,)* OV, It>(
                    stream1: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        stream~I: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                ) -> $crate::Stream<$crate::NestedCircuit, $crate::OrdZSet<OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OV: $crate::DBData,
                    It: IntoIterator<Item = OV> + 'static,
                {
                    $crate::inner_star_join_flatmap_body!(
                        stream1,
                        [#(stream~I,)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OV
                    )
                }
            }
        });
    };
}

/// Generate a `star_joinN` function.
///
/// Call this macro for every number N in order to generate an N-way star-join operator.
/// This operator is only available for RootCircuit. See `define_inner_star_join` for a
/// star join operator that works for NestedCircuit.
///
/// The operator computes an incremental join of multiple streams on the same key using a
/// user-provided join function that must return exactly one value per input tuple.  The
/// `saturate` flag for each input stream (except the first one) controls whether to compute
/// an outer or inner join for that stream.
///
/// Example generated function signature:
///
/// ```text
///     pub fn star_join4<K, V1, V2, V3, V4, OV>(
///         stream1: &Stream<RootCircuit, OrdIndexedZSet<K, V1>>,
///         (stream2, saturate2): (&Stream<RootCircuit, OrdIndexedZSet<K, V2>>, bool),
///         (stream3, saturate3): (&Stream<RootCircuit, OrdIndexedZSet<K, V3>>, bool),
///         (stream4, saturate4): (&Stream<RootCircuit, OrdIndexedZSet<K, V4>>, bool),
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> OV + Clone + 'static,
///     ) -> Stream<RootCircuit, OrdZSet<OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OV: DBData;
/// ```
#[macro_export]
macro_rules! define_star_join {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                pub fn [<star_join $n>]<K, V1, #(V~I,)* OV>(
                    stream1: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        (stream~I, saturate~I): (
                            &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                            bool
                        ),
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> OV + Clone + 'static,
                ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OV: $crate::DBData,
                {
                    $crate::star_join_body!(
                        stream1,
                        [#((stream~I, saturate~I),)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OV
                    )
                }
            }

        });
    };
}

/// Generate a `star_join_indexN` function.
///
/// Call this macro for every number N in order to generate an N-way star-join-index operator.
/// This operator is only available for RootCircuit. See `define_inner_star_join_index` for a
/// star-join-index operator that works for NestedCircuit.
///
/// The operator computes an incremental join of multiple streams on the same key using a
/// user-provided join function that can return 0 or more output key-value pairs for each input tuple.
/// The `saturate` flag for each input stream (except the first one) controls whether to compute an outer or inner join
/// for that stream.
///
/// Example generated function signature:
///
/// ```text
///     pub fn star_join_index4<K, V1, V2, V3, V4, OK, OV, It>(
///         stream1: &Stream<RootCircuit, OrdIndexedZSet<K, V1>>,
///         (stream2, saturate2): (
///             &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
///             bool,
///         ),
///         (stream3, saturate3): (
///             &Stream<RootCircuit, OrdIndexedZSet<K, V3>>,
///             bool,
///         ),
///         (stream4, saturate4): (
///             &Stream<RootCircuit, OrdIndexedZSet<K, V4>>,
///             bool,
///         ),
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> It + Clone + 'static,
///     ) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
///     where
///         K: DBData,
///         V1: DBData,
///         V2: DBData,
///         V3: DBData,
///         V4: DBData,
///         OK: DBData,
///         OV: DBData,
///         It: IntoIterator<Item = (OK, OV)> + 'static;
/// ```
#[macro_export]
macro_rules! define_star_join_index {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                pub fn [<star_join_index $n>]<K, V1, #(V~I,)* OK, OV, It>(
                    stream1: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        (stream~I, saturate~I): (
                            &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                            bool
                        ),
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<OK, OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OK: $crate::DBData,
                    OV: $crate::DBData,
                    It: IntoIterator<Item = (OK, OV)> + 'static,
                {
                    $crate::star_join_index_body!(
                        stream1,
                        [#((stream~I, saturate~I),)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OK,
                        OV
                    )
                }
            }

        });
    };
}

/// Generate a `star_join_flatmapN` function.
///
/// Call this macro for every number N in order to generate an N-way star join flatmap operator.
/// This operator is only available for RootCircuit. See `define_inner_star_join_flatmap` for a
/// star join flatmap operator that works for NestedCircuit.
///
/// The operator computes an incremental join of multiple streams on the same key using a
/// user-provided join function that can return 0 or more output values for each input tuple.
/// The `saturate` flag for each input stream (except the first one) controls whether to compute
/// an outer or inner join for that stream.
///
/// Example generated function signature:
///
/// ```text
///     pub fn star_join_flatmap4<K, V1, V2, V3, V4, OV, It>(
///         stream1: &Stream<RootCircuit, OrdIndexedZSet<K, V1>>,
///         (stream2, saturate2): (
///             &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
///             bool,
///         ),
///         (stream3, saturate3): (
///             &Stream<RootCircuit, OrdIndexedZSet<K, V3>>,
///             bool,
///         ),
///         (stream4, saturate4): (
///             &Stream<RootCircuit, OrdIndexedZSet<K, V4>>,
///             bool,
///         ),
///         join_func: impl Fn(&K, &V1, &V2, &V3, &V4) -> It + Clone + 'static,
///     ) -> Stream<RootCircuit, OrdZSet<OV>>;
/// ```
#[macro_export]
macro_rules! define_star_join_flatmap {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                pub fn [<star_join_flatmap $n>]<K, V1, #(V~I,)* OV, It>(
                    stream1: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>,
                    #(
                        (stream~I, saturate~I): (
                            &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                            bool
                        ),
                    )*
                    join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                    #(V~I: $crate::DBData,)*
                    OV: $crate::DBData,
                    It: IntoIterator<Item = OV> + 'static,
                {
                    $crate::star_join_flatmap_body!(
                        stream1,
                        [#((stream~I, saturate~I),)*],
                        join_func,
                        [V1, #(V~I,)*],
                        [#((I - 2),)*],
                        K,
                        OV
                    )
                }
            }
        });
    };
}
