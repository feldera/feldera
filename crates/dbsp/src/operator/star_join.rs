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

#[macro_export]
macro_rules! define_star_join {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                impl<K, V1> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<star_join $n>]<#(V~I,)* OV>(
                        &self,
                        #(
                            (stream~I, saturate~I): (
                                &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                                bool
                            ),
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> OV + Clone + 'static,
                    ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OV: $crate::DBData,
                    {
                        $crate::star_join_body!(
                            self,
                            [#((stream~I, saturate~I),)*],
                            join_func,
                            [V1, #(V~I,)*],
                            [#((I - 2),)*],
                            K,
                            OV
                        )
                    }
                }
            }
        });
    };
}

#[macro_export]
macro_rules! define_inner_star_join {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                impl<K, V1> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<inner_star_join $n>]<#(V~I,)* OV>(
                        &self,
                        #(
                            stream~I: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> OV + Clone + 'static,
                    ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OV: $crate::DBData,
                    {
                        $crate::inner_star_join_root_body!(
                            self,
                            [#(stream~I,)*],
                            join_func,
                            [V1, #(V~I,)*],
                            [#((I - 2),)*],
                            K,
                            OV
                        )
                    }
                }

                impl<K, V1> $crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<inner_star_join $n>]<#(V~I,)* OV>(
                        &self,
                        #(
                            stream~I: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> OV + Clone + 'static,
                    ) -> $crate::Stream<$crate::NestedCircuit, $crate::OrdZSet<OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OV: $crate::DBData,
                    {
                        $crate::inner_star_join_body!(
                            self,
                            [#(stream~I,)*],
                            join_func,
                            [V1, #(V~I,)*],
                            [#((I - 2),)*],
                            K,
                            OV
                        )
                    }
                }
            }
        });
    };
}

#[macro_export]
macro_rules! define_inner_star_join_index {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                impl<K, V1> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<inner_star_join_index $n>]<#(V~I,)* OK, OV, It>(
                        &self,
                        #(
                            stream~I: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                    ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<OK, OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OK: $crate::DBData,
                        OV: $crate::DBData,
                        It: IntoIterator<Item = (OK, OV)> + 'static,
                    {
                        $crate::inner_star_join_index_root_body!(
                            self,
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

                impl<K, V1> $crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<inner_star_join_index $n>]<#(V~I,)* OK, OV, It>(
                        &self,
                        #(
                            stream~I: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                    ) -> $crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<OK, OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OK: $crate::DBData,
                        OV: $crate::DBData,
                        It: IntoIterator<Item = (OK, OV)> + 'static,
                    {
                        $crate::inner_star_join_index_body!(
                            self,
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
            }
        });
    };
}

#[macro_export]
macro_rules! define_inner_star_join_flatmap {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                impl<K, V1> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<inner_star_join_flatmap $n>]<#(V~I,)* OV, It>(
                        &self,
                        #(
                            stream~I: &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                    ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OV: $crate::DBData,
                        It: IntoIterator<Item = OV> + 'static,
                    {
                        $crate::inner_star_join_flatmap_root_body!(
                            self,
                            [#(stream~I,)*],
                            join_func,
                            [V1, #(V~I,)*],
                            [#((I - 2),)*],
                            K,
                            OV
                        )
                    }
                }

                impl<K, V1> $crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<inner_star_join_flatmap $n>]<#(V~I,)* OV, It>(
                        &self,
                        #(
                            stream~I: &$crate::Stream<$crate::NestedCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                    ) -> $crate::Stream<$crate::NestedCircuit, $crate::OrdZSet<OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OV: $crate::DBData,
                        It: IntoIterator<Item = OV> + 'static,
                    {
                        $crate::inner_star_join_flatmap_body!(
                            self,
                            [#(stream~I,)*],
                            join_func,
                            [V1, #(V~I,)*],
                            [#((I - 2),)*],
                            K,
                            OV
                        )
                    }
                }
            }
        });
    };
}

#[macro_export]
macro_rules! define_star_join_index {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                impl<K, V1> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<star_join_index $n>]<#(V~I,)* OK, OV, It>(
                        &self,
                        #(
                            (stream~I, saturate~I): (
                                &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                                bool
                            ),
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                    ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<OK, OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OK: $crate::DBData,
                        OV: $crate::DBData,
                        It: IntoIterator<Item = (OK, OV)> + 'static,
                    {
                        $crate::star_join_index_body!(
                            self,
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
            }
        });
    };
}

#[macro_export]
macro_rules! define_star_join_flatmap {
    ($n:literal) => {
        seq_macro::seq!(I in 2..=$n {
            paste::paste! {
                impl<K, V1> $crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V1>>
                where
                    K: $crate::DBData,
                    V1: $crate::DBData,
                {
                    pub fn [<star_join_flatmap $n>]<#(V~I,)* OV, It>(
                        &self,
                        #(
                            (stream~I, saturate~I): (
                                &$crate::Stream<$crate::RootCircuit, $crate::OrdIndexedZSet<K, V~I>>,
                                bool
                            ),
                        )*
                        join_func: impl Fn(&K, &V1, #(&V~I,)*) -> It + Clone + 'static,
                    ) -> $crate::Stream<$crate::RootCircuit, $crate::OrdZSet<OV>>
                    where
                        #(V~I: $crate::DBData,)*
                        OV: $crate::DBData,
                        It: IntoIterator<Item = OV> + 'static,
                    {
                        $crate::star_join_flatmap_body!(
                            self,
                            [#((stream~I, saturate~I),)*],
                            join_func,
                            [V1, #(V~I,)*],
                            [#((I - 2),)*],
                            K,
                            OV
                        )
                    }
                }
            }
        });
    };
}
