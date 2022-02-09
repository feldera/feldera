/// Allows easily creating [`FiniteHashMap`](crate::algebra::FiniteHashMap)s
#[macro_export]
macro_rules! finite_map {
    // Create an empty map
    () => {
        $crate::algebra::FiniteHashMap::new()
    };

    // Create a map from elements
    ($($key:expr => $value:expr),+ $(,)?) => {{
        let mut map = $crate::algebra::FiniteHashMap::with_capacity(
            $crate::count_elements!($($key),+),
        );

        $( $crate::algebra::FiniteHashMap::increment(&mut map, &$key, $value); )+

        map
    }};
}

/// Support macro for counting the number of map elements
#[macro_export]
#[doc(hidden)]
macro_rules! count_elements {
    (@replace $_:expr) => {
        ()
    };

    ($($_:expr),+) => {
        <[()]>::len(&[$($crate::count_elements!(@replace $_),)+])
    };
}
