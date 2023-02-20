use std::time::Duration;

pub trait PartialOrder: Eq {
    /// Returns true if one element is strictly less than the other
    #[inline]
    fn less_than(&self, other: &Self) -> bool {
        self != other && self.less_equal(other)
    }

    /// Returns true if one element is less than or equal to the other
    fn less_equal(&self, other: &Self) -> bool;
}

pub trait TotalOrder: PartialOrder {}

macro_rules! impl_order {
    ($($type:ty),* $(,)?) => (
        $(
            impl PartialOrder for $type {
                #[inline]
                fn less_than(&self, other: &Self) -> bool {
                    self < other
                }

                #[inline]
                fn less_equal(&self, other: &Self) -> bool {
                    self <= other
                }
            }

            impl TotalOrder for $type {}
        )*
    )
}

// Implement `PartialOrder` and `TotalOrder` for primitive types
impl_order! {
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    (),
    Duration,
}
