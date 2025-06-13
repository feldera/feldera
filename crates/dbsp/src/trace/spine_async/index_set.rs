use std::{
    fmt::Debug,
    ops::{BitOr, BitOrAssign},
};

/// A set of indexes (numbers) in the range 0..=63.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct IndexSet(u64);

impl IndexSet {
    /// Returns a bit-set with only `index` set to 1.
    pub fn for_index(index: usize) -> Self {
        debug_assert!(index < 64);
        Self(1 << index)
    }

    /// Returns a bit-set with the bits in `mask`.
    #[cfg(test)]
    pub fn for_mask(mask: impl Into<u64>) -> Self {
        Self(mask.into())
    }

    /// Returns an empty set.
    pub fn empty() -> Self {
        Self(0)
    }

    /// Returns true if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Returns true if the set contains exactly one index.
    #[allow(dead_code)]
    pub fn is_singleton(&self) -> bool {
        self.0.is_power_of_two()
    }

    /// Returns true if the set contains exactly zero or one indexes.
    pub fn is_short(&self) -> bool {
        self.without_first().is_empty()
    }

    /// Returns true if the set contains two or more indexes.
    pub fn is_long(&self) -> bool {
        !self.is_short()
    }

    /// Returns the number of indexes in the set.
    pub fn len(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns the smallest index in the set.
    pub fn first(&self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }

    // Returns the set with the first index removed. If this set is empty,
    // returns an empty set.
    pub fn without_first(&self) -> Self {
        Self(self.0 & (self.0.wrapping_sub(1)))
    }

    /// Adds `index` to the set.
    pub fn add(&mut self, index: usize) {
        self.0 |= 1 << index;
    }

    /// Removes `index` from the set.
    pub fn remove(&mut self, index: usize) {
        self.0 &= !(1 << index);
    }
}

impl Debug for IndexSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexSet[")?;
        for (index, bit) in self.into_iter().enumerate() {
            if index > 0 {
                write!(f, ",")?;
            }
            write!(f, "{bit}")?;
        }
        write!(f, "]")
    }
}

impl FromIterator<usize> for IndexSet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = usize>,
    {
        let mut set = Self::empty();
        for index in iter {
            set |= Self::for_index(index);
        }
        set
    }
}

impl BitOr for IndexSet {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for IndexSet {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

pub struct IndexSetIter(IndexSet);

impl Iterator for IndexSetIter {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.first().inspect(|_| self.0 = self.0.without_first())
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.0.len();
        (n, Some(n))
    }
}

impl IntoIterator for IndexSet {
    type Item = usize;
    type IntoIter = IndexSetIter;
    fn into_iter(self) -> Self::IntoIter {
        IndexSetIter(self)
    }
}
