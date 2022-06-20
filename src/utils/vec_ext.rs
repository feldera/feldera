/// Extension methods for [`Vec`]
pub(crate) trait VecExt<T> {
    /// Returns the unused capacity of the current [`Vec`],
    /// equivalent to `vec.capacity() - vec.len()`
    fn spare_capacity(&self) -> usize;

    /// Returns `true` if the current [`Vec`] has any unused capacity
    /// available
    #[inline]
    fn has_spare_capacity(&self) -> bool {
        self.spare_capacity() != 0
    }

    /// Pushes to a [`Vec`] without reallocating, equivalent
    /// to [`Vec::push()`] but without any checks
    ///
    /// # Safety
    ///
    /// - `vec` must have a capacity greater than zero
    /// - `vec`'s length must be less than it's capacity
    ///
    /// # Examples
    ///
    /// ```rust
    /// let mut vec = Vec::with_capacity(1);
    /// unsafe { vec.push_unchecked("something") };
    ///
    /// assert_eq!(vec.len(), 1);
    /// assert_eq!(vec.capacity(), 1);
    /// assert_eq!(&vec, &["something"]);
    /// ```
    ///
    unsafe fn push_unchecked(&mut self, elem: T);
}

impl<T> VecExt<T> for Vec<T> {
    #[inline]
    fn spare_capacity(&self) -> usize {
        self.capacity() - self.len()
    }

    #[inline(always)]
    unsafe fn push_unchecked(&mut self, elem: T) {
        debug_assert_ne!(self.capacity(), 0, "cannot push to a vec of length zero");
        debug_assert!(
            self.len() < self.capacity(),
            "cannot push to a vec without spare capacity",
        );

        let len = self.len();
        self.spare_capacity_mut().get_unchecked_mut(0).write(elem);
        self.set_len(len + 1);
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::VecExt;

    #[test]
    fn spare_capacity() {
        let mut vec = Vec::with_capacity(0);
        assert_eq!(vec.spare_capacity(), 0);
        assert!(!vec.has_spare_capacity());

        vec.reserve_exact(10);
        assert_eq!(vec.spare_capacity(), 10);
        assert!(vec.has_spare_capacity());

        for i in 0..9 {
            vec.push(i);
        }
        assert_eq!(vec.spare_capacity(), 1);
        assert!(vec.has_spare_capacity());

        vec.push(0);
        assert_eq!(vec.spare_capacity(), 0);
        assert!(!vec.has_spare_capacity());
    }

    #[test]
    fn push_unchecked() {
        let mut vec = Vec::with_capacity(100);
        assert!(vec.is_empty());

        for i in 0..100 {
            unsafe { vec.push_unchecked(i) };
        }

        assert_eq!(vec.len(), 100);
        assert_eq!(vec.capacity(), 100);
        assert_eq!(vec, (0..100).collect::<Vec<_>>());
    }

    // This test is gated under `debug_assertions` since we use `debug_assert!()`
    // for checking `push_unchecked()`'s invariants
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic = "cannot push to a vec of length zero"]
    fn push_unchecked_zero_capacity() {
        let mut empty = Vec::with_capacity(0);
        unsafe { empty.push_unchecked(false) };
    }

    // This test is gated under `debug_assertions` since we use `debug_assert!()`
    // for checking `push_unchecked()`'s invariants
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic = "cannot push to a vec without spare capacity"]
    fn push_unchecked_full() {
        let mut full = Vec::with_capacity(10);
        for i in 0..10 {
            full.push(i);
        }

        unsafe { full.push_unchecked(0) };
    }
}
