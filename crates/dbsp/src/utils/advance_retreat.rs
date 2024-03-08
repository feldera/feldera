use crate::utils::bytes_of;
use std::{
    cmp::min,
    mem::{size_of, MaybeUninit},
};

const DEFAULT_SMALL_LIMIT: usize = 8;

/// Reports the number of elements satisfying the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
pub fn advance<T, F>(slice: &[T], function: F) -> usize
where
    F: Fn(&T) -> bool,
{
    advance_raw::<T, F, DEFAULT_SMALL_LIMIT>(slice, function)
}

/// Reports the number of elements satisfying the predicate with the additional
/// ability to specify the limit for linear searches
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
pub fn advance_raw<T, F, const SMALL_LIMIT: usize>(slice: &[T], function: F) -> usize
where
    F: Fn(&T) -> bool,
{
    // Exponential search if the answer isn't within `SMALL_LIMIT`.
    if slice.len() > SMALL_LIMIT && function(&slice[SMALL_LIMIT]) {
        // Skip `slice[..SMALL_LIMIT]` outright, the above condition established
        // that nothing within it satisfies the predicate
        let mut index = SMALL_LIMIT + 1;

        // FIXME: This is kinda weird
        if index < slice.len() && function(&slice[index]) {
            // Advance in exponentially growing steps
            let mut step = 1;
            while index + step < slice.len() && function(&slice[index + step]) {
                index += step;
                step <<= 1;
            }

            // Advance in exponentially shrinking steps
            step >>= 1;
            while step > 0 {
                if index + step < slice.len() && function(&slice[index + step]) {
                    index += step;
                }
                step >>= 1;
            }

            index += 1;
        }

        index

    // If `slice[SMALL_LIMIT..]` doesn't satisfy the predicate, we can simply
    // perform a linear search on `slice[..SMALL_LIMIT]`
    } else {
        // Clamp to the length of the slice, this branch will also be hit if the slice
        // is smaller than SMALL_LIMIT
        let limit = min(slice.len(), SMALL_LIMIT);

        slice[..limit]
            .iter()
            .position(|x| !function(x))
            // If nothing within `slice[..limit]` satisfies the predicate, we can advance
            // past the searched prefix
            .unwrap_or(limit)
    }
}

/// Reports the number of elements satisfying the predicate in the suffix of
/// `slice`.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `retreat` to use exponential search to
/// count the number of elements in time logarithmic in the result.
pub fn retreat<T, F>(slice: &[T], function: F) -> usize
where
    F: Fn(&T) -> bool,
{
    retreat_raw::<T, F, DEFAULT_SMALL_LIMIT>(slice, function)
}

/// Reports the number of elements satisfying the predicate in the suffix of
/// `slice` with the additional ability to specify the limit for linear
/// searches.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `retreat` to use exponential search to
/// count the number of elements in time logarithmic in the result.
pub fn retreat_raw<T, F, const SMALL_LIMIT: usize>(slice: &[T], function: F) -> usize
where
    F: Fn(&T) -> bool,
{
    if slice.is_empty() {
        return 0;
    }

    let last_index = slice.len() - 1;

    // Exponential search if the answer isn't within `SMALL_LIMIT`.
    if slice.len() > SMALL_LIMIT && function(&slice[last_index - SMALL_LIMIT]) {
        // Skip `slice[..SMALL_LIMIT]` outright, the above condition established
        // that nothing within it satisfies the predicate
        let mut index = SMALL_LIMIT + 1;

        if index < slice.len() && function(&slice[last_index - index]) {
            // Advance in exponentially growing steps
            let mut step = 1;
            while index + step < slice.len() && function(&slice[last_index - (index + step)]) {
                index += step;
                step <<= 1;
            }

            // Advance in exponentially shrinking steps
            step >>= 1;
            while step > 0 {
                if index + step < slice.len() && function(&slice[last_index - (index + step)]) {
                    index += step;
                }
                step >>= 1;
            }

            index += 1;
        }

        index

    // If `slice[.. last_index - SMALL_LIMIT]` doesn't satisfy the predicate, we
    // can simply perform a linear search on `slice[last_index -
    // SMALL_LIMIT..]`
    } else {
        // Clamp to the length of the slice, this branch will also be hit if the slice
        // is smaller than SMALL_LIMIT
        let limit = min(last_index, SMALL_LIMIT);

        slice[last_index - limit..]
            .iter()
            .rev()
            .position(|x| !function(x))
            // If nothing within `slice[last_index - limit ..]` satisfies the predicate, we can
            // advance past the searched prefix
            .unwrap_or(limit + 1)
    }
}

pub fn dyn_advance<T>(slice: &[T], function: &dyn Fn(*const u8) -> bool) -> usize {
    advance_erased(bytes_of(slice), size_of::<T>(), function)
}

pub fn advance_erased(
    slice: &[MaybeUninit<u8>],
    size: usize,
    function: &dyn Fn(*const u8) -> bool,
) -> usize {
    if size == 0 {
        return 0;
    }

    let slice = SlicePtr::new(slice, size);

    // We have to use `.get_unchecked()` here since otherwise LLVM's not smart
    // enough to elide bounds checking (we still get checks in debug mode though)
    unsafe {
        // Exponential search if the answer isn't within `small_limit`.
        if slice.len() > DEFAULT_SMALL_LIMIT && function(slice.get_unchecked(DEFAULT_SMALL_LIMIT)) {
            // start with no advance
            let mut index = DEFAULT_SMALL_LIMIT + 1;
            if index < slice.len() && function(slice.get_unchecked(index)) {
                // advance in exponentially growing steps.
                let mut step = 1;
                while index + step < slice.len() && function(slice.get_unchecked(index + step)) {
                    index += step;
                    step <<= 1;
                }

                // advance in exponentially shrinking steps.
                step >>= 1;
                while step > 0 {
                    if index + step < slice.len() && function(slice.get_unchecked(index + step)) {
                        index += step;
                    }
                    step >>= 1;
                }

                index += 1;
            }

            index
        } else {
            let limit = min(slice.len(), DEFAULT_SMALL_LIMIT);
            for offset in 0..limit {
                if !function(slice.get_unchecked(offset)) {
                    return offset;
                }
            }

            limit
        }
    }
}

pub fn dyn_retreat<T>(slice: &[T], function: &dyn Fn(*const u8) -> bool) -> usize {
    retreat_erased(bytes_of(slice), size_of::<T>(), function)
}

pub fn retreat_erased(
    slice: &[MaybeUninit<u8>],
    size: usize,
    function: &dyn Fn(*const u8) -> bool,
) -> usize {
    if size == 0 {
        return 0;
    }
    if slice.is_empty() {
        return 0;
    }

    let slice = SlicePtr::new(slice, size);
    let last_index = slice.len() - 1;

    unsafe {
        // Exponential search if the answer isn't within `SMALL_LIMIT`.
        if slice.len() > DEFAULT_SMALL_LIMIT
            && function(slice.get_unchecked(last_index - DEFAULT_SMALL_LIMIT))
        {
            // Skip `slice[..SMALL_LIMIT]` outright, the above condition established
            // that nothing within it satisfies the predicate
            let mut index = DEFAULT_SMALL_LIMIT + 1;

            if index < slice.len() && function(slice.get_unchecked(last_index - index)) {
                // Advance in exponentially growing steps
                let mut step = 1;
                while index + step < slice.len()
                    && function(slice.get_unchecked(last_index - (index + step)))
                {
                    index += step;
                    step <<= 1;
                }

                // Advance in exponentially shrinking steps
                step >>= 1;
                while step > 0 {
                    if index + step < slice.len()
                        && function(slice.get_unchecked(last_index - (index + step)))
                    {
                        index += step;
                    }
                    step >>= 1;
                }

                index += 1;
            }

            index

            // If `slice[.. last_index - SMALL_LIMIT]` doesn't satisfy the
            // predicate, we can simply perform a linear search on
            // `slice[last_index - SMALL_LIMIT..]`
        } else {
            // Clamp to the length of the slice, this branch will also be hit if the slice
            // is smaller than SMALL_LIMIT
            let limit = min(last_index, DEFAULT_SMALL_LIMIT);

            for offset in 0..=limit {
                if !function(slice.get_unchecked(last_index - offset)) {
                    return offset;
                }
            }
            // If nothing within `slice[last_index - limit ..]` satisfies the predicate, we
            // can advance past the searched prefix
            limit + 1
        }
    }
}

struct SlicePtr {
    ptr: *const u8,
    elements: usize,
    element_size: usize,
}

impl SlicePtr {
    #[inline]
    fn new(slice: &[MaybeUninit<u8>], element_size: usize) -> Self {
        debug_assert!(slice.len() % element_size == 0);

        Self {
            ptr: slice.as_ptr().cast(),
            elements: slice.len() / element_size,
            element_size,
        }
    }

    #[inline]
    const fn len(&self) -> usize {
        self.elements
    }

    /*#[inline]
    const fn is_empty(&self) -> bool {
        self.elements == 0
    }*/

    #[inline]
    unsafe fn get_unchecked(&self, idx: usize) -> *const u8 {
        debug_assert!(idx < self.elements);
        unsafe { self.ptr.add(idx * self.element_size) }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::{
        advance, advance_erased, advance_retreat::DEFAULT_SMALL_LIMIT, bytes_of, retreat,
        retreat_erased,
    };
    use proptest::{
        arbitrary::any, collection::vec, prop_assert_eq, proptest, sample::SizeRange,
        strategy::Strategy, test_runner::TestCaseResult,
    };
    use std::mem::{align_of, size_of};

    const HALF: usize = usize::MAX / 2;

    #[test]
    fn advance_empty() {
        // Haystack that's smaller than `DEFAULT_SMALL_LIMIT`
        let haystack = &[false, false, false, false, false];
        assert_eq!(advance(haystack, |&x| x), 0);
        assert_eq!(retreat(haystack, |&x| x), 0);

        // Haystack that turns `false` before `DEFAULT_SMALL_LIMIT`
        let haystack = &[
            false, false, false, false, false, false, false, false, false, false,
        ];
        assert_eq!(advance(haystack, |&x| x), 0);
        assert_eq!(retreat(haystack, |&x| x), 0);
    }

    #[test]
    fn advance_small() {
        // Haystack that's smaller than `DEFAULT_SMALL_LIMIT`
        let haystack = &[true, true, false, false, false];
        assert_eq!(advance(haystack, |&x| x), 2);
        assert_eq!(retreat(haystack, |&x| !x), 3);

        // Haystack that turns `false` before `DEFAULT_SMALL_LIMIT`
        let haystack = &[
            true, true, true, false, false, false, false, false, false, false,
        ];
        assert_eq!(advance(haystack, |&x| x), 3);
        assert_eq!(retreat(haystack, |&x| !x), 7);
    }

    #[test]
    fn advance_medium() {
        // Haystack that's longer than `DEFAULT_SMALL_LIMIT`
        let haystack = &[
            true, true, true, true, true, true, true, true, true, true, false, false, false, false,
            false, false, false, false, false, false,
        ];
        assert_eq!(advance(haystack, |&x| x), 10);
        assert_eq!(retreat(haystack, |&x| !x), 10);
    }

    #[test]
    fn advance_erased_empty() {
        // Haystack that's smaller than `DEFAULT_SMALL_LIMIT`
        let haystack: &[u64] = &[5, 5, 5, 5, 5];

        let count = advance_erased(bytes_of(haystack), size_of::<u64>(), &|x| unsafe {
            let value = *x.cast::<u64>();
            assert_eq!(value, 5);
            value == 1
        });
        assert_eq!(count, 0);

        // Haystack that turns `5` before `DEFAULT_SMALL_LIMIT`
        let haystack: &[u64] = &[5, 5, 5, 5, 5, 5, 5, 5, 5, 5];

        let count = advance_erased(bytes_of(haystack), size_of::<u64>(), &|x| unsafe {
            let value = *x.cast::<u64>();
            assert_eq!(value, 5);
            value == 1
        });
        assert_eq!(count, 0);
    }

    #[test]
    fn advance_erased_small() {
        // Haystack that's smaller than `DEFAULT_SMALL_LIMIT`
        let haystack: &[u64] = &[1, 1, 568, 568, 568];

        let count = advance_erased(bytes_of(haystack), size_of::<u64>(), &|x| unsafe {
            let value = *x.cast::<u64>();
            assert!(value == 1 || value == 568);
            value == 1
        });
        assert_eq!(count, 2);

        // Haystack that turns false before `DEFAULT_SMALL_LIMIT`
        let haystack: &[u64] = &[1, 1, 1, 568, 568, 568, 568, 568, 568, 568];

        let count = advance_erased(bytes_of(haystack), size_of::<u64>(), &|x| unsafe {
            let value = *x.cast::<u64>();
            assert!(value == 1 || value == 568);
            value == 1
        });
        assert_eq!(count, 3);
    }

    #[test]
    fn advance_erased_medium() {
        // Haystack that's longer than `DEFAULT_SMALL_LIMIT`
        let haystack: &[u64] = &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 568, 568, 568];

        let count = advance_erased(bytes_of(haystack), size_of::<u64>(), &|x| unsafe {
            let value = *x.cast::<u64>();
            assert!(value == 1 || value == 568);
            value == 1
        });
        assert_eq!(count, 10);
    }

    fn haystack(
        length: impl Into<SizeRange>,
        value: impl Strategy<Value = usize>,
    ) -> impl Strategy<Value = Vec<usize>> {
        vec(value, length.into()).prop_map(|mut vec| {
            vec.sort();
            vec
        })
    }

    fn advance_test(needle: usize, haystack: &[usize]) -> TestCaseResult {
        let count = advance(haystack, |&x| x < needle);
        let expected = haystack
            .iter()
            .position(|&x| x >= needle)
            .unwrap_or(haystack.len());

        prop_assert_eq!(count, expected);
        Ok(())
    }

    fn retreat_test(needle: usize, haystack: &[usize]) -> TestCaseResult {
        let count = retreat(haystack, |&x| x > needle);
        let expected = haystack
            .iter()
            .rev()
            .position(|&x| x <= needle)
            .unwrap_or(haystack.len());

        prop_assert_eq!(count, expected);
        Ok(())
    }

    fn advance_erased_test(needle: usize, haystack: &[usize]) -> TestCaseResult {
        let count = advance_erased(bytes_of(haystack), size_of::<usize>(), &|x| unsafe {
            assert!(
                x as usize & (align_of::<usize>() - 1) == 0,
                "unaligned pointer",
            );
            *x.cast::<usize>() < needle
        });
        let expected = haystack
            .iter()
            .position(|&x| x >= needle)
            .unwrap_or(haystack.len());

        prop_assert_eq!(count, expected);
        Ok(())
    }

    fn retreat_erased_test(needle: usize, haystack: &[usize]) -> TestCaseResult {
        let count = retreat_erased(bytes_of(haystack), size_of::<usize>(), &|x| unsafe {
            assert!(
                x as usize & (align_of::<usize>() - 1) == 0,
                "unaligned pointer",
            );
            *x.cast::<usize>() > needle
        });
        let expected = haystack
            .iter()
            .rev()
            .position(|&x| x <= needle)
            .unwrap_or(haystack.len());

        prop_assert_eq!(count, expected);
        Ok(())
    }

    proptest! {
        #[test]
        fn advance_less_than(needle in any::<usize>(), haystack in haystack(0..100_000usize, any::<usize>())) {
            advance_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_less_than(needle in any::<usize>(), haystack in haystack(0..100_000usize, any::<usize>())) {
            retreat_test(needle, &haystack)?;
        }

        // Force `advance()` to stop in the beginning of the haystack.
        #[test]
        fn advance_less_than_unsat1(needle in ..HALF, haystack in haystack(0..100_000usize, HALF..)) {
            advance_test(needle, &haystack)?;
        }

        // Force `advance()` to search the entire haystack
        #[test]
        fn advance_less_than_unsat2(needle in HALF.., haystack in haystack(0..100_000usize, ..HALF)) {
            advance_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_less_than_unsat1(needle in ..HALF, haystack in haystack(0..100_000usize, HALF..)) {
            retreat_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_less_than_unsat2(needle in HALF.., haystack in haystack(0..100_000usize, ..HALF)) {
            retreat_test(needle, &haystack)?;
        }

        // Ensure that we check the case of the haystack being shorter than `DEFAULT_SMALL_LIMIT`
        #[test]
        fn advance_less_than_small(needle in any::<usize>(), haystack in haystack(0..=DEFAULT_SMALL_LIMIT, any::<usize>())) {
            advance_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_less_than_small(needle in any::<usize>(), haystack in haystack(0..=DEFAULT_SMALL_LIMIT, any::<usize>())) {
            retreat_test(needle, &haystack)?;
        }

        // Force `advance()` to search the entire haystack
        #[test]
        fn advance_less_than_small_unsat1(needle in ..HALF, haystack in haystack(0..=DEFAULT_SMALL_LIMIT, HALF..)) {
            advance_test(needle, &haystack)?;
        }

        #[test]
        fn advance_less_than_small_unsat2(needle in HALF.., haystack in haystack(0..=DEFAULT_SMALL_LIMIT, ..HALF)) {
            advance_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_less_than_small_unsat1(needle in ..HALF, haystack in haystack(0..=DEFAULT_SMALL_LIMIT, HALF..)) {
            retreat_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_less_than_small_unsat2(needle in HALF.., haystack in haystack(0..=DEFAULT_SMALL_LIMIT, ..HALF)) {
            retreat_test(needle, &haystack)?;
        }

        #[test]
        fn advance_erased_less_than(needle in any::<usize>(), haystack in haystack(0..100_000usize, any::<usize>())) {
            advance_erased_test(needle, &haystack)?;
        }

        // Force `advance_erased()` to search the entire haystack
        #[test]
        fn advance_erased_less_than_unsat(needle in ..HALF, haystack in haystack(0..100_000usize, HALF..)) {
            advance_erased_test(needle, &haystack)?;
        }

        // Ensure that we check the case of the haystack being shorter than `DEFAULT_SMALL_LIMIT`
        #[test]
        fn advance_erased_less_than_small(needle in any::<usize>(), haystack in haystack(0..=DEFAULT_SMALL_LIMIT, any::<usize>())) {
            advance_erased_test(needle, &haystack)?;
        }

        // Force `advance_erased()` to search the entire haystack
        #[test]
        fn advance_erased_less_than_small_unsat(needle in ..HALF, haystack in haystack(0..=DEFAULT_SMALL_LIMIT, HALF..)) {
            advance_erased_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_erased_less_than(needle in any::<usize>(), haystack in haystack(0..100_000usize, any::<usize>())) {
            retreat_erased_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_erased_less_than_unsat1(needle in HALF.., haystack in haystack(0..100_000usize, ..HALF)) {
            retreat_erased_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_erased_less_than_unsat2(needle in ..HALF, haystack in haystack(0..100_000usize, HALF..)) {
            retreat_erased_test(needle, &haystack)?;
        }

        // Ensure that we check the case of the haystack being shorter than `DEFAULT_SMALL_LIMIT`
        #[test]
        fn retreat_erased_less_than_small(needle in any::<usize>(), haystack in haystack(0..=DEFAULT_SMALL_LIMIT, any::<usize>())) {
            retreat_erased_test(needle, &haystack)?;
        }

        #[test]
        fn retreat_erased_less_than_small_unsat(needle in HALF.., haystack in haystack(0..=DEFAULT_SMALL_LIMIT, ..HALF)) {
            advance_erased_test(needle, &haystack)?;
        }

    }
}
