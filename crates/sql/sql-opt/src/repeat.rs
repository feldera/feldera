//! Run an optimizer pass until it preserves the circuit or reaches its iteration limit.

pub enum RepeatResult<T> {
    Finished(T),
    DidNotConverge { last: T, iterations: usize },
}

/// The caller compares the identities of the operators and declarations after
/// each pass. Iteration numbers are zero-based in the optimizer trace.
pub fn repeat<T, E>(
    mut circuit: T,
    largest_component: usize,
    requested: Option<usize>,
    mut step: impl FnMut(T, usize) -> Result<(T, bool), E>,
) -> Result<RepeatResult<T>, E> {
    let limit = largest_component.max(10);
    for index in 0..limit {
        let (next, changed) = step(circuit, index)?;
        circuit = next;
        if !changed || requested == Some(index + 1) {
            return Ok(RepeatResult::Finished(circuit));
        }
    }
    Ok(RepeatResult::DidNotConverge {
        last: circuit,
        iterations: limit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_stop_or_convergence_wins_on_the_last_iteration() {
        for (requested, stable_at, finished) in [
            (Some(1), None, true),
            (Some(10), None, true),
            (None, Some(9), true),
            (Some(11), None, false),
        ] {
            let result = repeat(0, 3, requested, |state, index| {
                assert_eq!(state, index);
                Ok::<_, ()>((state + 1, Some(index) != stable_at))
            })
            .unwrap();
            match result {
                RepeatResult::Finished(steps) => {
                    assert!(finished);
                    assert_eq!(steps, requested.unwrap_or(10));
                }
                RepeatResult::DidNotConverge { last, iterations } => {
                    assert!(!finished);
                    assert_eq!((last, iterations), (10, 10));
                }
            }
        }
    }
}
