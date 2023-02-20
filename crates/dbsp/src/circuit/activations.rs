use size_of::SizeOf;
use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::BinaryHeap,
    mem::swap,
    rc::Rc,
    time::{Duration, Instant},
};

/// A capability to activate a specific path.
#[derive(Clone, Debug, SizeOf)]
pub struct Activator {
    path: Vec<usize>,
    queue: Rc<RefCell<Activations>>,
}

impl Activator {
    /// Creates a new activation handle
    pub fn new(path: &[usize], queue: Rc<RefCell<Activations>>) -> Self {
        Self {
            path: path.to_vec(),
            queue,
        }
    }

    /// Activates the associated path.
    pub fn activate(&self) {
        self.queue.borrow_mut().activate(&self.path);
    }

    /// Activates the associated path after a specified duration.
    pub fn activate_after(&self, delay: Duration) {
        if delay.is_zero() {
            self.activate();
        } else {
            self.queue.borrow_mut().activate_after(&self.path, delay);
        }
    }
}

/// Minimally allocating activation tracker.
#[derive(Debug, SizeOf)]
pub struct Activations {
    clean: usize,

    /// `(offset, length)`
    bounds: Vec<(usize, usize)>,
    /// A vec that contains the path segments stored in `bounds`
    slices: Vec<usize>,
    buffer: Vec<usize>,

    // Delayed activations.
    timer: Instant,
    // TODO: The paths here could be stored in `slices` or we could even just ditch the
    //       `Vec<usize>` path thing outright and just use `Rc<[usize]>` or something
    queue: BinaryHeap<Reverse<(Duration, Vec<usize>)>>,
}

impl Activations {
    /// Creates a new activation tracker.
    pub fn new(timer: Instant) -> Self {
        Self {
            clean: 0,
            bounds: Vec::new(),
            slices: Vec::new(),
            buffer: Vec::new(),
            timer,
            queue: BinaryHeap::new(),
        }
    }

    /// Activates the task addressed by `path`.
    pub fn activate(&mut self, path: &[usize]) {
        self.bounds.push((self.slices.len(), path.len()));
        self.slices.extend(path);
    }

    /// Schedules a future activation for the task addressed by `path`.
    pub fn activate_after(&mut self, path: &[usize], delay: Duration) {
        // TODO: We could have a minimum delay and immediately schedule anything less
        // than that delay.
        if delay == Duration::new(0, 0) {
            self.activate(path);
        } else {
            let moment = self.timer.elapsed() + delay;
            self.queue.push(Reverse((moment, path.to_vec())));
        }
    }

    /// Discards the current active set and presents the next active set.
    pub fn advance(&mut self) {
        // Drain timer-based activations.
        let now = self.timer.elapsed();
        while self.queue.peek().map(|Reverse((t, _))| t <= &now) == Some(true) {
            let Reverse((_, path)) = self.queue.pop().unwrap();
            self.activate(&path);
        }

        self.bounds.drain(..self.clean);

        {
            // Scoped, to allow borrow to drop.
            let slices = &self.slices;
            self.bounds
                .sort_by_key(|&(offset, len)| &slices[offset..offset + len]);
            self.bounds
                .dedup_by_key(|&mut (offset, len)| &slices[offset..offset + len]);
        }

        // Compact the slices.
        self.buffer.clear();
        for (offset, length) in self.bounds.iter_mut() {
            self.buffer.extend(&self.slices[*offset..*offset + *length]);
            *offset = self.buffer.len() - *length;
        }
        swap(&mut self.buffer, &mut self.slices);

        self.clean = self.bounds.len();
    }

    /// Maps a function across activated paths.
    pub fn map_active(&self, logic: impl Fn(&[usize])) {
        for &(offset, length) in self.bounds.iter() {
            logic(&self.slices[offset..offset + length]);
        }
    }

    /// Sets as active any symbols that follow `path`.
    pub fn for_extensions(&self, path: &[usize], mut action: impl FnMut(usize)) {
        let position = self.bounds[..self.clean]
            .binary_search_by_key(&path, |&(offset, len)| &self.slices[offset..offset + len]);
        let position = match position {
            Ok(x) => x,
            Err(x) => x,
        };

        let mut previous = None;
        self.bounds
            .iter()
            .cloned()
            .skip(position)
            .map(|(offset, len)| &self.slices[offset..offset + len])
            .take_while(|x| x.starts_with(path))
            .for_each(|x| {
                // push non-empty, non-duplicate extensions.
                if let Some(extension) = x.get(path.len()) {
                    if previous != Some(*extension) {
                        action(*extension);
                        previous = Some(*extension);
                    }
                }
            });
    }

    /// Time until next scheduled event.
    ///
    /// This method should be used before putting a worker thread to sleep, as
    /// it indicates the amount of time before the thread should be unparked
    /// for the next scheduled activation.
    pub fn empty_for(&self) -> Option<Duration> {
        if !self.bounds.is_empty() {
            Some(Duration::ZERO)
        } else {
            self.queue.peek().map(|&Reverse((time, _))| {
                let elapsed = self.timer.elapsed();
                if time < elapsed {
                    Duration::ZERO
                } else {
                    time - elapsed
                }
            })
        }
    }
}
