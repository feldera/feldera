//! This module contains the [`RowLayoutCache`] which allows us to only perform
//! fairly expensive layout calculations once for each layout

use crate::ir::{types::RowLayout, LayoutId};
use std::{
    cell::{Ref, RefCell},
    fmt::{self, Debug},
    rc::Rc,
};

#[derive(Clone)]
pub struct RowLayoutCache {
    inner: Rc<RefCell<RowLayoutCacheInner>>,
}

impl RowLayoutCache {
    /// Creates a new row layout cache
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Rc::new(RefCell::new(RowLayoutCacheInner::with_capacity(capacity))),
        }
    }

    pub fn contains(&self, layout_id: LayoutId) -> bool {
        (layout_id.into_inner() as usize) < self.inner.borrow().layouts.len()
    }

    pub fn get(&self, layout_id: LayoutId) -> Ref<'_, RowLayout> {
        Ref::map(self.inner.borrow(), |cache| cache.get(layout_id))
    }

    pub fn add(&self, layout: RowLayout) -> LayoutId {
        self.inner.borrow_mut().add(layout)
    }

    pub fn unit(&self) -> LayoutId {
        self.inner.borrow().unit_layout
    }

    pub fn layouts(&self) -> Ref<'_, Vec<RowLayout>> {
        Ref::map(self.inner.borrow(), |cache| &cache.layouts)
    }

    pub fn with_layouts<F>(&self, mut with_layouts: F)
    where
        F: FnMut(LayoutId, &RowLayout),
    {
        let inner = self.inner.borrow();
        for (layout_id, layout) in inner.layouts.iter().enumerate() {
            with_layouts(LayoutId::new(layout_id as u32 + 1), layout);
        }
    }
}

impl Default for RowLayoutCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for RowLayoutCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("RowLayoutCache");
        if let Ok(inner) = self.inner.try_borrow() {
            debug.field("layouts", &*inner);
        } else {
            debug.field("layouts", &"{ ... }");
        }
        debug.finish()
    }
}

struct RowLayoutCacheInner {
    layouts: Vec<RowLayout>,
    unit_layout: LayoutId,
}

impl RowLayoutCacheInner {
    fn with_capacity(capacity: usize) -> Self {
        let mut this = Self {
            layouts: Vec::with_capacity(capacity + 1),
            unit_layout: LayoutId::MAX,
        };

        let unit_layout = this.add(RowLayout::unit());
        this.unit_layout = unit_layout;

        this
    }

    fn add(&mut self, layout: RowLayout) -> LayoutId {
        // Get the layout from cache if possible
        if let Some(layout_idx) = self.layouts.iter().position(|cached| cached == &layout) {
            debug_assert!(
                layout_idx < u32::MAX as usize,
                "created more than {} layouts",
                u32::MAX,
            );

            LayoutId::new(layout_idx as u32 + 1)

        // Insert the layout if it doesn't exist yet
        } else {
            debug_assert!(
                self.layouts.len() < u32::MAX as usize,
                "created more than {} layouts",
                u32::MAX,
            );
            let layout_idx = self.layouts.len() as u32;
            self.layouts.push(layout);

            LayoutId::new(layout_idx + 1)
        }
    }

    fn get(&self, layout_id: LayoutId) -> &RowLayout {
        &self.layouts[layout_id.into_inner() as usize - 1]
    }
}

impl Debug for RowLayoutCacheInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.layouts
                    .iter()
                    .enumerate()
                    .map(|(layout_idx, layout)| (LayoutId::new(layout_idx as u32 + 1), layout)),
            )
            .finish()
    }
}
