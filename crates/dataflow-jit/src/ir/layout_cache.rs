//! This module contains the [`RowLayoutCache`] which allows us to only perform
//! fairly expensive layout calculations once for each layout

use crate::ir::{types::RowLayout, LayoutId, LayoutIdGen};
use std::{
    cell::{Ref, RefCell},
    collections::BTreeMap,
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
        Self {
            inner: Rc::new(RefCell::new(RowLayoutCacheInner::new())),
        }
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
        for (&layout_id, &idx) in inner.id_to_idx.iter() {
            with_layouts(layout_id, &inner.layouts[idx as usize]);
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
    idx_to_id: BTreeMap<u32, LayoutId>,
    id_to_idx: BTreeMap<LayoutId, u32>,
    layouts: Vec<RowLayout>,
    layout_id: LayoutIdGen,
    unit_layout: LayoutId,
}

impl RowLayoutCacheInner {
    fn new() -> Self {
        let mut this = Self {
            idx_to_id: BTreeMap::new(),
            id_to_idx: BTreeMap::new(),
            layouts: Vec::new(),
            layout_id: LayoutIdGen::new(),
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
                layout_idx <= u32::MAX as usize,
                "created more than {} layouts",
                u32::MAX,
            );

            *self
                .idx_to_id
                .get(&(layout_idx as u32))
                .expect("attempted to get layout that doesn't exist")

        // Insert the layout if it doesn't exist yet
        } else {
            debug_assert!(
                self.layouts.len() <= u32::MAX as usize,
                "created more than {} layouts",
                u32::MAX,
            );
            let layout_idx = self.layouts.len() as u32;

            let layout_id = self.layout_id.next();
            self.idx_to_id.insert(layout_idx, layout_id);
            self.id_to_idx.insert(layout_id, layout_idx);
            self.layouts.push(layout);

            layout_id
        }
    }

    fn get(&self, layout_id: LayoutId) -> &RowLayout {
        let layout_idx = *self
            .id_to_idx
            .get(&layout_id)
            .expect("attempted to get layout that doesn't exist") as usize;

        &self.layouts[layout_idx]
    }
}

impl Debug for RowLayoutCacheInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(
                self.id_to_idx
                    .iter()
                    .map(|(layout_id, &idx)| (layout_id, &self.layouts[idx as usize])),
            )
            .finish()
    }
}
