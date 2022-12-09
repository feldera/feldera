use crate::ir::types::RowLayout;
use std::{
    cell::{Ref, RefCell},
    collections::BTreeMap,
    fmt::{self, Debug, Display},
    rc::Rc,
};

#[derive(Clone, Debug)]
pub struct LayoutCache {
    inner: Rc<RefCell<LayoutCacheInner>>,
}

impl LayoutCache {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(LayoutCacheInner::new())),
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
}

struct LayoutCacheInner {
    idx_to_id: BTreeMap<u32, LayoutId>,
    id_to_idx: BTreeMap<LayoutId, u32>,
    layouts: Vec<RowLayout>,
    layout_id: u32,

    unit_layout: LayoutId,
}

impl LayoutCacheInner {
    fn new() -> Self {
        let mut this = Self {
            idx_to_id: BTreeMap::new(),
            id_to_idx: BTreeMap::new(),
            layouts: Vec::new(),
            layout_id: 0,
            unit_layout: LayoutId(0),
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

            let layout_id = {
                let id = self.layout_id;
                self.layout_id += 1;
                LayoutId(id)
            };

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

impl Debug for LayoutCacheInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(&self.layouts).finish()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct LayoutId(u32);

impl Debug for LayoutId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for LayoutId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "layout{}", self.0)
    }
}
