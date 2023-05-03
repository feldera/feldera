use crate::{
    codegen::{LayoutConfig, NativeLayout},
    ir::{LayoutId, RowLayout, RowLayoutCache},
};
use std::{
    cell::{Ref, RefCell},
    collections::BTreeMap,
    fmt::{self, Debug},
    rc::Rc,
};

/// A cache for [`NativeLayout`]s
#[derive(Debug, Clone)]
pub struct NativeLayoutCache {
    layout_cache: RowLayoutCache,
    layout_config: LayoutConfig,
    inner: Rc<RefCell<NativeLayoutCacheInner>>,
}

impl NativeLayoutCache {
    /// Create a new native layout cache
    pub(crate) fn new(layout_cache: RowLayoutCache, layout_config: LayoutConfig) -> Self {
        Self {
            layout_cache,
            layout_config,
            inner: Rc::new(RefCell::new(NativeLayoutCacheInner::new())),
        }
    }

    /// Get the [`NativeLayout`] for the given [`LayoutId`]
    pub fn layout_of(&self, layout_id: LayoutId) -> Ref<'_, NativeLayout> {
        self.get_layouts(layout_id).0
    }

    /// Get the [`RowLayout`] for the given [`LayoutId`]
    pub fn row_layout(&self, layout_id: LayoutId) -> Ref<'_, RowLayout> {
        self.layout_cache.get(layout_id)
    }

    /// Gets the [`RowLayoutCache`] native layout cache
    pub fn row_layout_cache(&self) -> &RowLayoutCache {
        &self.layout_cache
    }

    /// Get both the [`NativeLayout`] and [`RowLayout`] for the given
    /// [`LayoutId`]
    pub fn get_layouts(&self, layout_id: LayoutId) -> (Ref<'_, NativeLayout>, Ref<'_, RowLayout>) {
        let row_layout = self.layout_cache.get(layout_id);
        let native_layout = (|| {
            if let Ok(native_layout) =
                Ref::filter_map(self.inner.borrow(), |inner| inner.layouts.get(&layout_id))
            {
                return native_layout;
            }

            self.inner.borrow_mut().layouts.insert(
                layout_id,
                NativeLayout::from_row(&row_layout, &self.layout_config),
            );
            Ref::map(self.inner.borrow(), |inner| &inner.layouts[&layout_id])
        })();

        (native_layout, row_layout)
    }

    pub fn print_layouts(&self) {
        let layouts = Ref::map(self.inner.borrow(), |layouts| &layouts.layouts);
        println!("total layouts: {}", layouts.len());

        for (&layout_id, layout) in layouts.iter() {
            println!(
                "{layout_id}: {}\n  size {}\n  align {}\n  \
                padding bytes {}\n  columns {}\n  nullable columns {}\n  \
                bitsets {}\n  padding gaps {}",
                self.row_layout(layout_id),
                layout.size(),
                layout.align(),
                layout.total_padding(),
                layout.total_columns(),
                layout.nullable_columns(),
                layout.total_bitsets(),
                layout.padding_bytes().len(),
            );
        }
    }
}

struct NativeLayoutCacheInner {
    layouts: BTreeMap<LayoutId, NativeLayout>,
}

impl NativeLayoutCacheInner {
    const fn new() -> Self {
        Self {
            layouts: BTreeMap::new(),
        }
    }
}

impl Debug for NativeLayoutCacheInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(&self.layouts).finish()
    }
}
