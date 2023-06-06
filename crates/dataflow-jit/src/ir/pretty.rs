pub use pretty::{termcolor, Arena, DocAllocator, DocBuilder};

use crate::ir::RowLayoutCache;

/// The default rendering width to use, 80 characters
pub const DEFAULT_WIDTH: usize = 80;

pub trait Pretty<'a, D, A = ()>
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, allocator: &'a D, layout_cache: &RowLayoutCache) -> DocBuilder<'a, D, A>;
}
