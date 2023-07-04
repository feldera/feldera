use crate::ir::{
    exprs::Constant,
    nodes::StreamLayout,
    pretty::{DocAllocator, DocBuilder, Pretty},
    validate::{ValidationError, ValidationResult},
    NodeId, RowLayout, RowLayoutCache,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub struct StreamLiteral {
    /// The literal's layout
    layout: StreamLayout,
    /// The values within the stream
    value: StreamCollection,
}

impl StreamLiteral {
    /// Create a new stream literal
    pub const fn new(layout: StreamLayout, value: StreamCollection) -> Self {
        Self { layout, value }
    }

    /// Create an empty stream literal
    pub const fn empty(layout: StreamLayout) -> Self {
        let value = match layout {
            StreamLayout::Set(..) => StreamCollection::Set(Vec::new()),
            StreamLayout::Map(..) => StreamCollection::Map(Vec::new()),
        };

        Self { layout, value }
    }

    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }

    pub const fn value(&self) -> &StreamCollection {
        &self.value
    }

    pub fn layout_mut(&mut self) -> &mut StreamLayout {
        &mut self.layout
    }

    pub fn value_mut(&mut self) -> &mut StreamCollection {
        &mut self.value
    }

    pub fn is_set(&self) -> bool {
        self.layout.is_set()
    }

    pub fn is_map(&self) -> bool {
        self.layout.is_map()
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn consolidate(&mut self) {
        self.value.consolidate();
    }

    pub fn to_consolidated(&self) -> Self {
        Self {
            layout: self.layout,
            value: self.value.to_consolidated(),
        }
    }

    pub(crate) fn validate_layout(
        &self,
        node_id: NodeId,
        layout_cache: &RowLayoutCache,
        errors: &mut Vec<ValidationError>,
    ) {
        match self.value() {
            StreamCollection::Set(set) => {
                let key_layout = self.layout().expect_set(
                    "got a ConstantStream with a map layout that contains a set constant",
                );
                let key_layout = layout_cache.get(key_layout);

                for (key, _diff) in set {
                    if let Err(error) = key.validate_layout(node_id, &key_layout) {
                        errors.push(error);
                    }
                }
            }

            StreamCollection::Map(map) => {
                let (key_layout, value_layout) = self.layout().expect_map(
                    "got a ConstantStream with a set layout that contains a map constant",
                );
                let (key_layout, value_layout) =
                    (layout_cache.get(key_layout), layout_cache.get(value_layout));

                for (key, value, _diff) in map {
                    if let Err(error) = key.validate_layout(node_id, &key_layout) {
                        errors.push(error);
                    }

                    if let Err(error) = value.validate_layout(node_id, &value_layout) {
                        errors.push(error);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub enum StreamCollection {
    Set(Vec<(RowLiteral, i32)>),
    Map(Vec<(RowLiteral, RowLiteral, i32)>),
}

impl StreamCollection {
    pub fn len(&self) -> usize {
        match self {
            Self::Set(set) => set.len(),
            Self::Map(map) => map.len(),
        }
    }

    pub const fn empty(layout: StreamLayout) -> StreamCollection {
        match layout {
            StreamLayout::Set(_) => Self::Set(Vec::new()),
            StreamLayout::Map(..) => Self::Map(Vec::new()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn consolidate(&mut self) {
        match self {
            Self::Set(set) => {
                // FIXME: We really should be sorting by the criteria that the
                // runtime rows will be sorted by so we have less work to do at
                // runtime, but technically any sorting criteria works as long
                // as it's consistent and allows us to deduplicate the stream
                set.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Deduplicate rows and combine their weights
                set.dedup_by(|(a, weight_a), (b, weight_b)| {
                    if a == b {
                        *weight_b = weight_b
                            .checked_add(*weight_a)
                            .expect("weight overflow in constant stream");

                        true
                    } else {
                        false
                    }
                });

                // Remove all zero weights
                set.retain(|&(_, weight)| weight != 0);
            }

            Self::Map(map) => {
                // FIXME: We really should be sorting by the criteria that the
                // runtime rows will be sorted by so we have less work to do at
                // runtime, but technically any sorting criteria works as long
                // as it's consistent and allows us to deduplicate the stream
                map.sort_by(|(key_a, value_a, _), (key_b, value_b, _)| {
                    key_a.cmp(key_b).then_with(|| value_a.cmp(value_b))
                });

                // Deduplicate rows and combine their weights
                map.dedup_by(|(key_a, value_a, weight_a), (key_b, value_b, weight_b)| {
                    if key_a == key_b && value_a == value_b {
                        *weight_b = weight_b
                            .checked_add(*weight_a)
                            .expect("weight overflow in constant stream");

                        true
                    } else {
                        false
                    }
                });

                // Remove all zero weights
                map.retain(|&(_, _, weight)| weight != 0);
            }
        }
    }

    pub fn to_consolidated(&self) -> Self {
        let mut this = self.clone();
        this.consolidate();
        this
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &StreamCollection
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        match self {
            StreamCollection::Set(set) => alloc.intersperse(
                set.iter().map(|(key, weight)| {
                    key.pretty(alloc, cache)
                        .append(alloc.text(","))
                        .append(alloc.space())
                        .append(alloc.text(format!("{weight:+}")))
                        .parens()
                }),
                alloc.text(",").append(alloc.line()),
            ),
            StreamCollection::Map(map) => alloc.intersperse(
                map.iter().map(|(key, value, weight)| {
                    key.pretty(alloc, cache)
                        .append(alloc.text(","))
                        .append(alloc.space())
                        .append(value.pretty(alloc, cache))
                        .append(alloc.text(","))
                        .append(alloc.space())
                        .append(alloc.text(format!("{weight:+}")))
                        .parens()
                }),
                alloc.text(",").append(alloc.line()),
            ),
        }
        .braces()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub struct RowLiteral {
    rows: Vec<NullableConstant>,
}

impl RowLiteral {
    #[inline]
    pub fn new(rows: Vec<NullableConstant>) -> Self {
        Self { rows }
    }

    #[inline]
    pub fn rows(&self) -> &[NullableConstant] {
        &self.rows
    }

    #[inline]
    pub fn iter(&self) -> <&[NullableConstant] as IntoIterator>::IntoIter {
        self.into_iter()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    // FIXME: Return `ValidationError`s instead of panicking
    #[track_caller]
    pub(crate) fn validate_layout(&self, node_id: NodeId, layout: &RowLayout) -> ValidationResult {
        assert_eq!(
            layout.len(),
            self.len(),
            "mismatched column count in ConstantStream {node_id}, expected {} column{} but got {}",
            layout.len(),
            if layout.len() == 1 { "" } else { "s" },
            self.len(),
        );

        for (idx, (column, (column_ty, nullable))) in self.iter().zip(layout.iter()).enumerate() {
            assert_eq!(
                column.is_nullable(),
                nullable,
                "column {idx} of ConstantStream {node_id} has mismatched nullability, \
                column {idx} is{} nullable while the constant's {idx} column is{} nullable",
                if nullable { "" } else { " not" },
                if column.is_nullable() { "" } else { " not" },
            );

            match column {
                NullableConstant::NonNull(value) => {
                    assert_eq!(
                        value.column_type(),
                        column_ty,
                        "column {idx} of constant stream should have type of {column_ty}, got {}",
                        value.column_type(),
                    );
                }

                NullableConstant::Nullable(Some(value)) => {
                    assert_eq!(
                        value.column_type(),
                        column_ty,
                        "column {idx} of constant stream should have type of {column_ty}, got {}",
                        value.column_type(),
                    );
                }

                // Null values have the correct type
                NullableConstant::Nullable(None) => {}
            }
        }

        Ok(())
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &RowLiteral
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc
            .intersperse(
                self.rows.iter().map(|column| column.pretty(alloc, cache)),
                alloc.text(",").append(alloc.space()),
            )
            .braces()
    }
}

impl IntoIterator for RowLiteral {
    type Item = NullableConstant;
    type IntoIter = <Vec<NullableConstant> as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl<'a> IntoIterator for &'a RowLiteral {
    type Item = &'a NullableConstant;
    type IntoIter = <&'a [NullableConstant] as IntoIterator>::IntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.rows().iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub enum NullableConstant {
    NonNull(Constant),
    Nullable(Option<Constant>),
}

impl NullableConstant {
    #[inline]
    pub const fn null() -> Self {
        Self::Nullable(None)
    }

    /// Returns `true` if the nullable constant is [`NonNull`].
    ///
    /// [`NonNull`]: NullableConstant::NonNull
    #[must_use]
    #[inline]
    pub const fn is_non_null(&self) -> bool {
        matches!(self, Self::NonNull(..))
    }

    /// Returns `true` if the nullable constant is [`Nullable`].
    ///
    /// [`Nullable`]: NullableConstant::Nullable
    #[must_use]
    #[inline]
    pub const fn is_nullable(&self) -> bool {
        matches!(self, Self::Nullable(..))
    }
}

impl<'a, D, A> Pretty<'a, D, A> for &NullableConstant
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized + 'a,
    DocBuilder<'a, D, A>: Clone,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        match self {
            NullableConstant::NonNull(value) => value.pretty(alloc, cache),
            NullableConstant::Nullable(Some(value)) => {
                alloc.text("?").append(value.pretty(alloc, cache))
            }
            NullableConstant::Nullable(None) => alloc.text("null"),
        }
    }
}
