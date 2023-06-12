use crate::ir::{
    exprs::{
        BinaryOp, Call, Cast, Constant, Copy, CopyRowTo, Drop, Expr, IsNull, Load, Nop, NullRow,
        RowOrScalar, Select, SetNull, Store, UnaryOp, Uninit, UninitRow,
    },
    ExprId, LayoutId, RValue,
};

pub trait ExprVisitor {
    fn visit_call(&mut self, _call: &Call) {}
    fn visit_cast(&mut self, _cast: &Cast) {}
    fn visit_load(&mut self, _load: &Load) {}
    fn visit_store(&mut self, _store: &Store) {}
    fn visit_select(&mut self, _select: &Select) {}
    fn visit_is_null(&mut self, _is_null: &IsNull) {}
    fn visit_bin_op(&mut self, _binop: &BinaryOp) {}
    fn visit_copy(&mut self, _copy: &Copy) {}
    fn visit_unary_op(&mut self, _unary_op: &UnaryOp) {}
    fn visit_null_row(&mut self, _null_row: &NullRow) {}
    fn visit_set_null(&mut self, _set_null: &SetNull) {}
    fn visit_constant(&mut self, _constant: &Constant) {}
    fn visit_copy_row_to(&mut self, _copy_row_to: &CopyRowTo) {}
    fn visit_uninit_row(&mut self, _uninit_row: &UninitRow) {}
    fn visit_uninit(&mut self, _uninit: &Uninit) {}
    fn visit_noop(&mut self, _noop: &Nop) {}
    fn visit_drop(&mut self, _drop: &Drop) {}
}

pub trait MutExprVisitor {
    fn visit_call(&mut self, _call: &mut Call) {}
    fn visit_cast(&mut self, _cast: &mut Cast) {}
    fn visit_load(&mut self, _load: &mut Load) {}
    fn visit_store(&mut self, _store: &mut Store) {}
    fn visit_select(&mut self, _select: &mut Select) {}
    fn visit_is_null(&mut self, _is_null: &mut IsNull) {}
    fn visit_bin_op(&mut self, _binop: &mut BinaryOp) {}
    fn visit_copy(&mut self, _copy: &mut Copy) {}
    fn visit_unary_op(&mut self, _unary_op: &mut UnaryOp) {}
    fn visit_null_row(&mut self, _null_row: &mut NullRow) {}
    fn visit_set_null(&mut self, _set_null: &mut SetNull) {}
    fn visit_constant(&mut self, _constant: &mut Constant) {}
    fn visit_copy_row_to(&mut self, _copy_row_to: &mut CopyRowTo) {}
    fn visit_uninit_row(&mut self, _uninit_row: &mut UninitRow) {}
    fn visit_uninit(&mut self, _uninit: &mut Uninit) {}
    fn visit_noop(&mut self, _noop: &mut Nop) {}
    fn visit_drop(&mut self, _drop: &mut Drop) {}
}

impl Expr {
    pub fn apply<V>(&self, visitor: &mut V)
    where
        V: ExprVisitor + ?Sized,
    {
        match self {
            Self::Call(call) => visitor.visit_call(call),
            Self::Cast(cast) => visitor.visit_cast(cast),
            Self::Load(load) => visitor.visit_load(load),
            Self::Store(store) => visitor.visit_store(store),
            Self::Select(select) => visitor.visit_select(select),
            Self::IsNull(is_null) => visitor.visit_is_null(is_null),
            Self::BinOp(binop) => visitor.visit_bin_op(binop),
            Self::Copy(copy) => visitor.visit_copy(copy),
            Self::UnaryOp(unary_op) => visitor.visit_unary_op(unary_op),
            Self::NullRow(null_row) => visitor.visit_null_row(null_row),
            Self::SetNull(set_null) => visitor.visit_set_null(set_null),
            Self::Constant(constant) => visitor.visit_constant(constant),
            Self::CopyRowTo(copy_row_to) => visitor.visit_copy_row_to(copy_row_to),
            Self::UninitRow(uninit_row) => visitor.visit_uninit_row(uninit_row),
            Self::Uninit(uninit) => visitor.visit_uninit(uninit),
            Self::Nop(noop) => visitor.visit_noop(noop),
            Self::Drop(drop) => visitor.visit_drop(drop),
        }
    }

    pub fn apply_mut<V>(&mut self, visitor: &mut V)
    where
        V: MutExprVisitor + ?Sized,
    {
        match self {
            Self::Call(call) => visitor.visit_call(call),
            Self::Cast(cast) => visitor.visit_cast(cast),
            Self::Load(load) => visitor.visit_load(load),
            Self::Store(store) => visitor.visit_store(store),
            Self::Select(select) => visitor.visit_select(select),
            Self::IsNull(is_null) => visitor.visit_is_null(is_null),
            Self::BinOp(binop) => visitor.visit_bin_op(binop),
            Self::Copy(copy) => visitor.visit_copy(copy),
            Self::UnaryOp(unary_op) => visitor.visit_unary_op(unary_op),
            Self::NullRow(null_row) => visitor.visit_null_row(null_row),
            Self::SetNull(set_null) => visitor.visit_set_null(set_null),
            Self::Constant(constant) => visitor.visit_constant(constant),
            Self::CopyRowTo(copy_row_to) => visitor.visit_copy_row_to(copy_row_to),
            Self::UninitRow(uninit_row) => visitor.visit_uninit_row(uninit_row),
            Self::Uninit(uninit) => visitor.visit_uninit(uninit),
            Self::Nop(noop) => visitor.visit_noop(noop),
            Self::Drop(drop) => visitor.visit_drop(drop),
        }
    }
}

pub struct MapLayouts<F: ?Sized> {
    map_layout: F,
}

impl<F> MapLayouts<F> {
    pub fn new(map_layout: F) -> Self {
        Self { map_layout }
    }
}

impl<F> ExprVisitor for MapLayouts<F>
where
    F: FnMut(LayoutId) + ?Sized,
{
    fn visit_load(&mut self, load: &Load) {
        (self.map_layout)(load.source_layout());
    }

    fn visit_store(&mut self, store: &Store) {
        (self.map_layout)(store.target_layout());
    }

    fn visit_is_null(&mut self, is_null: &IsNull) {
        (self.map_layout)(is_null.target_layout());
    }

    fn visit_set_null(&mut self, set_null: &SetNull) {
        (self.map_layout)(set_null.target_layout());
    }

    fn visit_copy_row_to(&mut self, copy_row_to: &CopyRowTo) {
        (self.map_layout)(copy_row_to.layout());
    }

    fn visit_null_row(&mut self, null_row: &NullRow) {
        (self.map_layout)(null_row.layout);
    }

    fn visit_uninit_row(&mut self, uninit_row: &UninitRow) {
        (self.map_layout)(uninit_row.layout);
    }

    fn visit_call(&mut self, call: &Call) {
        for arg in call.arg_types() {
            if let RowOrScalar::Row(layout) = *arg {
                (self.map_layout)(layout);
            }
        }
    }

    fn visit_uninit(&mut self, uninit: &Uninit) {
        if let RowOrScalar::Row(layout) = uninit.value() {
            (self.map_layout)(layout);
        }
    }

    fn visit_select(&mut self, select: &Select) {
        if let RowOrScalar::Row(layout) = select.value_type() {
            (self.map_layout)(layout);
        }
    }

    fn visit_drop(&mut self, drop: &Drop) {
        if let RowOrScalar::Row(layout) = drop.value_type() {
            (self.map_layout)(layout);
        }
    }
}

impl<F> MutExprVisitor for MapLayouts<F>
where
    F: FnMut(&mut LayoutId) + ?Sized,
{
    fn visit_load(&mut self, load: &mut Load) {
        (self.map_layout)(load.source_layout_mut());
    }

    fn visit_store(&mut self, store: &mut Store) {
        (self.map_layout)(store.target_layout_mut());
    }

    fn visit_is_null(&mut self, is_null: &mut IsNull) {
        (self.map_layout)(is_null.target_layout_mut());
    }

    fn visit_set_null(&mut self, set_null: &mut SetNull) {
        (self.map_layout)(set_null.target_layout_mut());
    }

    fn visit_copy_row_to(&mut self, copy_row_to: &mut CopyRowTo) {
        (self.map_layout)(copy_row_to.layout_mut());
    }

    fn visit_null_row(&mut self, null_row: &mut NullRow) {
        (self.map_layout)(&mut null_row.layout);
    }

    fn visit_uninit_row(&mut self, uninit_row: &mut UninitRow) {
        (self.map_layout)(&mut uninit_row.layout);
    }

    fn visit_call(&mut self, call: &mut Call) {
        for arg in call.arg_types_mut() {
            if let RowOrScalar::Row(layout) = arg {
                (self.map_layout)(layout);
            }
        }
    }

    fn visit_uninit(&mut self, uninit: &mut Uninit) {
        if let RowOrScalar::Row(layout) = uninit.value_mut() {
            (self.map_layout)(layout);
        }
    }

    fn visit_select(&mut self, select: &mut Select) {
        if let RowOrScalar::Row(layout) = select.value_type_mut() {
            (self.map_layout)(layout);
        }
    }

    fn visit_drop(&mut self, drop: &mut Drop) {
        if let RowOrScalar::Row(layout) = drop.value_type_mut() {
            (self.map_layout)(layout);
        }
    }
}

pub struct MapExprIds<F: ?Sized> {
    map_expr: F,
}

impl<F> MapExprIds<F> {
    pub fn new(map_expr: F) -> Self {
        Self { map_expr }
    }
}

impl<F> MutExprVisitor for MapExprIds<F>
where
    F: FnMut(&mut ExprId) + ?Sized,
{
    fn visit_call(&mut self, call: &mut Call) {
        call.args_mut()
            .iter_mut()
            .for_each(|expr| (self.map_expr)(expr));
    }

    fn visit_cast(&mut self, cast: &mut Cast) {
        (self.map_expr)(cast.value_mut());
    }

    fn visit_load(&mut self, load: &mut Load) {
        (self.map_expr)(load.source_mut());
    }

    fn visit_store(&mut self, store: &mut Store) {
        (self.map_expr)(store.target_mut());

        if let RValue::Expr(value) = store.value_mut() {
            (self.map_expr)(value);
        }
    }

    fn visit_select(&mut self, select: &mut Select) {
        (self.map_expr)(select.cond_mut());
        (self.map_expr)(select.if_true_mut());
        (self.map_expr)(select.if_false_mut());
    }

    fn visit_is_null(&mut self, is_null: &mut IsNull) {
        (self.map_expr)(is_null.target_mut());
    }

    fn visit_set_null(&mut self, set_null: &mut SetNull) {
        (self.map_expr)(set_null.target_mut());

        if let RValue::Expr(is_null) = set_null.is_null_mut() {
            (self.map_expr)(is_null);
        }
    }

    fn visit_bin_op(&mut self, binop: &mut BinaryOp) {
        (self.map_expr)(binop.lhs_mut());
        (self.map_expr)(binop.rhs_mut());
    }

    fn visit_copy(&mut self, copy: &mut Copy) {
        (self.map_expr)(copy.value_mut());
    }

    fn visit_unary_op(&mut self, unary_op: &mut UnaryOp) {
        (self.map_expr)(unary_op.value_mut());
    }

    fn visit_copy_row_to(&mut self, copy_row_to: &mut CopyRowTo) {
        (self.map_expr)(copy_row_to.src_mut());
        (self.map_expr)(copy_row_to.dest_mut());
    }

    fn visit_drop(&mut self, drop: &mut Drop) {
        (self.map_expr)(drop.value_mut());
    }
}
