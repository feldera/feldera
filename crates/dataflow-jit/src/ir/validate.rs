use crate::ir::{
    BinOpKind, BlockId, Expr, ExprId, Function, Graph, InputFlags, LayoutCache, LayoutId, Node,
    NodeId, RValue, RowType,
};
use std::collections::{BTreeMap, BTreeSet};

pub struct Validator {
    /// A set of all nodes that exist
    nodes: BTreeSet<NodeId>,
    /// A map of nodes to their inputs (if they accept inputs)
    // TODO: TinyVec<[LayoutId; 5]>
    node_inputs: BTreeMap<NodeId, Vec<NodeId>>,
    /// A map of nodes to their output layout (if they produce an output)
    node_outputs: BTreeMap<NodeId, LayoutId>,
    function_validator: FunctionValidator,
}

impl Validator {
    pub fn new() -> Self {
        Self {
            nodes: BTreeSet::new(),
            node_inputs: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            function_validator: FunctionValidator::new(),
        }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.node_inputs.clear();
        self.node_outputs.clear();
    }

    // FIXME: Make this return a result instead of panicking
    pub fn validate_graph(&mut self, graph: &Graph) {
        self.clear();

        // Collect all nodes and the layouts of their outputs
        for (&node_id, node) in graph.nodes() {
            // If the graph already contained a node with this id
            if !self.nodes.insert(node_id) {
                panic!("graph contained duplicate node id: {node_id}")
            }

            // FIXME: Factor a lot of this querying into the `DataflowNode` trait
            match node {
                Node::Map(map) => {
                    self.node_inputs.insert(node_id, vec![map.input()]);
                    self.node_outputs.insert(node_id, map.layout());
                }

                Node::Neg(neg) => {
                    self.node_inputs.insert(node_id, vec![neg.input()]);
                    self.node_outputs.insert(node_id, neg.output_layout());
                }
                Node::Sum(sum) => {
                    self.node_inputs.insert(node_id, sum.inputs().to_vec());
                }
                Node::Fold(_) => todo!(),

                Node::Sink(sink) => {
                    self.node_inputs.insert(node_id, vec![sink.input()]);
                }

                Node::Source(source) => {
                    self.node_outputs.insert(node_id, source.layout());
                }

                Node::Filter(_) => todo!(),
                Node::IndexWith(_) => todo!(),
                Node::Differentiate(_) => todo!(),
            }
        }

        let unit_ty = graph.layout_cache().unit();
        for (&node_id, node) in graph.nodes() {
            match node {
                Node::Map(map) => {
                    assert_eq!(map.map_fn().return_type(), unit_ty);

                    let input_layout = self.get_expected_input(node_id, map.input());
                    let expected = &[(input_layout, false), (map.layout(), true)];

                    assert_eq!(expected.len(), map.map_fn().args().len());
                    for (idx, (&(expected_layout, is_mutable), &(layout, _, flags))) in
                        expected.iter().zip(map.map_fn().args()).enumerate()
                    {
                        assert_eq!(
                            expected_layout,
                            layout,
                            "the {idx}th argument to a map function had an incorrect layout: expected {:?}, got {:?}",
                            graph.layout_cache().get(expected_layout),
                            graph.layout_cache().get(layout),
                        );

                        assert_eq!(
                            flags.contains(InputFlags::MUTABLE),
                            is_mutable,
                            "the {idx}th argument to a map function was {}mutable when it should {}have been",
                            if is_mutable { "not" } else { "" },
                            if is_mutable { "" } else { "not" },
                        );
                    }

                    self.function_validator
                        .validate_function(map.map_fn(), graph.layout_cache());
                }

                Node::Neg(neg) => {
                    let input_layout = self.get_expected_input(node_id, neg.input());
                    assert_eq!(input_layout, neg.output_layout());
                }

                Node::Sum(_) => {}

                Node::Fold(_) => {}
                Node::Sink(_) => {}
                Node::Source(_) => {}
                Node::Filter(_) => {}
                Node::IndexWith(_) => {}
                Node::Differentiate(_) => {}
            }
        }
    }

    #[track_caller]
    fn get_expected_input(&self, node: NodeId, input: NodeId) -> LayoutId {
        if let Some(&input_layout) = self.node_outputs.get(&input) {
            input_layout
        } else {
            panic!("node {node}'s input {input} does not exist");
        }
    }
}

pub struct FunctionValidator {
    exprs: BTreeSet<ExprId>,
    /// Expressions that produce values will have a type which will
    /// either be a row type or an entire row
    expr_types: BTreeMap<ExprId, Result<RowType, LayoutId>>,
    /// A map from all expressions containing row types to their mutability
    expr_row_mutability: BTreeMap<ExprId, bool>,
    /// Expressions that don't produce any outputs, like stores
    non_producing_exprs: BTreeSet<ExprId>,
    blocks: BTreeSet<BlockId>,
    // TODO: Block parameters once those are implemented
    // TODO: Control flow validation
}

impl FunctionValidator {
    pub fn new() -> Self {
        Self {
            exprs: BTreeSet::new(),
            expr_types: BTreeMap::new(),
            expr_row_mutability: BTreeMap::new(),
            non_producing_exprs: BTreeSet::new(),
            blocks: BTreeSet::new(),
        }
    }

    pub fn clear(&mut self) {
        self.exprs.clear();
        self.expr_types.clear();
        self.expr_row_mutability.clear();
        self.non_producing_exprs.clear();
        self.blocks.clear();
    }

    pub fn validate_function(&mut self, func: &Function, layout_cache: &LayoutCache) {
        self.clear();

        self.exprs.extend(func.exprs().keys().copied());
        self.blocks.extend(func.blocks().keys().copied());

        for &(layout_id, expr_id, flags) in func.args() {
            if !self.exprs.insert(expr_id) {
                panic!("duplicate expression {expr_id} (declared first as a function parameter)");
            }

            self.expr_types.insert(expr_id, Err(layout_id));
            self.expr_row_mutability
                .insert(expr_id, flags.contains(InputFlags::MUTABLE));
        }

        // Infer expression types
        let mut stack = vec![func.entry_block()];
        while let Some(block_id) = stack.pop() {
            let block = func.blocks().get(&block_id).unwrap_or_else(|| {
                panic!("function attempted to use block that doesn't exist: {block_id}")
            });

            assert_eq!(
                block.id(),
                block_id,
                "block has mismatched id: {block_id} != {}",
                block.id(),
            );

            for &expr_id in block.body() {
                let expr = func.exprs().get(&expr_id).unwrap_or_else(|| {
                    panic!("block {block_id} mentioned expression {expr_id} which doesn't exist")
                });

                match expr {
                    Expr::Load(load) => {
                        if let Some(&source_ty) = self.expr_types.get(&load.source()) {
                            match source_ty {
                                Ok(row_ty) => panic!(
                                    "load {expr_id} attempted to load from a scalar type {row_ty:?} produced from {}",
                                    load.source(),
                                ),

                                Err(src_layout) => {
                                    let layout = layout_cache.get(src_layout);
                                    if let Some(row_ty) = layout.get_row_type(load.row()) {
                                        let prev = self.expr_types.insert(expr_id, Ok(row_ty));
                                        assert_eq!(prev, None, "declared load expr twice: {expr_id}");
                                    } else {
                                        panic!(
                                            "load {expr_id} attempted to load from row {} of {layout:?} when the layout only has {} rows",
                                            load.row(),
                                            layout.len(),
                                        );
                                    }
                                }
                            }
                        } else if self.non_producing_exprs.contains(&load.source()) {
                            panic!(
                                "load {expr_id} attempted to load from a value that doesn't produce any outputs: {}",
                                load.source(),
                            );
                        } else {
                            panic!(
                                "load {expr_id} attempted to load from a value that doesn't exist: {}",
                                load.source(),
                            );
                        }
                    }

                    Expr::Store(store) => {
                        if !self.non_producing_exprs.insert(expr_id) {
                            panic!("store repeated in multiple basic blocks: {expr_id} (second decl in {block_id})")
                        }

                        if let Some(&target_ty) = self.expr_types.get(&store.target()) {
                            match target_ty {
                                Ok(row_ty) => panic!(
                                    "store {expr_id} attempted to store to a scalar type {row_ty:?} produced from {}",
                                    store.target(),
                                ),

                                Err(row_layout) => {
                                    let layout = layout_cache.get(row_layout);
                                    if let Some(row_ty) = layout.get_row_type(store.row()) {
                                        let rval_ty = self.get_rval_type(store.value()).unwrap_or_else(|layout| {
                                            panic!(
                                                "store {expr_id} attempted to store a row value with a layout {:?}",
                                                layout_cache.get(layout),
                                            )
                                        });

                                        assert_eq!(
                                            row_ty,
                                            rval_ty,
                                            "store {expr_id} attempted to store a value of type {rval_ty:?} to a row of type {row_ty:?}",
                                        );

                                    } else {
                                        panic!(
                                            "store {expr_id} attempted to store to row {} of {layout:?} when the layout only has {} rows",
                                            store.row(),
                                            layout.len(),
                                        );
                                    }
                                },
                            }
                        } else if self.non_producing_exprs.contains(&store.target()) {
                            panic!(
                                "store {expr_id} attempted to store to a value that doesn't produce any outputs: {}",
                                store.target(),
                            );
                        } else {
                            panic!(
                                "store {expr_id} attempted to store to a value that doesn't exist: {}",
                                store.target(),
                            );
                        }

                        // Ensure we're only storing to mutable rows
                        if !*self.expr_row_mutability.get(&store.target()).unwrap() {
                            panic!(
                                "store {expr_id} stored to {} which is an immutable row",
                                store.target(),
                            );
                        }
                    }

                    // FIXME: Better errors
                    Expr::BinOp(binop) => {
                        let lhs_ty = self.expr_types.get(&binop.lhs()).unwrap().unwrap();
                        let rhs_ty = self.expr_types.get(&binop.rhs()).unwrap().unwrap();
                        assert_eq!(lhs_ty, rhs_ty);

                        match binop.kind() {
                            BinOpKind::Eq | BinOpKind::Neq => {
                                let prev = self.expr_types.insert(expr_id, Ok(RowType::Bool));
                                assert!(prev.is_none());
                            }

                            BinOpKind::Add
                            | BinOpKind::Sub
                            | BinOpKind::Mul
                            | BinOpKind::And
                            | BinOpKind::Or => {
                                assert_ne!(lhs_ty, RowType::String);

                                let prev = self.expr_types.insert(expr_id, Ok(lhs_ty));
                                assert!(prev.is_none());
                            }
                        }
                    }

                    Expr::IsNull(_) => todo!(),

                    Expr::CopyVal(_) => todo!(),

                    Expr::NullRow(_) => todo!(),

                    Expr::SetNull(_) => todo!(),

                    Expr::Constant(_) => todo!(),

                    Expr::CopyRowTo(_) => todo!(),

                    Expr::UninitRow(_) => todo!(),
                }
            }
        }
    }

    fn get_rval_type(&self, rvalue: &RValue) -> Result<RowType, LayoutId> {
        match rvalue {
            RValue::Expr(expr_id) => self.expr_types[expr_id],
            RValue::Imm(constant) => Ok(constant.row_type()),
        }
    }
}
