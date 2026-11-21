use crate::{
    ExpressionLimits, LiteralLimits, TypeId, ValidationError, ValidationLimits,
    expression_objects::{
        EnvironmentId, ExpressionObjects, ObjectKind, ObjectPosition, SourceContext,
    },
    expressions::ExprId,
    graph::check_graph,
    input_producers::{InputProducers, RelationForm},
    input_uses::{InputOwner, InputUses},
    operators::{ComponentPositions, FunctionBody, FunctionSource, OperatorObject, RowFieldNames},
    relation_properties::{Properties, RelationProperties, WindowBound},
    source::{Origin, Position},
    types::TypeKind,
};
use std::collections::{HashMap, HashSet, VecDeque};

type Result<T> = std::result::Result<T, ValidationError>;
pub(crate) fn require(condition: bool, message: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(ValidationError {
            path: "expression_objects".into(),
            message,
        })
    }
}

/// Dynamic work has its own bound: bounded object and environment arenas alone
/// still permit a Cartesian-product traversal. Repeated edges also consume work.
#[derive(Clone, Copy, Debug)]
pub struct ObjectLimits {
    pub graph: ExpressionLimits,
    pub states: usize,
    pub work: usize,
}
impl Default for ObjectLimits {
    fn default() -> Self {
        Self {
            graph: ExpressionLimits::default(),
            states: 1_000_000,
            work: 4_000_000,
        }
    }
}

struct Budget {
    remaining: usize,
}
impl Budget {
    fn spend(&mut self, count: usize) -> Result<()> {
        self.remaining = self.remaining.checked_sub(count).ok_or(ValidationError {
            path: "expression_objects".into(),
            message: "expression work exceeds limit",
        })?;
        Ok(())
    }
}

impl ExpressionObjects {
    /// Validate intrinsic objects, all construction dependencies, then each
    /// evaluation use. Correlation/source environments are explicit inputs;
    /// proving their statement/catalog ownership requires the full S1 adapter.
    pub fn validate(
        &self,
        inputs: &InputUses,
        producers: &InputProducers,
        properties: &RelationProperties,
        type_limits: ValidationLimits,
        literal_limits: LiteralLimits,
        limits: ObjectLimits,
    ) -> Result<()> {
        properties.validate_with_operators(
            producers,
            inputs,
            type_limits,
            limits.states,
            Some(&self.operator_objects),
        )?;
        let types = inputs.type_table.validated(type_limits)?;
        let mut entries = Budget {
            remaining: limits.graph.entries,
        };
        for count in [
            self.expressions.len(),
            self.operators.len(),
            self.operator_objects.len(),
            self.environments.len(),
            self.relation_uses.len(),
            self.value_uses.len(),
        ] {
            entries.spend(count)?;
        }
        let mut references = Budget {
            remaining: limits.graph.references,
        };
        let mut strings = Budget {
            remaining: limits.graph.identifier_bytes,
        };
        self.check_environments(inputs, &mut references)?;
        let function_roots = self.check_operators(inputs, &mut references, &mut strings)?;
        let mut edges = Vec::with_capacity(self.expressions.len() + self.operator_objects.len());
        let mut input_objects = vec![false; inputs.objects.len()];
        for expression in &self.expressions {
            inputs
                .type_table
                .reference(expression.ty, "expression object type")?;
            let children: Vec<usize> = expression.kind.children().map(|id| id.0 as usize).collect();
            references.spend(children.len())?;
            require(
                children.iter().all(|id| *id < self.expressions.len()),
                "expression child outside arena",
            )?;
            require(
                expression.kind.call().is_some()
                    || matches!(expression.position, ObjectPosition::Absent {}),
                "non-call object has parser coordinates",
            )?;
            match &expression.kind {
                ObjectKind::InputRef { object } => {
                    let reference =
                        inputs
                            .objects
                            .get(object.0 as usize)
                            .ok_or(ValidationError {
                                path: "expression_objects".into(),
                                message: "input object outside arena",
                            })?;
                    require(
                        !input_objects[object.0 as usize] && reference.ty == expression.ty,
                        "input identity duplicated or type changed",
                    )?;
                    input_objects[object.0 as usize] = true;
                }
                ObjectKind::SearchArgument { ranges, .. } => {
                    references.spend(ranges.len().saturating_mul(2))?;
                    crate::search::SearchRange::validate_all(
                        ranges,
                        expression.ty,
                        &types,
                        literal_limits,
                    )?;
                }
                ObjectKind::Literal { literal } => {
                    require(
                        literal.ty == expression.ty && matches!(literal.origin, Origin::Absent {}),
                        "literal header differs from object",
                    )?;
                    types.validate_literal(literal, literal_limits)?;
                }
                ObjectKind::LambdaRef { name, .. } => {
                    strings.spend(name.len())?;
                }
                ObjectKind::Lambda { parameters, body } => {
                    require(
                        self.expressions[body.0 as usize].ty == expression.ty,
                        "lambda result differs from body",
                    )?;
                    let mut declarations = HashSet::new();
                    for parameter in parameters {
                        match &self.expressions[parameter.0 as usize].kind {
                            ObjectKind::LambdaRef { name, index } => {
                                strings.spend(name.len())?;
                                require(
                                    declarations.insert((name, index)),
                                    "duplicate lambda parameter",
                                )?;
                            }
                            _ => {
                                return require(
                                    false,
                                    "lambda parameter is not a declaration reference",
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
            let mut construction = children;
            if let Some((signature, arguments)) = expression.kind.call() {
                let signature =
                    self.operators
                        .get(signature.0 as usize)
                        .ok_or(ValidationError {
                            path: "expression_objects".into(),
                            message: "call signature outside arena",
                        })?;
                require(
                    signature.result == expression.ty
                        && signature.parameters.len() == arguments.len(),
                    "call signature differs",
                )?;
                require(
                    signature
                        .parameters
                        .iter()
                        .zip(arguments)
                        .all(|(ty, argument)| *ty == self.expressions[argument.0 as usize].ty),
                    "call argument type differs",
                )?;
                let search = matches!(&self.operator_objects[signature.object.0 as usize],
                    OperatorObject::Builtin { registry, registry_version, key }
                    if registry == "calcite-s1-fixture" && registry_version == "1" && key == "SqlStdOperatorTable.SEARCH");
                if search {
                    require(
                        arguments.len() == 2
                            && matches!(
                                self.expressions[arguments[1].0 as usize].kind,
                                ObjectKind::SearchArgument { .. }
                            ),
                        "SEARCH requires a typed range argument",
                    )?;
                }
                for (index, argument) in arguments.iter().enumerate() {
                    require(
                        !matches!(
                            self.expressions[argument.0 as usize].kind,
                            ObjectKind::SearchArgument { .. }
                        ) || (search && index == 1),
                        "search argument used outside SEARCH",
                    )?;
                }
                construction.push(self.expressions.len() + signature.object.0 as usize);
                references.spend(1)?;
            }
            if expression.kind.call().is_none() {
                require(
                    construction.iter().all(|id| {
                        !matches!(
                            self.expressions[*id].kind,
                            ObjectKind::SearchArgument { .. }
                        )
                    }),
                    "search argument used outside SEARCH",
                )?;
            }
            edges.push(construction);
        }
        for root in self
            .relation_uses
            .iter()
            .flat_map(|usage| &usage.roots)
            .chain(self.value_uses.iter().flat_map(|usage| &usage.roots))
            .chain(function_roots.iter().map(|(root, _)| root))
        {
            require(
                !self
                    .expressions
                    .get(root.0 as usize)
                    .is_some_and(|expression| {
                        matches!(expression.kind, ObjectKind::SearchArgument { .. })
                    }),
                "search argument cannot be an evaluation root",
            )?;
        }
        require(
            !input_objects.contains(&false),
            "input arena object missing from expression graph",
        )?;
        for object in &self.operator_objects {
            edges.push(match object {
                OperatorObject::Function {
                    body: FunctionBody::ObjectSql { root, .. },
                    ..
                } => vec![root.0 as usize],
                _ => vec![],
            });
        }
        check_graph(&edges, limits.graph.depth, "expression construction")?;
        let roots = self.check_relation_uses(inputs, producers, properties, &mut references)?;
        let values = self.check_value_uses(inputs, &mut references)?;
        let mut walk = Walk::new(self, inputs, limits);
        for (root, environment) in roots.into_iter().chain(function_roots).chain(values) {
            walk.enqueue(root.0 as usize, environment.0 as usize)?;
        }
        walk.run()
    }

    fn check_environments(&self, inputs: &InputUses, budget: &mut Budget) -> Result<()> {
        for environment in &self.environments {
            budget.spend(1 + environment.correlations.len())?;
            require(
                (environment.input.0 as usize) < inputs.environments.len(),
                "evaluation input outside arena",
            )?;
            if let SourceContext::Present { source } = environment.source {
                require(
                    (source.0 as usize) < inputs.type_table.sources.len(),
                    "evaluation source outside arena",
                )?;
            }
            let mut identities = HashSet::new();
            for correlation in &environment.correlations {
                inputs
                    .type_table
                    .reference(correlation.row, "correlation row")?;
                require(
                    matches!(
                        inputs.type_table.types[correlation.row.0 as usize].kind,
                        TypeKind::Row { .. }
                    ),
                    "correlation requires row type",
                )?;
                require(
                    identities.insert(correlation.identity),
                    "duplicate correlation declaration",
                )?;
            }
        }
        Ok(())
    }

    fn check_operators(
        &self,
        inputs: &InputUses,
        references: &mut Budget,
        strings: &mut Budget,
    ) -> Result<Vec<(ExprId, EnvironmentId)>> {
        let mut roots = Vec::new();
        let mut builtins = HashSet::new();
        let mut functions = HashSet::new();
        for object in &self.operator_objects {
            match object {
                OperatorObject::Builtin {
                    registry,
                    registry_version,
                    key,
                } => {
                    for name in [registry, registry_version, key] {
                        strings.spend(name.len())?;
                    }
                    require(
                        builtins.insert((registry, registry_version, key)),
                        "duplicate builtin identity",
                    )?;
                }
                OperatorObject::SumZero {} => {}
                OperatorObject::TypeConstructor { name, .. } => {
                    references.spend(name.names.len() + 2)?;
                    for name in &name.names {
                        strings.spend(name.len())?;
                    }
                    if let ComponentPositions::Present { positions } = &name.components {
                        references.spend(positions.len())?;
                    }
                }
                OperatorObject::RowConstructor { name, fields } => {
                    strings.spend(name.len())?;
                    if let RowFieldNames::Present { names } = fields {
                        references.spend(names.len())?;
                        for name in names.iter().flatten() {
                            strings.spend(name.len())?;
                        }
                    }
                }
                OperatorObject::AggregateFunction {
                    name, parameters, ..
                } => {
                    references.spend(name.names.len() + parameters.len() + 2)?;
                    for text in &name.names {
                        strings.spend(text.len())?;
                    }
                    for parameter in parameters {
                        strings.spend(parameter.name.len())?;
                    }
                    if let ComponentPositions::Present { positions } = &name.components {
                        references.spend(positions.len())?;
                    }
                }
                OperatorObject::Function {
                    name,
                    parameters,
                    diagnostic,
                    body,
                    ..
                } => {
                    for name in [name, &diagnostic.text, &diagnostic.internal_text] {
                        strings.spend(name.len())?;
                    }
                    references.spend(parameters.len() + 6)?;
                    for parameter in parameters {
                        strings.spend(parameter.name.len())?;
                    }
                    match body {
                        FunctionBody::External {} => {}
                        FunctionBody::Sql { .. } => {
                            return require(false, "fragment function scope in object graph")
                                .map(|_| roots);
                        }
                        FunctionBody::ObjectSql {
                            environment,
                            root,
                            source,
                        } => {
                            require(
                                (root.0 as usize) < self.expressions.len(),
                                "function body outside expression arena",
                            )?;
                            let context = self.environment(*environment)?;
                            require(
                                context.correlations.is_empty(),
                                "function body captures caller correlations",
                            )?;
                            let input = &inputs.environments[context.input.0 as usize];
                            let InputOwner::Function { function } = input.owner else {
                                return require(
                                    false,
                                    "function body requires parameter environment",
                                )
                                .map(|_| roots);
                            };
                            require(
                                functions.insert(function),
                                "function parameter environment has multiple owners",
                            )?;
                            let TypeKind::Row { fields, .. } =
                                &inputs.type_table.types[input.row.0 as usize].kind
                            else {
                                unreachable!()
                            };
                            require(
                                fields.len() == parameters.len()
                                    && fields.iter().zip(parameters).all(|(field, parameter)| {
                                        field.name.name == parameter.name
                                            && field.ty == parameter.ty
                                    }),
                                "function parameter row differs",
                            )?;
                            if let FunctionSource::Captured { source } = source {
                                require(
                                    context.source == SourceContext::Present { source: *source },
                                    "function body source differs",
                                )?;
                            }
                            roots.push((*root, *environment));
                        }
                    }
                }
            }
            object.validate(&inputs.type_table, "expression operator")?;
        }
        for operator in &self.operators {
            references.spend(operator.parameters.len() + 2)?;
            require(
                (operator.object.0 as usize) < self.operator_objects.len(),
                "operator object outside arena",
            )?;
            for ty in operator
                .parameters
                .iter()
                .chain(std::iter::once(&operator.result))
            {
                inputs.type_table.reference(*ty, "call signature type")?;
            }
        }
        Ok(roots)
    }

    fn environment(&self, id: EnvironmentId) -> Result<&crate::expression_objects::Environment> {
        self.environments.get(id.0 as usize).ok_or(ValidationError {
            path: "expression_objects".into(),
            message: "evaluation environment outside arena",
        })
    }

    fn check_value_uses(
        &self,
        inputs: &InputUses,
        budget: &mut Budget,
    ) -> Result<Vec<(ExprId, EnvironmentId)>> {
        let mut roots = Vec::new();
        for usage in &self.value_uses {
            budget.spend(usage.roots.len() + 1)?;
            let environment = self.environment(usage.environment)?;
            require(
                matches!(
                    inputs.environments[environment.input.0 as usize].owner,
                    InputOwner::Constant {}
                ) && environment.correlations.is_empty(),
                "statement value requires an empty constant environment",
            )?;
            for root in &usage.roots {
                require(
                    (root.0 as usize) < self.expressions.len(),
                    "value root outside expression arena",
                )?;
                require(
                    !matches!(
                        self.expressions[root.0 as usize].kind,
                        ObjectKind::WindowAggregate { .. }
                    ),
                    "statement value cannot be a window aggregate",
                )?;
                roots.push((*root, usage.environment));
            }
        }
        Ok(roots)
    }

    fn check_relation_uses(
        &self,
        inputs: &InputUses,
        producers: &InputProducers,
        properties: &RelationProperties,
        budget: &mut Budget,
    ) -> Result<Vec<(ExprId, EnvironmentId)>> {
        let mut roots = Vec::new();
        let mut definitions = vec![None; producers.relations.len()];
        let mut uses = HashSet::new();
        for usage in &self.relation_uses {
            budget.spend(usage.roots.len() + 2)?;
            let relation = usage.relation as usize;
            require(
                relation < producers.relations.len(),
                "expression owner outside relation arena",
            )?;
            require(
                uses.insert((relation, usage.environment.0)),
                "duplicate relation/environment use",
            )?;
            let producer = &producers.relations[relation];
            let environment = self.environment(usage.environment)?;
            match &inputs.environments[environment.input.0 as usize].owner {
                InputOwner::Relations { inputs } => require(
                    producer.form != RelationForm::LogicalWindow && inputs == &producer.inputs,
                    "relation evaluation inputs differ",
                )?,
                InputOwner::Window {
                    relation: owner,
                    inputs,
                } => require(
                    producer.form == RelationForm::LogicalWindow
                        && *owner == usage.relation
                        && inputs == &producer.inputs,
                    "window evaluation inputs differ",
                )?,
                InputOwner::Function { .. } | InputOwner::Constant {} => {
                    return require(false, "relation uses non-relational environment")
                        .map(|_| roots);
                }
            }
            if let Some(previous) = definitions[relation] {
                require(
                    previous == usage.roots.as_slice(),
                    "shared relation expression identities differ",
                )?;
            }
            definitions[relation] = Some(usage.roots.as_slice());
            let TypeKind::Row { fields, .. } =
                &inputs.type_table.types[producer.row.0 as usize].kind
            else {
                unreachable!()
            };
            require(
                usage.roots.len()
                    == root_count(
                        &properties.relations[relation].fields,
                        fields.len(),
                        producer,
                    ),
                "relation expression slot count differs",
            )?;
            for root in &usage.roots {
                require(
                    (root.0 as usize) < self.expressions.len(),
                    "relation root outside expression arena",
                )?;
                roots.push((*root, usage.environment));
            }
            crate::object_relation_validation::check_slots(
                self,
                inputs,
                producer,
                &properties.relations[relation].fields,
                &usage.roots,
            )?;
        }
        require(
            definitions.iter().all(Option::is_some),
            "relation missing expression field record",
        )?;
        Ok(roots)
    }
}

fn root_count(
    properties: &Properties,
    width: usize,
    producer: &crate::input_producers::InputProducer,
) -> usize {
    match properties {
        Properties::Project { expressions, .. } => *expressions as usize,
        Properties::Filter { .. } | Properties::TableFunction { .. } => 1,
        Properties::Values { tuples } => *tuples as usize * width,
        Properties::Join { .. } => {
            if producer.form == RelationForm::LogicalAsofJoin {
                2
            } else {
                1
            }
        }
        Properties::Aggregate { calls, .. } => calls
            .iter()
            .map(|call| call.direct_arguments as usize)
            .sum(),
        Properties::Window { groups } => {
            producer.constants.len()
                + groups
                    .iter()
                    .map(|group| {
                        let offset = |bound| {
                            usize::from(matches!(
                                bound,
                                WindowBound::Preceding | WindowBound::Following
                            ))
                        };
                        group.calls as usize + offset(group.lower) + offset(group.upper)
                    })
                    .sum::<usize>()
        }
        Properties::Sort { offset, fetch, .. } => usize::from(*offset) + usize::from(*fetch),
        _ => 0,
    }
}

struct Frame {
    environment: usize,
    parent: Option<usize>,
    lambda: Option<usize>,
    depth: usize,
}
struct Walk<'a> {
    graph: &'a ExpressionObjects,
    inputs: &'a InputUses,
    limits: ObjectLimits,
    budget: Budget,
    frames: Vec<Frame>,
    lambda_frames: HashMap<(usize, usize), usize>,
    pending: VecDeque<(usize, usize)>,
    seen: HashSet<(usize, usize)>,
    objects_seen: Vec<bool>,
    input_pairs: HashSet<(u32, u32)>,
    comparison_costs: HashMap<TypeId, usize>,
}
impl<'a> Walk<'a> {
    fn new(graph: &'a ExpressionObjects, inputs: &'a InputUses, limits: ObjectLimits) -> Self {
        Self {
            graph,
            inputs,
            limits,
            budget: Budget {
                remaining: limits.work,
            },
            frames: (0..graph.environments.len())
                .map(|environment| Frame {
                    environment,
                    parent: None,
                    lambda: None,
                    depth: 0,
                })
                .collect(),
            lambda_frames: HashMap::new(),
            pending: VecDeque::new(),
            seen: HashSet::new(),
            objects_seen: vec![false; graph.expressions.len()],
            input_pairs: HashSet::new(),
            comparison_costs: HashMap::new(),
        }
    }
    fn enqueue(&mut self, expression: usize, frame: usize) -> Result<()> {
        self.budget.spend(1)?;
        if self.seen.insert((expression, frame)) {
            require(
                self.seen.len() <= self.limits.states,
                "expression/context state limit exceeded",
            )?;
            self.pending.push_back((expression, frame));
        }
        Ok(())
    }
    fn field(&mut self, row: TypeId, index: u32, result: TypeId) -> Result<()> {
        let TypeKind::Row { fields, .. } = &self.inputs.type_table.types[row.0 as usize].kind
        else {
            return require(false, "field requires row type");
        };
        let field = fields.get(index as usize).ok_or(ValidationError {
            path: "expression_objects".into(),
            message: "field outside evaluation row",
        })?;
        if field.ty != result {
            // Charge a conservative byte count before a potentially wide row
            // comparison. Repeated uses cannot multiply unbounded shape work.
            let cost = *self.comparison_costs.entry(field.ty).or_insert_with(|| {
                serde_json::to_vec(&self.inputs.type_table.types[field.ty.0 as usize].kind)
                    .expect("type kinds serialize")
                    .len()
            });
            self.budget.spend(cost)?;
        }
        require(
            self.inputs
                .type_table
                .reference_type_matches(field.ty, result),
            "field type differs in evaluation context",
        )
    }
    fn run(&mut self) -> Result<()> {
        while let Some((id, frame)) = self.pending.pop_front() {
            self.objects_seen[id] = true;
            let expression = &self.graph.expressions[id];
            let environment_id = self.frames[frame].environment;
            let environment = &self.graph.environments[environment_id];
            if let ObjectPosition::Present {
                position,
                internal: Some(_),
            } = &expression.position
            {
                require(
                    !position.quoted,
                    "extended expression position cannot be quoted",
                )?;
            }
            if let ObjectPosition::Present { position, .. } = &expression.position
                && (position.line != 0
                    || position.column != 0
                    || position.end_line != 0
                    || position.end_column != 0)
            {
                let SourceContext::Present { source } = environment.source else {
                    return require(false, "positioned expression has no source context");
                };
                let start = Position {
                    line: position.line,
                    column: position.column,
                };
                let end = Position {
                    line: position.end_line,
                    column: position.end_column,
                };
                let origin = if [start.line, start.column, end.line, end.column].contains(&0) {
                    Origin::Generated { source, start, end }
                } else {
                    Origin::Source { source, start, end }
                };
                self.inputs
                    .type_table
                    .origin(&origin, "expression use source")?;
            }
            require(
                !matches!(expression.kind, ObjectKind::WindowAggregate { .. })
                    || !matches!(
                        self.inputs.environments[environment.input.0 as usize].owner,
                        InputOwner::Constant {}
                    ),
                "statement value cannot contain a window aggregate",
            )?;
            let mut child_frame = frame;
            match &expression.kind {
                ObjectKind::InputRef { object } => {
                    let input = &self.inputs.objects[object.0 as usize];
                    self.field(
                        self.inputs.environments[environment.input.0 as usize].row,
                        input.field,
                        expression.ty,
                    )?;
                    self.input_pairs.insert((object.0, environment.input.0));
                }
                ObjectKind::Correlation { identity } => {
                    self.budget.spend(environment.correlations.len())?;
                    require(
                        environment.correlations.iter().any(|binding| {
                            binding.identity == *identity && binding.row == expression.ty
                        }),
                        "correlation is unbound or has wrong type",
                    )?;
                }
                ObjectKind::FieldAccess { input, field } => self.field(
                    self.graph.expressions[input.0 as usize].ty,
                    *field,
                    expression.ty,
                )?,
                ObjectKind::Lambda { .. } => {
                    if let Some(existing) = self.lambda_frames.get(&(frame, id)) {
                        child_frame = *existing;
                    } else {
                        require(
                            self.frames.len() < self.limits.states
                                && self.frames[frame].depth < self.limits.graph.depth,
                            "lambda context limit exceeded",
                        )?;
                        child_frame = self.frames.len();
                        self.frames.push(Frame {
                            environment: environment_id,
                            parent: Some(frame),
                            lambda: Some(id),
                            depth: self.frames[frame].depth + 1,
                        });
                        self.lambda_frames.insert((frame, id), child_frame);
                    }
                }
                ObjectKind::LambdaRef { index, name } => {
                    let mut cursor = Some(frame);
                    let mut found = false;
                    while let Some(scope) = cursor {
                        self.budget.spend(1)?;
                        if let Some(lambda) = self.frames[scope].lambda {
                            let ObjectKind::Lambda { parameters, .. } =
                                &self.graph.expressions[lambda].kind
                            else {
                                unreachable!()
                            };
                            self.budget.spend(parameters.len())?;
                            for parameter in parameters {
                                let declaration = &self.graph.expressions[parameter.0 as usize];
                                let ObjectKind::LambdaRef {
                                    index: candidate,
                                    name: candidate_name,
                                } = &declaration.kind
                                else {
                                    unreachable!()
                                };
                                if candidate == index {
                                    self.budget.spend(name.len().max(candidate_name.len()))?;
                                    if candidate_name == name {
                                        require(
                                            declaration.ty == expression.ty,
                                            "lambda reference type differs",
                                        )?;
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if found {
                                break;
                            }
                        }
                        cursor = self.frames[scope].parent;
                    }
                    require(found, "lambda reference has no lexical declaration")?;
                }
                _ => {}
            }
            for child in expression.kind.children() {
                self.enqueue(child.0 as usize, child_frame)?;
            }
        }
        require(
            !self.objects_seen.contains(&false),
            "expression object has no evaluation use",
        )?;
        require(
            self.input_pairs.len() == self.inputs.uses.len()
                && self.inputs.uses.iter().all(|usage| {
                    self.input_pairs
                        .contains(&(usage.object.0, usage.environment.0))
                }),
            "expression uses differ from input-use contract",
        )
    }
}
