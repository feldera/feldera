//! Recover the type of UNNEST from the collection closure and its projected columns.
use feldera_sql_ir::{CircuitEditor, Error, Field, NodeId, TypeKind};

fn select(fields: &[NodeId], indexes: Option<&Field>) -> Result<Vec<NodeId>, Error> {
    let Some(Field::Array(indexes)) = indexes else {
        return Err(Error::new(
            "flatmap projection requires a list of field indexes",
        ));
    };
    indexes
        .iter()
        .map(|index| {
            let Field::Data(index) = index else {
                return Err(Error::new("invalid flatmap field index"));
            };
            index
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .and_then(|n| fields.get(n))
                .copied()
                .ok_or_else(|| Error::new("flatmap field index outside its input row"))
        })
        .collect()
}

pub fn rebuild_flatmap(
    edit: &mut CircuitEditor,
    original: NodeId,
    id: NodeId,
) -> Result<NodeId, Error> {
    let flatmap = edit.node(id)?;
    let closure = edit.node(flatmap.edge("collectionExpression")?)?;
    let [parameter] = closure
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("flatmap collection closure requires one parameter"))?;
    let reference = edit.node(parameter)?.edge("type")?;
    let row = edit.node(reference)?.edge("type")?;
    if id == original && row == flatmap.edge("inputRowType")? {
        return Ok(id);
    }
    rebuild_from_row(edit, id, row)
}

fn rebuild_from_row(edit: &mut CircuitEditor, id: NodeId, row: NodeId) -> Result<NodeId, Error> {
    let flatmap = edit.node(id)?;
    let mut columns = select(
        &edit.node(row)?.edges("tupFields")?,
        flatmap.field("passthroughIndexes"),
    )?;
    let function_type = flatmap.edge("type")?;
    let shuffle = flatmap.field("shuffle").cloned();
    columns.extend(flatmap_generated_types(edit, id)?);
    let Some(Field::Object(shuffle)) = shuffle else {
        return Err(Error::new("flatmap requires an output shuffle"));
    };
    match shuffle.get("class") {
        Some(Field::Data(name)) if name == "ExplicitShuffle" => {
            columns = select(&columns, shuffle.get("indexes"))?
        }
        Some(Field::Data(name)) if name == "IdShuffle" => (),
        _ => return Err(Error::new("unknown flatmap output shuffle")),
    }
    let result = edit.tuple_type(id, false, columns)?;
    let parameter = edit.reference_type(edit.type_id(row)?)?;
    let ty = edit.derive_inner(
        function_type,
        [
            ("resultType", Field::Reference(result.into())),
            (
                "parameterTypes",
                Field::Array(vec![Field::Reference(parameter.into())]),
            ),
        ],
    )?;
    edit.derive_inner(
        id,
        [
            ("type", Field::Reference(ty)),
            ("inputRowType", Field::Reference(row)),
        ],
    )
}

/// Rebuild UNNEST with a narrowed input, retaining the ordinality, postprocessing and output shuffle.
pub fn replace_flatmap_input(
    edit: &mut CircuitEditor,
    flatmap: NodeId,
    row: NodeId,
    collection: NodeId,
    passthrough: Vec<usize>,
) -> Result<NodeId, Error> {
    let columns = passthrough.into_iter().map(|i| Field::Data(i.into())).collect();
    let narrowed = edit.derive_inner(flatmap, [
        ("collectionExpression", Field::Reference(collection)),
        ("passthroughIndexes", Field::Array(columns)),
    ])?;
    rebuild_from_row(edit, narrowed, row)
}

/// Types emitted by UNNEST after the passthrough fields and before the shuffle.
pub fn flatmap_generated_types(edit: &mut CircuitEditor, id: NodeId) -> Result<Vec<NodeId>, Error> {
    let flatmap = edit.node(id)?;
    let mut columns = Vec::new();
    if flatmap.field("postProcess").is_some() {
        for post in flatmap.edges("postProcess")? {
            let ty = edit.node(post)?.edge("type")?;
            columns.push(edit.node(ty)?.edge("resultType")?);
        }
    } else {
        let ordinal = flatmap
            .field("ordinalityIndexType")
            .and_then(Field::node_id);
        let closure = edit.node(flatmap.edge("collectionExpression")?)?;
        let collection = edit.node(closure.edge("type")?)?.edge("resultType")?;
        let args = edit.node(collection)?.edges("typeArgs")?;
        match (edit.type_kind(edit.type_id(collection)?)?, args.as_slice()) {
            (TypeKind::Map, [key, value]) => columns.extend([*key, *value]),
            (TypeKind::Array, [element]) => {
                let ty = edit.type_id(*element)?;
                if matches!(edit.type_kind(ty)?, TypeKind::Tuple | TypeKind::RawTuple) {
                    let width = edit.node(*element)?.edges("tupFields")?.len();
                    for i in 0..width {
                        columns.push(edit.tuple_field_type(ty, i)?.into());
                    }
                } else {
                    columns.push(*element);
                }
            }
            _ => return Err(Error::new("flatmap requires an array or map")),
        }
        columns.extend(ordinal);
    }
    Ok(columns)
}

/// Column positions before and after an UNNEST output shuffle.
pub struct FlatmapColumns {
    pub width: usize,
    pub output: Vec<usize>,
    pub passthrough: Vec<usize>,
}

impl FlatmapColumns {
    pub fn read(node: &feldera_sql_ir::Node) -> Result<Self, Error> {
        let Some(Field::Object(shuffle)) = node.field("shuffle") else {
            return Err(Error::new("flatmap requires an output shuffle"));
        };
        let width = shuffle
            .get("inputLength")
            .and_then(index)
            .ok_or_else(|| Error::new("invalid flatmap shuffle width"))?;
        let output = match shuffle.get("class") {
            Some(Field::Data(name)) if name == "IdShuffle" => (0..width).collect(),
            Some(Field::Data(name)) if name == "ExplicitShuffle" => {
                indexes(shuffle.get("indexes"))?
            }
            _ => return Err(Error::new("unknown flatmap shuffle")),
        };
        if output.iter().any(|&i| i >= width) {
            return Err(Error::new("flatmap shuffle index exceeds input width"));
        }
        Ok(Self {
            width,
            output,
            passthrough: indexes(node.field("passthroughIndexes"))?,
        })
    }
}

/// Compose an output projection with UNNEST's existing shuffle. Discard
/// passthrough and postprocessing columns that the composition no longer emits.
pub fn project_flatmap(
    edit: &mut CircuitEditor,
    id: NodeId,
    fields: &[usize],
) -> Result<NodeId, Error> {
    let node = edit.node(id)?;
    let input = node.edge("inputRowType")?;
    let FlatmapColumns {
        width,
        output,
        passthrough,
    } = FlatmapColumns::read(node)?;
    let selected = fields
        .iter()
        .map(|&i| {
            output
                .get(i)
                .copied()
                .ok_or_else(|| Error::new("projection exceeds flatmap output width"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let emitted: std::collections::HashSet<_> = selected.iter().copied().collect();
    let mut discarded = vec![false; width];
    let mut kept = Vec::new();
    for (slot, &column) in passthrough.iter().enumerate() {
        if emitted.contains(&column) {
            kept.push(Field::Data(column.into()));
        } else {
            *discarded
                .get_mut(slot)
                .ok_or_else(|| Error::new("passthrough exceeds shuffle width"))? = true;
        }
    }
    let mut changes = vec![("passthroughIndexes", Field::Array(kept))];
    if node.field("postProcess").is_some() {
        let mut kept = Vec::new();
        for (offset, post) in node.edges("postProcess")?.into_iter().enumerate() {
            let slot = passthrough.len() + offset;
            if emitted.contains(&slot) {
                kept.push(Field::Reference(post));
            } else {
                *discarded
                    .get_mut(slot)
                    .ok_or_else(|| Error::new("postprocessing exceeds shuffle width"))? = true;
            }
        }
        changes.push(("postProcess", Field::Array(kept)));
    }
    let mut remaining = 0;
    let compressed: Vec<_> = discarded
        .iter()
        .map(|&removed| {
            let slot = remaining;
            if !removed {
                remaining += 1usize;
                Some(slot)
            } else {
                None
            }
        })
        .collect();
    let selected = selected
        .into_iter()
        .map(|i| {
            compressed
                .get(i)
                .copied()
                .flatten()
                .map(|slot| Field::Data(slot.into()))
                .ok_or_else(|| Error::new("flatmap projection requested a removed field"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    changes.push((
        "shuffle",
        Field::Object(
            [
                ("class".into(), Field::Data("ExplicitShuffle".into())),
                ("inputLength".into(), Field::Data(remaining.into())),
                ("indexes".into(), Field::Array(selected)),
            ]
            .into(),
        ),
    ));
    let copy = edit.derive_inner(id, changes)?;
    let projected = rebuild_from_row(edit, copy, input)?;
    // withShuffle composes and compresses into a new ExplicitShuffle even when
    // the indexes stay the same. Later deep copies must share that new owner.
    if edit.node(id)?.field("shuffleIdentity").is_none() {
        return Ok(projected);
    }
    let NodeId::Inner(owner) = copy else {
        unreachable!("derived flatmap is an inner node")
    };
    edit.derive_inner(projected, [("shuffleIdentity", Field::Data(owner.into()))])
}

fn index(field: &Field) -> Option<usize> {
    let Field::Data(value) = field else {
        return None;
    };
    usize::try_from(value.as_u64()?).ok()
}

fn indexes(field: Option<&Field>) -> Result<Vec<usize>, Error> {
    let Some(Field::Array(fields)) = field else {
        return Err(Error::new("expected flatmap field indexes"));
    };
    fields
        .iter()
        .map(|field| index(field).ok_or_else(|| Error::new("invalid flatmap field index")))
        .collect()
}
