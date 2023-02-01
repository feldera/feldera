use cranelift::{
    codegen::{
        entity::SecondaryMap,
        ir::{entities::AnyEntity, Function, Inst},
        write::{FuncWriter, PlainWriter},
    },
    prelude::{Block, Value},
};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
};
use xxhash_rust::xxh3::Xxh3Builder;

#[derive(Clone, Debug)]
pub(crate) struct CommentWriter {
    global_comments: Vec<String>,
    entity_comments: HashMap<AnyEntity, String, Xxh3Builder>,
}

impl CommentWriter {
    pub(crate) fn new(symbol: &str, abi: &str) -> Self {
        let global_comments = vec![
            format!("symbol {symbol}"),
            format!("abi {abi}"),
            String::new(),
        ];

        Self {
            global_comments,
            entity_comments: HashMap::with_hasher(Xxh3Builder::new()),
        }
    }

    pub(crate) fn add_global_comment<S: Into<String>>(&mut self, comment: S) {
        self.global_comments.push(comment.into());
    }

    pub(crate) fn add_comment<S: Into<String> + AsRef<str>, E: Into<AnyEntity>>(
        &mut self,
        entity: E,
        comment: S,
    ) {
        match self.entity_comments.entry(entity.into()) {
            Entry::Occupied(mut occ) => {
                occ.get_mut().push('\n');
                occ.get_mut().push_str(comment.as_ref());
            }

            Entry::Vacant(vac) => {
                vac.insert(comment.into());
            }
        }
    }
}

impl FuncWriter for &'_ CommentWriter {
    fn write_preamble(
        &mut self,
        w: &mut dyn fmt::Write,
        func: &Function,
    ) -> Result<bool, fmt::Error> {
        for comment in &self.global_comments {
            if !comment.is_empty() {
                writeln!(w, "; {}", comment)?;
            } else {
                writeln!(w)?;
            }
        }
        if !self.global_comments.is_empty() {
            writeln!(w)?;
        }

        self.super_preamble(w, func)
    }

    fn write_entity_definition(
        &mut self,
        w: &mut dyn fmt::Write,
        _func: &Function,
        entity: AnyEntity,
        value: &dyn fmt::Display,
    ) -> fmt::Result {
        write!(w, "    {} = {}", entity, value)?;

        if let Some(comment) = self.entity_comments.get(&entity) {
            writeln!(w, " ; {}", comment.replace('\n', "\n; "))
        } else {
            writeln!(w)
        }
    }

    fn write_block_header(
        &mut self,
        w: &mut dyn fmt::Write,
        func: &Function,
        block: Block,
        indent: usize,
    ) -> fmt::Result {
        PlainWriter.write_block_header(w, func, block, indent)
    }

    fn write_instruction(
        &mut self,
        w: &mut dyn fmt::Write,
        func: &Function,
        aliases: &SecondaryMap<Value, Vec<Value>>,
        inst: Inst,
        indent: usize,
    ) -> fmt::Result {
        if let Some(comment) = self.entity_comments.get(&inst.into()) {
            writeln!(w, "; {}", comment.replace('\n', "\n; "))?;
        }
        PlainWriter.write_instruction(w, func, aliases, inst, indent)?;

        Ok(())
    }
}
