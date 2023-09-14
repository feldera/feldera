use crate::{
    codegen::json::{JsonDeserConfig, JsonSerConfig},
    ir::LayoutId,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub struct Demands {
    #[allow(clippy::type_complexity)]
    pub(super) csv: BTreeMap<LayoutId, Vec<(usize, usize, Option<String>)>>,
    pub(super) deserialize_json: BTreeMap<LayoutId, JsonDeserConfig>,
    pub(super) serialize_json: BTreeMap<LayoutId, JsonSerConfig>,
}

impl Demands {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            csv: BTreeMap::new(),
            deserialize_json: BTreeMap::new(),
            serialize_json: BTreeMap::new(),
        }
    }

    pub fn add_csv_deserialize(
        &mut self,
        layout: LayoutId,
        column_mappings: Vec<(usize, usize, Option<String>)>,
    ) {
        let displaced = self.csv.insert(layout, column_mappings);
        assert_eq!(displaced, None);
    }

    pub fn add_json_deserialize(&mut self, layout: LayoutId, mappings: JsonDeserConfig) {
        let displaced = self.deserialize_json.insert(layout, mappings);
        assert_eq!(displaced, None);
    }

    pub fn add_json_serialize(&mut self, layout: LayoutId, mappings: JsonSerConfig) {
        let displaced = self.serialize_json.insert(layout, mappings);
        assert_eq!(displaced, None);
    }

    // TODO: Return result
    pub(super) fn validate(&self) {
        let mut destination_columns = BTreeSet::new();
        for (&layout_id, csv_columns) in &self.csv {
            for &(csv_column, row_column, ref fmt) in csv_columns {
                if !destination_columns.insert(row_column) {
                    panic!(
                        "multiple csv columns write to the same row column for \
                         layout {layout_id}, `[{csv_column}, {row_column}, {fmt:?}]`"
                    );
                }
            }

            destination_columns.clear();
        }
    }
}
