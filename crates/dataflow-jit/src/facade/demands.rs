use crate::{
    codegen::json::{JsonDeserConfig, JsonSerConfig},
    ir::{DemandId, DemandIdGen, LayoutId},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub struct Demands {
    #[allow(clippy::type_complexity)]
    pub(super) csv: BTreeMap<DemandId, (LayoutId, Vec<(usize, usize, Option<String>)>)>,
    pub(super) deserialize_json: BTreeMap<DemandId, JsonDeserConfig>,
    pub(super) serialize_json: BTreeMap<DemandId, JsonSerConfig>,
    pub(super) demand_layouts: BTreeMap<DemandId, LayoutId>,
    ids: DemandIdGen,
}

impl Demands {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            csv: BTreeMap::new(),
            deserialize_json: BTreeMap::new(),
            serialize_json: BTreeMap::new(),
            demand_layouts: BTreeMap::new(),
            ids: DemandIdGen::new(),
        }
    }

    pub fn total_demands(&self) -> usize {
        self.csv.len() + self.deserialize_json.len() + self.serialize_json.len()
    }

    fn next_demand(&mut self, layout: LayoutId) -> DemandId {
        let id = self.ids.next();
        self.demand_layouts.insert(id, layout);
        id
    }

    #[must_use = "deserialization demands can only be used through their `DemandId`"]
    pub fn add_csv_deserialize(
        &mut self,
        layout: LayoutId,
        column_mappings: Vec<(usize, usize, Option<String>)>,
    ) -> DemandId {
        let id = self.next_demand(layout);
        self.csv.insert(id, (layout, column_mappings));
        id
    }

    #[must_use = "deserialization demands can only be used through their `DemandId`"]
    pub fn add_json_deserialize(&mut self, mappings: JsonDeserConfig) -> DemandId {
        let id = self.next_demand(mappings.layout);
        self.deserialize_json.insert(id, mappings);
        id
    }

    #[must_use = "serialization demands can only be used through their `DemandId`"]
    pub fn add_json_serialize(&mut self, mappings: JsonSerConfig) -> DemandId {
        let id = self.next_demand(mappings.layout);
        self.serialize_json.insert(id, mappings);
        id
    }

    // TODO: Return result
    pub(super) fn validate(&self) {
        let mut destination_columns = BTreeSet::new();
        for (&demand_id, (layout_id, csv_columns)) in &self.csv {
            for &(csv_column, row_column, ref fmt) in csv_columns {
                if !destination_columns.insert(row_column) {
                    panic!(
                        "multiple csv columns write to the same row column for \
                         demand {demand_id}, layout {layout_id} `[{csv_column}, {row_column}, {fmt:?}]`"
                    );
                }
            }

            destination_columns.clear();
        }
    }
}
