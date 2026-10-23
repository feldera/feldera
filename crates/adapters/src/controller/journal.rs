use std::collections::{HashMap, HashSet};

use feldera_adapterlib::transport::Step;
use feldera_types::{config::InputEndpointConfig, transaction::TransactionId};
use rmpv::Value as RmpValue;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// A record in the journal, useful for replaying a step.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StepMetadata {
    /// Step number.
    pub step: Step,

    /// Id of the transaction whose circuit evaluation consumed this step's
    /// input.
    ///
    /// Replay groups journaled steps into transactions by this id and commits
    /// at each boundary (where the id changes from one journaled step to the
    /// next), reproducing the original transaction structure regardless of how
    /// many physical steps each commit took during recording. Fault-tolerant
    /// output connectors key their exactly-once dedup on this id, which (unlike
    /// the physical step number) is deterministic across replay.
    pub transaction_id: TransactionId,

    /// Names of input endpoints removed in the step.
    pub remove_inputs: HashSet<String>,

    /// Input endpoints added in the step, with their configurations.
    ///
    /// If a given name is in both `remove_inputs` and `add_inputs`, then the
    /// step replaced an endpoint with the given name by a new, otherwise
    /// unrelated endpoint.
    pub add_inputs: HashMap<String, InputEndpointConfig>,

    /// Changes to existing input endpoints within the step.
    ///
    /// Currently we only log changes to the endpoint's paused state.
    pub changed_inputs: HashMap<String, bool>,

    /// Logs for the endpoints included in the step.
    ///
    /// A given endpoint is included if it existed before the step and is not in
    /// `remove_inputs`, or if it is included in `add_inputs`.
    pub input_logs: HashMap<String, InputLog>,

    /// Changes to existing output endpoints within the step.
    ///
    /// Currently we only log changes to the endpoint's paused state, so that
    /// replay drops the output of a paused endpoint exactly as the recorded
    /// run did.
    ///
    /// This field must stay last: records are encoded as arrays, so a journal
    /// written before this field existed is one element short, and only a
    /// trailing field can be defaulted.
    #[serde(default)]
    pub changed_outputs: HashMap<String, bool>,
}

/// A journal record for a single endpoint for a single step.
///
/// The endpoint's name is the key in [StepMetadata::input_logs].
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InputLog {
    /// Data for replay.
    ///
    /// This is filled in by input adapters that log actual data records
    /// (e.g. the HTTP and ad hoc query input adapters). For the other adapters,
    /// which only log metadata (such as record offsets), this field is
    /// [RmpValue::Nil].
    pub data: RmpValue,

    /// Metadata for seek and replay.
    ///
    /// This is filled in by input adapters that log metadata (such as record
    /// offsets).
    #[serde(with = "as_json_string")]
    pub metadata: JsonValue,

    /// Checksums of the input data.
    pub checksums: InputChecksums,
}

mod as_json_string {
    use serde::de::{Deserialize, Deserializer};
    use serde::ser::{Serialize, Serializer};
    use serde_json::Value as JsonValue;

    pub(super) fn serialize<S>(value: &JsonValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = serde_json::to_string(value).map_err(serde::ser::Error::custom)?;
        s.serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<JsonValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        serde_json_path_to_error::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Input data statistics.
///
/// This allows checking that an input step replayed the same data as the
/// original run.
#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct InputChecksums {
    /// Number of records.
    pub num_records: u64,

    /// Hash of the records.
    pub hash: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use feldera_adapterlib::transport::Step;
    use feldera_types::{config::InputEndpointConfig, transaction::TransactionId};
    use serde::Serialize;

    use super::{InputLog, StepMetadata};

    /// A record written before [StepMetadata::changed_outputs] existed is one
    /// element short of the current struct.  It must still decode, with no
    /// output endpoint changes to replay.
    #[test]
    fn test_read_record_without_changed_outputs() {
        /// [StepMetadata] as it was before `changed_outputs` was added.
        #[derive(Serialize)]
        struct OldStepMetadata {
            step: Step,
            transaction_id: TransactionId,
            remove_inputs: HashSet<String>,
            add_inputs: HashMap<String, InputEndpointConfig>,
            changed_inputs: HashMap<String, bool>,
            input_logs: HashMap<String, InputLog>,
        }

        let old = OldStepMetadata {
            step: 7,
            transaction_id: 3,
            remove_inputs: HashSet::from(["gone".into()]),
            add_inputs: HashMap::new(),
            changed_inputs: HashMap::from([("paused_input".to_string(), true)]),
            input_logs: HashMap::new(),
        };

        let mut encoded = Vec::new();
        rmp_serde::encode::write(&mut encoded, &old).unwrap();
        let decoded = rmp_serde::decode::from_slice::<StepMetadata>(&encoded).unwrap();

        assert_eq!(decoded.step, 7);
        assert_eq!(decoded.changed_inputs, old.changed_inputs);
        assert!(decoded.changed_outputs.is_empty());
    }
}
