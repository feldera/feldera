//! The rows of the support bundle dialog and how each edits
//! [`BundleOptions`]: checkboxes for what to gather, a stepper for how many
//! collections, and a row that starts the download.

use crate::gateway::BundleOptions;

/// One row of the dialog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BundleSetting {
    /// Collect fresh data before answering.
    Collect,
    /// How many collections to include, newest first.
    Limit,
    CircuitProfile,
    HeapProfile,
    Metrics,
    Logs,
    Stats,
    PipelineConfig,
    SystemConfig,
    DataflowGraph,
    PipelineEvents,
    /// Not a setting: starts the download with the settings above.
    Download,
}

/// The collection counts the stepper walks through; the last is "all".
const LIMIT_STEPS: [Option<u64>; 6] = [Some(1), Some(2), Some(3), Some(5), Some(10), None];

impl BundleSetting {
    pub const ALL: [Self; 12] = [
        Self::Collect,
        Self::Limit,
        Self::CircuitProfile,
        Self::HeapProfile,
        Self::Metrics,
        Self::Logs,
        Self::Stats,
        Self::PipelineConfig,
        Self::SystemConfig,
        Self::DataflowGraph,
        Self::PipelineEvents,
        Self::Download,
    ];

    /// The row's text.
    ///
    /// ```
    /// use f5a::app::bundle_settings::BundleSetting;
    ///
    /// assert_eq!(BundleSetting::Logs.label(), "Include logs");
    /// assert_eq!(BundleSetting::Download.label(), "Download now");
    /// ```
    pub const fn label(self) -> &'static str {
        match self {
            Self::Collect => "Collect fresh data before downloading",
            Self::Limit => "Collections to include",
            Self::CircuitProfile => "Include circuit profile",
            Self::HeapProfile => "Include heap profile",
            Self::Metrics => "Include metrics",
            Self::Logs => "Include logs",
            Self::Stats => "Include stats",
            Self::PipelineConfig => "Include pipeline configuration",
            Self::SystemConfig => "Include system configuration",
            Self::DataflowGraph => "Include dataflow graph",
            Self::PipelineEvents => "Include pipeline events",
            Self::Download => "Download now",
        }
    }

    /// The checkbox state of a boolean row; `None` for the stepper and the
    /// download row.
    ///
    /// ```
    /// use f5a::app::bundle_settings::BundleSetting;
    /// use f5a::gateway::BundleOptions;
    ///
    /// let options = BundleOptions { logs: false, ..Default::default() };
    /// assert_eq!(BundleSetting::Logs.is_on(&options), Some(false));
    /// assert_eq!(BundleSetting::Collect.is_on(&options), Some(true));
    /// assert_eq!(BundleSetting::Limit.is_on(&options), None);
    /// ```
    pub fn is_on(self, options: &BundleOptions) -> Option<bool> {
        Some(match self {
            Self::Collect => options.collect,
            Self::CircuitProfile => options.circuit_profile,
            Self::HeapProfile => options.heap_profile,
            Self::Metrics => options.metrics,
            Self::Logs => options.logs,
            Self::Stats => options.stats,
            Self::PipelineConfig => options.pipeline_config,
            Self::SystemConfig => options.system_config,
            Self::DataflowGraph => options.dataflow_graph,
            Self::PipelineEvents => options.pipeline_events,
            Self::Limit | Self::Download => return None,
        })
    }

    /// Flip a checkbox row; the stepper row steps up instead, and the
    /// download row is left to the caller.
    ///
    /// ```
    /// use f5a::app::bundle_settings::BundleSetting;
    /// use f5a::gateway::BundleOptions;
    ///
    /// let mut options = BundleOptions::default();
    /// BundleSetting::Metrics.toggle(&mut options);
    /// assert!(!options.metrics);
    /// BundleSetting::Limit.toggle(&mut options);
    /// assert_eq!(options.limit, Some(2));
    /// ```
    pub fn toggle(self, options: &mut BundleOptions) {
        match self {
            Self::Collect => options.collect = !options.collect,
            Self::Limit => step_limit(options, 1),
            Self::CircuitProfile => options.circuit_profile = !options.circuit_profile,
            Self::HeapProfile => options.heap_profile = !options.heap_profile,
            Self::Metrics => options.metrics = !options.metrics,
            Self::Logs => options.logs = !options.logs,
            Self::Stats => options.stats = !options.stats,
            Self::PipelineConfig => options.pipeline_config = !options.pipeline_config,
            Self::SystemConfig => options.system_config = !options.system_config,
            Self::DataflowGraph => options.dataflow_graph = !options.dataflow_graph,
            Self::PipelineEvents => options.pipeline_events = !options.pipeline_events,
            Self::Download => {}
        }
    }
}

/// Move the collection limit along the ladder 1, 2, 3, 5, 10, all, stopping
/// at the ends; a value off the ladder lands on the neighboring rung.
///
/// ```
/// use f5a::app::bundle_settings::step_limit;
/// use f5a::gateway::BundleOptions;
///
/// let mut options = BundleOptions::default();
/// step_limit(&mut options, -1);
/// assert_eq!(options.limit, Some(1), "one is the floor");
/// for _ in 0..9 {
///     step_limit(&mut options, 1);
/// }
/// assert_eq!(options.limit, None, "past ten comes everything");
/// ```
pub fn step_limit(options: &mut BundleOptions, delta: i32) {
    // `None` means every collection, so it sits above every count.
    let current = options.limit;
    let above = |step: Option<u64>| match (step, current) {
        (None, None) | (Some(_), None) => false,
        (None, Some(_)) => true,
        (Some(step), Some(value)) => step > value,
    };
    let below = |step: Option<u64>| match (step, current) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(step), Some(value)) => step < value,
    };
    let next = if delta > 0 {
        LIMIT_STEPS.iter().copied().find(|step| above(*step))
    } else if delta < 0 {
        LIMIT_STEPS.iter().rev().copied().find(|step| below(*step))
    } else {
        None
    };
    if let Some(step) = next {
        options.limit = step;
    }
}

/// The stepper's text, e.g. "current only", "last 3", or "all retained".
///
/// ```
/// use f5a::app::bundle_settings::limit_label;
/// use f5a::gateway::BundleOptions;
///
/// assert_eq!(limit_label(&BundleOptions::default()), "current only");
/// assert_eq!(limit_label(&BundleOptions { limit: Some(3), ..Default::default() }), "last 3");
/// assert_eq!(limit_label(&BundleOptions { limit: None, ..Default::default() }), "all retained");
/// ```
pub fn limit_label(options: &BundleOptions) -> String {
    match options.limit {
        Some(1) => "current only".to_string(),
        Some(count) => format!("last {count}"),
        None => "all retained".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{BundleSetting, LIMIT_STEPS, limit_label, step_limit};
    use crate::gateway::BundleOptions;

    #[test]
    fn every_checkbox_row_toggles_its_own_flag_and_nothing_else() {
        for setting in BundleSetting::ALL {
            let Some(before) = setting.is_on(&BundleOptions::default()) else {
                continue;
            };
            let mut options = BundleOptions::default();
            setting.toggle(&mut options);
            assert_eq!(setting.is_on(&options), Some(!before), "{setting:?}");
            let flipped = BundleSetting::ALL
                .iter()
                .filter(|other| other.is_on(&options) != other.is_on(&BundleOptions::default()))
                .count();
            assert_eq!(flipped, 1, "{setting:?} touched another flag");
            assert_eq!(options.limit, Some(1), "{setting:?} moved the limit");
        }
    }

    #[test]
    fn the_stepper_walks_the_ladder_and_stops_at_both_ends() {
        let mut options = BundleOptions::default();
        for expected in LIMIT_STEPS.iter().skip(1) {
            step_limit(&mut options, 1);
            assert_eq!(options.limit, *expected);
        }
        step_limit(&mut options, 1);
        assert_eq!(options.limit, None, "the top holds");
        step_limit(&mut options, -1);
        assert_eq!(options.limit, Some(10));
        assert_eq!(limit_label(&options), "last 10");
        // A value off the ladder lands on the neighboring rung.
        options.limit = Some(7);
        step_limit(&mut options, 1);
        assert_eq!(options.limit, Some(10));
        options.limit = Some(7);
        step_limit(&mut options, -1);
        assert_eq!(options.limit, Some(5));
        step_limit(&mut options, 0);
        assert_eq!(options.limit, Some(5), "a zero step is a no-op");
    }

    #[test]
    fn the_download_row_is_inert_as_a_setting() {
        let mut options = BundleOptions::default();
        BundleSetting::Download.toggle(&mut options);
        assert_eq!(options, BundleOptions::default());
        assert_eq!(BundleSetting::Download.is_on(&options), None);
    }
}
