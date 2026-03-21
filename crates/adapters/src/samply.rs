use tracing::{error, info};

pub(crate) type SamplyProfile = Option<Result<Vec<u8>, String>>;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) enum SamplyStatus {
    #[default]
    Idle,
    InProgress {
        expected_after: chrono::DateTime<chrono::Utc>,
    },
}

#[derive(Debug, Default, Clone)]
pub(crate) struct SamplyState {
    pub(crate) last_profile: SamplyProfile,
    pub(crate) samply_status: SamplyStatus,
}

impl SamplyState {
    pub(crate) fn start_profiling(&mut self, expected_after: chrono::DateTime<chrono::Utc>) {
        self.samply_status = SamplyStatus::InProgress { expected_after };
    }

    pub(crate) fn complete_profiling(&mut self, result: Result<Vec<u8>, anyhow::Error>) {
        match result {
            Ok(data) => {
                info!("collected samply profile: {} bytes", data.len());
                self.last_profile = Some(Ok(data));
            }
            Err(error) => {
                error!("samply profiling failed: {:?}", error);
                self.last_profile = Some(Err(error.to_string()));
            }
        }
        self.samply_status = SamplyStatus::Idle;
    }
}
