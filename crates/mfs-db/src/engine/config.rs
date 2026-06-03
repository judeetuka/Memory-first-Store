use crate::engine::DurabilityMode;
use std::path::PathBuf;

pub const DEFAULT_MAX_COLLECTIONS: usize = 1024;
pub const DEFAULT_RAW_INITIAL_CAPACITY: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    pub max_collections: usize,
    pub raw_initial_capacity: usize,
    pub durability: DurabilityMode,
    pub wal_path: Option<PathBuf>,
    pub checkpoint_dir: Option<PathBuf>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_collections: DEFAULT_MAX_COLLECTIONS,
            raw_initial_capacity: DEFAULT_RAW_INITIAL_CAPACITY,
            durability: DurabilityMode::MemoryOnly,
            wal_path: None,
            checkpoint_dir: None,
        }
    }
}

impl EngineConfig {
    pub fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }

    pub fn with_wal_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.wal_path = Some(path.into());
        self
    }

    pub fn with_checkpoint_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.checkpoint_dir = Some(dir.into());
        self
    }
}
