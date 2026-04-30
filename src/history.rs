use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunRecord {
    pub id: String,
    pub mission: String,
    pub timestamp: DateTime<Utc>,
    pub status: RunStatus,
    pub steps: Vec<StepRecord>,
    pub elapsed_ms: u128,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum RunStatus {
    Success,
    Failed(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StepRecord {
    pub name: String,
    pub status: StepStatus,
    pub elapsed_ms: u128,
    pub attempts: u32,
    pub output: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum StepStatus {
    Success,
    Failed,
    Skipped,
}

fn history_path() -> PathBuf {
    PathBuf::from(".connor_history.json")
}

pub fn load_history() -> Vec<RunRecord> {
    let path = history_path();
    if !path.exists() {
        return vec![];
    }
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_run(record: RunRecord) -> Result<()> {
    let mut history = load_history();
    history.insert(0, record);
    history.truncate(50); // keep last 50 runs
    let json = serde_json::to_string_pretty(&history)?;
    fs::write(history_path(), json)?;
    Ok(())
}

pub fn last_failed_step() -> Option<String> {
    let history = load_history();
    let last = history.first()?;
    if let RunStatus::Failed(step) = &last.status {
        Some(step.clone())
    } else {
        None
    }
}
