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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_util::CWD_LOCK;
    use std::path::PathBuf;

    fn mk_record(id: &str, status: RunStatus) -> RunRecord {
        RunRecord {
            id: id.into(),
            mission: "m".into(),
            timestamp: Utc::now(),
            status,
            elapsed_ms: 100,
            steps: vec![StepRecord {
                name: "s".into(),
                status: StepStatus::Success,
                elapsed_ms: 50,
                attempts: 1,
                output: Some("out".into()),
            }],
        }
    }

    /// Run a test body inside a fresh temp working directory so the relative
    /// history path is isolated. Serialized against other cwd-mutating tests.
    fn in_isolated_cwd(body: impl FnOnce(PathBuf)) {
        let _guard = CWD_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!(
            "connor-history-test-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let old = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        body(dir.clone());
        std::env::set_current_dir(old).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn save_load_roundtrip() {
        in_isolated_cwd(|_| {
            save_run(mk_record("r1", RunStatus::Success)).unwrap();
            save_run(mk_record("r2", RunStatus::Failed("s".into()))).unwrap();
            let history = load_history();
            assert_eq!(history.len(), 2);
            assert_eq!(history[0].id, "r2");
            assert_eq!(history[0].status, RunStatus::Failed("s".into()));
            assert_eq!(history[0].steps[0].output.as_deref(), Some("out"));
        });
    }

    #[test]
    fn history_is_truncated_to_50_runs() {
        in_isolated_cwd(|_| {
            for i in 0..55 {
                save_run(mk_record(&format!("r{i}"), RunStatus::Success)).unwrap();
            }
            let history = load_history();
            assert_eq!(history.len(), 50);
            assert_eq!(history[0].id, "r54");
            assert_eq!(history[49].id, "r5");
        });
    }

    #[test]
    fn load_history_returns_empty_when_missing() {
        in_isolated_cwd(|_| {
            assert!(load_history().is_empty());
        });
    }

    #[test]
    fn last_failed_step_reads_first_failed_run() {
        in_isolated_cwd(|_| {
            assert_eq!(last_failed_step(), None);
            save_run(mk_record("ok", RunStatus::Success)).unwrap();
            assert_eq!(last_failed_step(), None);
            save_run(mk_record("bad", RunStatus::Failed("deploy".into()))).unwrap();
            assert_eq!(last_failed_step().as_deref(), Some("deploy"));
        });
    }
}
