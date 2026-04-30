use crate::history::{RunRecord, RunStatus};
use anyhow::Result;
use serde_json::json;

pub async fn notify(url: &str, record: &RunRecord) -> Result<()> {
    let status = match &record.status {
        RunStatus::Success => "success",
        RunStatus::Failed(_) => "failed",
    };

    let failed_step = match &record.status {
        RunStatus::Failed(s) => Some(s.as_str()),
        _ => None,
    };

    let payload = json!({
        "mission": record.mission,
        "status": status,
        "elapsed_ms": record.elapsed_ms,
        "timestamp": record.timestamp.to_rfc3339(),
        "failed_step": failed_step,
        "steps": record.steps.iter().map(|s| json!({
            "name": s.name,
            "status": format!("{:?}", s.status).to_lowercase(),
            "elapsed_ms": s.elapsed_ms,
            "attempts": s.attempts,
        })).collect::<Vec<_>>(),
    });

    let client = reqwest::Client::new();
    client
        .post(url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;

    Ok(())
}
