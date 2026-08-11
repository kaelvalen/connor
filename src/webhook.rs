use crate::history::{RunRecord, RunStatus};
use anyhow::Result;
use serde_json::json;

pub fn build_payload(record: &RunRecord) -> serde_json::Value {
    let status = match &record.status {
        RunStatus::Success => "success",
        RunStatus::Failed(_) => "failed",
    };

    let failed_step = match &record.status {
        RunStatus::Failed(s) => Some(s.as_str()),
        _ => None,
    };

    json!({
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
    })
}

pub async fn notify(url: &str, record: &RunRecord) -> Result<()> {
    let payload = build_payload(record);

    let client = reqwest::Client::new();
    client
        .post(url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{StepRecord, StepStatus};
    use chrono::Utc;

    fn mk_record() -> RunRecord {
        RunRecord {
            id: "run-1".into(),
            mission: "operation-cleanup".into(),
            timestamp: Utc::now(),
            status: RunStatus::Failed("test".into()),
            elapsed_ms: 1234,
            steps: vec![
                StepRecord {
                    name: "lint".into(),
                    status: StepStatus::Success,
                    elapsed_ms: 34,
                    attempts: 1,
                    output: None,
                },
                StepRecord {
                    name: "test".into(),
                    status: StepStatus::Failed,
                    elapsed_ms: 800,
                    attempts: 3,
                    output: None,
                },
                StepRecord {
                    name: "build".into(),
                    status: StepStatus::Skipped,
                    elapsed_ms: 0,
                    attempts: 0,
                    output: None,
                },
            ],
        }
    }

    #[test]
    fn payload_has_expected_shape() {
        let v = build_payload(&mk_record());
        assert_eq!(v["mission"], "operation-cleanup");
        assert_eq!(v["status"], "failed");
        assert_eq!(v["elapsed_ms"], 1234);
        assert!(v["timestamp"].is_string());
        assert_eq!(v["failed_step"], "test");
        let steps = v["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0]["name"], "lint");
        assert_eq!(steps[0]["status"], "success");
        assert_eq!(steps[1]["status"], "failed");
        assert_eq!(steps[1]["attempts"], 3);
        assert_eq!(steps[2]["status"], "skipped");
    }

    #[test]
    fn payload_for_success_run_has_no_failed_step() {
        let mut record = mk_record();
        record.status = RunStatus::Success;
        let v = build_payload(&record);
        assert_eq!(v["status"], "success");
        assert!(v["failed_step"].is_null());
    }

    #[tokio::test]
    async fn notify_posts_payload_to_endpoint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            request
        });

        let record = mk_record();
        notify(&format!("http://{addr}/hook"), &record)
            .await
            .unwrap();

        let request = server.await.unwrap();
        assert!(request.starts_with("POST /hook HTTP/1.1"));
        assert!(request.contains("content-type: application/json"));
        assert!(request.contains("\"mission\":\"operation-cleanup\""));
        assert!(request.contains("\"status\":\"failed\""));
    }
}
