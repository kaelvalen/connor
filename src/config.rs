use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub mission: Mission,
    pub steps: Vec<Step>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Mission {
    pub name: String,
    pub target: Option<String>,
    pub retry_limit: Option<u32>,
    pub stop_on_failure: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Step {
    pub name: String,
    pub cmd: String,
    pub depends_on: Option<Vec<String>>,
    pub retry: Option<u32>,
    pub env: Option<std::collections::HashMap<String, String>>,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Mission briefing not found: {}", path))?;
        toml::from_str(&content).context("Failed to parse mission briefing")
    }

    pub fn default_toml() -> &'static str {
        r#"[mission]
name = "operation-cleanup"
target = "production"
retry_limit = 3
stop_on_failure = true

[[steps]]
name = "compile"
cmd = "cargo build --release"

[[steps]]
name = "test"
cmd = "cargo test"
depends_on = ["compile"]

[[steps]]
name = "deploy"
cmd = "echo Deploying to production"
depends_on = ["test"]
retry = 2
"#
    }
}

pub fn find_config() -> &'static str {
    if Path::new("connor.toml").exists() {
        "connor.toml"
    } else {
        ".connor.toml"
    }
}