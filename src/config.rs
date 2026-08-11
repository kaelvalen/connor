use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub mission: Mission,
    pub steps: Vec<Step>,
    #[serde(skip)]
    pub base_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Mission {
    pub name: String,
    pub target: Option<String>,
    pub retry_limit: Option<u32>,
    pub stop_on_failure: Option<bool>,
    pub env_file: Option<String>,
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Step {
    pub name: String,
    pub cmd: String,
    pub depends_on: Option<Vec<String>>,
    pub retry: Option<u32>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
    pub timeout_secs: Option<u64>,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Mission briefing not found: {}", path))?;
        let mut config: Config =
            toml::from_str(&content).context("Failed to parse mission briefing")?;
        config.base_dir = Path::new(path)
            .parent()
            .map(|p| p.to_path_buf())
            .filter(|p| !p.as_os_str().is_empty());
        Ok(config)
    }

    pub fn default_toml() -> &'static str {
        r#"[mission]
name = "operation-cleanup"
target = "production"
retry_limit = 3
stop_on_failure = true
# timeout_secs = 600      # default per-step timeout
# env_file = ".env"      # load KEY=VALUE pairs for ${VAR} interpolation

[[steps]]
name = "compile"
cmd = "cargo build --release"
# timeout_secs = 300
# cwd = "backend"        # run in a subdirectory

[[steps]]
name = "test"
cmd = "cargo test"
depends_on = ["compile"]

[[steps]]
name = "deploy"
cmd = "echo Deploying to ${TARGET_HOST}"
depends_on = ["test"]
retry = 2
env = { DEPLOY_TOKEN = "${DEPLOY_TOKEN}" }
"#
    }
}

/// Find `connor.toml` (or `.connor.toml`) by walking up from the current
/// directory, git-style. The first match wins.
pub fn find_config() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        for name in ["connor.toml", ".connor.toml"] {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        if !dir.pop() {
            return None;
        }
    }
}

#[cfg(test)]
pub mod test_util {
    use std::sync::Mutex;
    /// Serializes tests that change the process working directory.
    pub static CWD_LOCK: Mutex<()> = Mutex::new(());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::test_util::CWD_LOCK;

    fn step(name: &str) -> Step {
        Step {
            name: name.into(),
            cmd: "true".into(),
            depends_on: None,
            retry: None,
            env: None,
            cwd: None,
            timeout_secs: None,
        }
    }

    #[test]
    fn parses_full_config_with_new_fields() {
        let toml = r#"
[mission]
name = "m"
env_file = "ci.env"
timeout_secs = 120

[[steps]]
name = "a"
cmd = "echo hi"
cwd = "sub"
timeout_secs = 30
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert_eq!(cfg.mission.env_file.as_deref(), Some("ci.env"));
        assert_eq!(cfg.mission.timeout_secs, Some(120));
        assert_eq!(cfg.steps[0].cwd.as_deref(), Some("sub"));
        assert_eq!(cfg.steps[0].timeout_secs, Some(30));
    }

    #[test]
    fn missing_fields_default_to_none() {
        let toml = "[mission]\nname = \"m\"\n\n[[steps]]\nname = \"a\"\ncmd = \"x\"\n";
        let cfg: Config = toml::from_str(toml).unwrap();
        assert!(cfg.mission.env_file.is_none());
        assert!(cfg.mission.timeout_secs.is_none());
        assert!(cfg.steps[0].cwd.is_none());
        assert!(cfg.steps[0].timeout_secs.is_none());
    }

    #[test]
    fn default_toml_is_valid() {
        let cfg: Config = toml::from_str(Config::default_toml()).unwrap();
        assert_eq!(cfg.steps.len(), 3);
        assert_eq!(
            cfg.steps[2].depends_on.as_deref(),
            Some(&["test".to_string()][..])
        );
    }

    #[test]
    fn load_sets_base_dir() {
        let dir = std::env::temp_dir().join(format!("connor-cfg-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("connor.toml");
        std::fs::write(&path, Config::default_toml()).unwrap();
        let cfg = Config::load(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.base_dir, Some(dir.clone()));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_errors_on_missing_file() {
        assert!(Config::load("/nonexistent/connor.toml").is_err());
    }

    #[test]
    fn load_errors_on_invalid_toml() {
        let dir = std::env::temp_dir().join(format!("connor-cfg-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("connor.toml");
        std::fs::write(&path, "mission = [").unwrap();
        assert!(Config::load(path.to_str().unwrap()).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn find_config_walks_up_directories() {
        let _guard = CWD_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("connor-find-{}", std::process::id()));
        let nested = root.join("a").join("b");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(root.join("connor.toml"), "[mission]\nname = \"m\"\n").unwrap();

        let old_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(&nested).unwrap();
        let found = find_config();
        std::env::set_current_dir(old_cwd).unwrap();

        assert_eq!(found, Some(root.join("connor.toml")));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn find_config_prefers_connor_toml_over_hidden() {
        let _guard = CWD_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("connor-find2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("connor.toml"), "").unwrap();
        std::fs::write(dir.join(".connor.toml"), "").unwrap();

        let old_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        let found = find_config();
        std::env::set_current_dir(old_cwd).unwrap();

        assert_eq!(found, Some(dir.join("connor.toml")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn step_is_cloneable_and_defaults_work() {
        let s = step("x");
        let s2 = s.clone();
        assert_eq!(s.name, s2.name);
    }
}
