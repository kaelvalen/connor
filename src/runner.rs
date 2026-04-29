use crate::config::{Config, Step};
use crate::history::{RunRecord, RunStatus, StepRecord, StepStatus};
use crate::logger;
use anyhow::Result;
use chrono::Utc;
use colored::Colorize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::process::Command;
use tokio::sync::Mutex;

pub struct Runner {
    pub config: Config,
    pub from_step: Option<String>,
    pub dry_run: bool,
    pub log_file: Option<String>,
}

pub struct RunResult {
    pub success: bool,
    pub record: RunRecord,
}

impl Runner {
    pub fn new(
        config: Config,
        from_step: Option<String>,
        dry_run: bool,
        log_file: Option<String>,
    ) -> Self {
        Self { config, from_step, dry_run, log_file }
    }

    pub async fn run(&self) -> Result<RunResult> {
        let mission = &self.config.mission;
        let target = mission.target.clone().unwrap_or_else(|| "unknown".into());
        let retry_limit = mission.retry_limit.unwrap_or(3);
        let stop_on_failure = mission.stop_on_failure.unwrap_or(true);

        logger::mission_start(&mission.name, &target);

        if self.dry_run {
            return self.dry_run_preview().await;
        }

        let stages = self.build_stages()?;
        let total_steps: usize = stages.iter().map(|s| s.len()).sum();
        let start = Instant::now();
        let run_id = Utc::now().timestamp_millis().to_string();

        let step_records: Arc<Mutex<Vec<StepRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let mut failed_step: Option<String> = None;
        let mut step_index = 0usize;

        let skipping_until = self.from_step.clone();
        let mut skipping = skipping_until.is_some();

        'outer: for stage in &stages {
            if skipping {
                if let Some(ref from) = skipping_until {
                    if stage.iter().any(|s| &s.name == from) {
                        skipping = false;
                    }
                }
            }

            if skipping {
                for step in stage {
                    logger::info(&format!("Skipping {} (before retry target)", step.name));
                    step_records.lock().await.push(StepRecord {
                        name: step.name.clone(),
                        status: StepStatus::Skipped,
                        elapsed_ms: 0,
                        attempts: 0,
                        output: None,
                    });
                }
                step_index += stage.len();
                continue;
            }

            // Parallel stage
            if stage.len() > 1 {
                logger::parallel_stage_start(
                    stage.iter().map(|s| s.name.as_str()).collect(),
                    step_index + 1,
                    total_steps,
                );
            }

            let mut handles = Vec::new();

            for step in stage {
                step_index += 1;
                let step = step.clone();
                let records_clone = Arc::clone(&step_records);
                let log_file = self.log_file.clone();
                let idx = step_index;
                let total = total_steps;
                let retry = retry_limit;
                let is_solo = stage.len() == 1;

                if is_solo {
                    logger::step_start(&step.name, idx, total);
                }

                let handle = tokio::spawn(async move {
                    let max_attempts = (step.retry.unwrap_or(0) + 1).max(1);
                    let effective_retry = retry.min(max_attempts);

                    let (success, elapsed, attempts, output) =
                        execute_step(&step, effective_retry, log_file.as_deref()).await;

                    if is_solo {
                        if success {
                            logger::step_success(&step.name, elapsed);
                        } else {
                            logger::step_failure(&step.name, None);
                        }
                    }

                    records_clone.lock().await.push(StepRecord {
                        name: step.name.clone(),
                        status: if success { StepStatus::Success } else { StepStatus::Failed },
                        elapsed_ms: elapsed,
                        attempts,
                        output,
                    });

                    (step.name.clone(), success)
                });

                handles.push(handle);
            }

            let mut stage_failed: Option<String> = None;
            for handle in handles {
                let (name, success) = handle.await?;
                if !success && stage_failed.is_none() {
                    stage_failed = Some(name);
                }
            }

            if stage.len() > 1 {
                logger::parallel_stage_end(stage_failed.is_none());
            }

            if let Some(ref f) = stage_failed {
                failed_step = Some(f.clone());
                if stop_on_failure {
                    break 'outer;
                }
            }
        }

        let elapsed_total = start.elapsed().as_millis();
        let success = failed_step.is_none();

        if success {
            logger::pipeline_success(elapsed_total);
        } else {
            logger::pipeline_failure(failed_step.as_deref().unwrap_or("unknown"));
        }

        let mut records = step_records.lock().await.clone();
        let order: Vec<&str> = self.config.steps.iter().map(|s| s.name.as_str()).collect();
        records.sort_by_key(|r| order.iter().position(|&n| n == r.name).unwrap_or(999));

        let record = RunRecord {
            id: run_id,
            mission: mission.name.clone(),
            timestamp: Utc::now(),
            status: if success {
                RunStatus::Success
            } else {
                RunStatus::Failed(failed_step.unwrap_or_default())
            },
            steps: records,
            elapsed_ms: elapsed_total,
        };

        Ok(RunResult { success, record })
    }

    async fn dry_run_preview(&self) -> Result<RunResult> {
        let stages = self.build_stages()?;
        let mission = &self.config.mission;

        println!("{}", "DRY RUN — No commands will be executed\n".yellow().bold());

        for (i, stage) in stages.iter().enumerate() {
            if stage.len() > 1 {
                println!("{} [parallel]", format!("Stage {}:", i + 1).cyan().bold());
                for step in stage {
                    println!(
                        "  {} {} → {}",
                        "◈".yellow(),
                        step.name.white().bold(),
                        step.cmd.dimmed()
                    );
                    if let Some(ref deps) = step.depends_on {
                        println!("    {} {}", "depends:".dimmed(), deps.join(", ").dimmed());
                    }
                }
            } else {
                let step = &stage[0];
                println!(
                    "{} {} → {}",
                    format!("Step {}:", i + 1).cyan().bold(),
                    step.name.white().bold(),
                    step.cmd.dimmed()
                );
                if let Some(ref deps) = step.depends_on {
                    println!("  {} {}", "depends:".dimmed(), deps.join(", ").dimmed());
                }
                if let Some(retry) = step.retry {
                    println!("  {} {}", "retry:".dimmed(), retry.to_string().yellow());
                }
            }
            println!();
        }

        let total: usize = stages.iter().map(|s| s.len()).sum();
        println!(
            "{} {} steps across {} stages",
            "◈ Total:".cyan(),
            total,
            stages.len()
        );

        let record = RunRecord {
            id: "dry-run".into(),
            mission: mission.name.clone(),
            timestamp: Utc::now(),
            status: RunStatus::Success,
            steps: vec![],
            elapsed_ms: 0,
        };

        Ok(RunResult { success: true, record })
    }

    pub fn build_stages(&self) -> Result<Vec<Vec<Step>>> {
        let steps = &self.config.steps;
        let mut assigned: HashMap<String, usize> = HashMap::new();
        let mut stages: Vec<Vec<Step>> = Vec::new();

        for step in steps {
            let stage_idx = step
                .depends_on
                .as_ref()
                .map(|deps| {
                    deps.iter()
                        .filter_map(|d| assigned.get(d))
                        .copied()
                        .max()
                        .map(|s| s + 1)
                        .unwrap_or(0)
                })
                .unwrap_or(0);

            if stages.len() <= stage_idx {
                stages.resize_with(stage_idx + 1, Vec::new);
            }

            stages[stage_idx].push(step.clone());
            assigned.insert(step.name.clone(), stage_idx);
        }

        Ok(stages)
    }
}

async fn execute_step(
    step: &Step,
    max_attempts: u32,
    log_file: Option<&str>,
) -> (bool, u128, u32, Option<String>) {
    let mut attempts = 0u32;
    let start = Instant::now();
    let mut last_output: Option<String> = None;

    loop {
        attempts += 1;
        let result = spawn_step(step, log_file).await;

        match result {
            Ok((true, output)) => {
                return (true, start.elapsed().as_millis(), attempts, Some(output));
            }
            Ok((false, output)) => {
                last_output = Some(output);
                if attempts < max_attempts {
                    logger::step_retry(&step.name, attempts, max_attempts);
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                } else {
                    return (false, start.elapsed().as_millis(), attempts, last_output);
                }
            }
            Err(e) => {
                last_output = Some(e.to_string());
                if attempts < max_attempts {
                    logger::step_retry(&step.name, attempts, max_attempts);
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                } else {
                    return (false, start.elapsed().as_millis(), attempts, last_output);
                }
            }
        }
    }
}

async fn spawn_step(step: &Step, log_file: Option<&str>) -> Result<(bool, String)> {
    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", &step.cmd]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", &step.cmd]);
        c
    };

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    if let Some(ref env_vars) = step.env {
        for (k, v) in env_vars {
            cmd.env(k, v);
        }
    }

    let output = cmd.output().await?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let combined = format!("{}{}", stdout, stderr);

    if !stdout.trim().is_empty() {
        print!("{}", stdout);
    }
    if !stderr.trim().is_empty() {
        eprint!("{}", stderr);
    }

    if let Some(path) = log_file {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let ts = Utc::now().format("%Y-%m-%d %H:%M:%S");
            let _ = writeln!(file, "[{}] [{}]\n{}\n", ts, step.name, combined.trim());
        }
    }

    Ok((output.status.success(), combined))
}

// suppress unused import warning
fn _use_hashset(_: HashSet<String>) {}