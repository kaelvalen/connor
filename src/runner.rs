use crate::config::{Config, Step};
use crate::history::{RunRecord, RunStatus, StepRecord, StepStatus};
use crate::logger;
use anyhow::{anyhow, Result};
use chrono::Utc;
use colored::Colorize;
use std::collections::{BTreeSet, HashMap};
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
                let is_solo = stage.len() == 1;
                let emit_output = is_solo;

                if is_solo {
                    logger::step_start(&step.name, idx, total);
                }

                let handle = tokio::spawn(async move {
                    let attempts_limit = max_attempts(retry_limit, step.retry);

                    let (success, elapsed, attempts, output) =
                        execute_step(&step, attempts_limit, log_file.as_deref(), emit_output).await;
                    let output_for_summary = output.clone();

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

                    (step.name.clone(), success, elapsed, output_for_summary)
                });

                handles.push(handle);
            }

            let mut stage_failed: Option<String> = None;
            for handle in handles {
                let (name, success, elapsed, output) = handle.await?;
                if !success && stage_failed.is_none() {
                    stage_failed = Some(name.clone());
                }
                if stage.len() > 1 {
                    if success {
                        logger::step_success(&name, elapsed);
                    } else {
                        logger::step_failure(&name, None);
                        if let Some(out) = output {
                            let out = truncate_output(&out, 2000);
                            logger::step_output(&name, &out);
                        }
                    }
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
        if steps.is_empty() {
            return Ok(vec![]);
        }

        // Validate and index steps by name.
        let mut by_name: HashMap<&str, usize> = HashMap::with_capacity(steps.len());
        for (idx, step) in steps.iter().enumerate() {
            if by_name.insert(step.name.as_str(), idx).is_some() {
                return Err(anyhow!("Duplicate step name '{}'", step.name));
            }
        }

        if let Some(ref from) = self.from_step {
            if !by_name.contains_key(from.as_str()) {
                return Err(anyhow!(
                    "Unknown step '{}' for --from/connor retry target",
                    from
                ));
            }
        }

        // Build adjacency and indegree.
        let mut indegree = vec![0usize; steps.len()];
        let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); steps.len()];
        let mut deps: Vec<Vec<usize>> = vec![Vec::new(); steps.len()];

        for (idx, step) in steps.iter().enumerate() {
            let dep_names = step.depends_on.as_deref().unwrap_or(&[]);
            for dep in dep_names {
                let dep_idx = *by_name
                    .get(dep.as_str())
                    .ok_or_else(|| anyhow!("Step '{}' depends on unknown step '{}'", step.name, dep))?;
                if dep_idx == idx {
                    return Err(anyhow!("Step '{}' cannot depend on itself", step.name));
                }
                indegree[idx] += 1;
                outgoing[dep_idx].push(idx);
                deps[idx].push(dep_idx);
            }
        }

        // Kahn topo sort, deterministically preferring original order.
        let mut ready: BTreeSet<usize> = indegree
            .iter()
            .enumerate()
            .filter_map(|(i, d)| (*d == 0).then_some(i))
            .collect();

        let mut topo: Vec<usize> = Vec::with_capacity(steps.len());
        let mut in_topo = vec![false; steps.len()];
        while let Some(&i) = ready.iter().next() {
            ready.remove(&i);
            topo.push(i);
            in_topo[i] = true;
            for &to in &outgoing[i] {
                indegree[to] -= 1;
                if indegree[to] == 0 {
                    ready.insert(to);
                }
            }
        }

        if topo.len() != steps.len() {
            // Remaining nodes are part of (or blocked by) a cycle.
            let mut remaining: Vec<&str> = (0..steps.len())
                .filter(|i| !in_topo[*i])
                .map(|i| steps[i].name.as_str())
                .collect();
            remaining.sort_unstable();
            return Err(anyhow!(
                "Dependency cycle detected involving: {}",
                remaining.join(", ")
            ));
        }

        // Stage = longest path depth (max(dep_stage)+1). This yields maximal parallelism.
        let mut stage_of = vec![0usize; steps.len()];
        for &i in &topo {
            let mut s = 0usize;
            for &d in &deps[i] {
                s = s.max(stage_of[d] + 1);
            }
            stage_of[i] = s;
        }

        let max_stage = *stage_of.iter().max().unwrap_or(&0);
        let mut stages: Vec<Vec<Step>> = vec![Vec::new(); max_stage + 1];
        for (idx, step) in steps.iter().enumerate() {
            stages[stage_of[idx]].push(step.clone());
        }

        Ok(stages)
    }
}

fn max_attempts(global_retry_limit: u32, step_extra_retry: Option<u32>) -> u32 {
    // retry_limit is "max retries per step" (i.e. additional tries after the first).
    // step.retry adds more retries for that step.
    1 + global_retry_limit + step_extra_retry.unwrap_or(0)
}

fn retry_backoff_ms(attempt: u32) -> u64 {
    // attempt is 1-based; backoff applies after a failure, before attempt+1.
    // 500ms, 1s, 2s, 4s... capped at 10s with a small deterministic jitter.
    let pow = attempt.saturating_sub(1).min(10);
    let base = 500u64.saturating_mul(1u64 << pow);
    let capped = base.min(10_000);
    let jitter = (Utc::now().timestamp_subsec_millis() as u64) % 250;
    capped + jitter
}

fn truncate_output(s: &str, max_chars: usize) -> String {
    if s.len() <= max_chars {
        return s.to_string();
    }
    let mut out = s.chars().take(max_chars).collect::<String>();
    out.push_str("\n… (truncated)");
    out
}

async fn execute_step(
    step: &Step,
    max_attempts: u32,
    log_file: Option<&str>,
    emit_output: bool,
) -> (bool, u128, u32, Option<String>) {
    let mut attempts = 0u32;
    let start = Instant::now();

    loop {
        attempts += 1;
        let result = spawn_step(step, log_file, emit_output).await;

        match result {
            Ok((true, output)) => {
                return (true, start.elapsed().as_millis(), attempts, Some(output));
            }
            Ok((false, output)) => {
                if attempts < max_attempts {
                    logger::step_retry(&step.name, attempts, max_attempts);
                    let sleep_ms = retry_backoff_ms(attempts);
                    tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                } else {
                    return (false, start.elapsed().as_millis(), attempts, Some(output));
                }
            }
            Err(e) => {
                if attempts < max_attempts {
                    logger::step_retry(&step.name, attempts, max_attempts);
                    let sleep_ms = retry_backoff_ms(attempts);
                    tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                } else {
                    return (
                        false,
                        start.elapsed().as_millis(),
                        attempts,
                        Some(e.to_string()),
                    );
                }
            }
        }
    }
}

async fn spawn_step(step: &Step, log_file: Option<&str>, emit_output: bool) -> Result<(bool, String)> {
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

    if emit_output {
        if !stdout.trim().is_empty() {
            print!("{}", stdout);
        }
        if !stderr.trim().is_empty() {
            eprint!("{}", stderr);
        }
    }

    if let Some(path) = log_file {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let ts = Utc::now().format("%Y-%m-%d %H:%M:%S");
            let _ = writeln!(file, "[{}] [{}]\n{}\n", ts, step.name, combined.trim());
        }
    }

    Ok((output.status.success(), combined))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Mission, Step};

    fn mk_config(steps: Vec<Step>) -> Config {
        Config {
            mission: Mission { name: "m".into(), target: None, retry_limit: Some(3), stop_on_failure: Some(true) },
            steps,
        }
    }

    #[test]
    fn stages_parallel_then_dependent() {
        let cfg = mk_config(vec![
            Step { name: "lint".into(), cmd: "x".into(), depends_on: None, retry: None, env: None },
            Step { name: "test".into(), cmd: "x".into(), depends_on: None, retry: None, env: None },
            Step {
                name: "build".into(),
                cmd: "x".into(),
                depends_on: Some(vec!["lint".into(), "test".into()]),
                retry: None,
                env: None,
            },
        ]);
        let r = Runner::new(cfg, None, true, None);
        let stages = r.build_stages().unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), vec!["lint", "test"]);
        assert_eq!(stages[1].iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), vec!["build"]);
    }

    #[test]
    fn build_stages_errors_on_unknown_dep() {
        let cfg = mk_config(vec![Step {
            name: "a".into(),
            cmd: "x".into(),
            depends_on: Some(vec!["missing".into()]),
            retry: None,
            env: None,
        }]);
        let r = Runner::new(cfg, None, true, None);
        let err = r.build_stages().unwrap_err().to_string();
        assert!(err.contains("depends on unknown step"));
    }

    #[test]
    fn build_stages_errors_on_cycle() {
        let cfg = mk_config(vec![
            Step { name: "a".into(), cmd: "x".into(), depends_on: Some(vec!["b".into()]), retry: None, env: None },
            Step { name: "b".into(), cmd: "x".into(), depends_on: Some(vec!["a".into()]), retry: None, env: None },
        ]);
        let r = Runner::new(cfg, None, true, None);
        let err = r.build_stages().unwrap_err().to_string();
        assert!(err.contains("Dependency cycle detected"));
    }

    #[test]
    fn build_stages_errors_on_duplicate_names() {
        let cfg = mk_config(vec![
            Step { name: "a".into(), cmd: "x".into(), depends_on: None, retry: None, env: None },
            Step { name: "a".into(), cmd: "x".into(), depends_on: None, retry: None, env: None },
        ]);
        let r = Runner::new(cfg, None, true, None);
        let err = r.build_stages().unwrap_err().to_string();
        assert!(err.contains("Duplicate step name"));
    }

    #[test]
    fn retry_attempts_math() {
        assert_eq!(max_attempts(3, None), 4);
        assert_eq!(max_attempts(3, Some(2)), 6);
        assert_eq!(max_attempts(0, Some(0)), 1);
    }
}
