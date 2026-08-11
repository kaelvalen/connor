use crate::config::{Config, Step};
use crate::env::{interpolate, parse_env_file, redact};
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
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

pub struct Runner {
    pub config: Config,
    pub from_step: Option<String>,
    pub dry_run: bool,
    pub log_file: Option<String>,
    env_file: HashMap<String, String>,
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
        let env_file = load_env_file(&config);
        Self {
            config,
            from_step,
            dry_run,
            log_file,
            env_file,
        }
    }

    pub async fn run(&self) -> Result<RunResult> {
        let mission = &self.config.mission;
        let target = mission.target.clone().unwrap_or_else(|| "unknown".into());
        let retry_limit = mission.retry_limit.unwrap_or(3);
        let stop_on_failure = mission.stop_on_failure.unwrap_or(true);
        let mission_timeout = mission.timeout_secs;

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
                let env_file = self.env_file.clone();
                let base_dir = self.config.base_dir.clone();
                let idx = step_index;
                let total = total_steps;
                let is_solo = stage.len() == 1;
                let emit_output = is_solo;
                let timeout_secs = step.timeout_secs.or(mission_timeout);

                if is_solo {
                    logger::step_start(&step.name, idx, total);
                }

                let handle = tokio::spawn(async move {
                    let attempts_limit = max_attempts(retry_limit, step.retry);
                    let (lookup, secrets) = resolve_step_env(&step, &env_file);

                    // Resolve relative cwd against the config directory so the
                    // behavior is identical no matter where `connor` is invoked
                    // from (config is found by walking up).
                    let mut step_eff = step.clone();
                    if let Some(resolved) =
                        resolve_cwd(step.cwd.as_deref(), base_dir.as_deref(), &lookup)
                    {
                        step_eff.cwd = Some(resolved);
                    }

                    let (success, elapsed, attempts, output, timed_out) = execute_step(
                        &step_eff,
                        attempts_limit,
                        log_file.as_deref(),
                        emit_output,
                        timeout_secs,
                        &secrets,
                        &lookup,
                    )
                    .await;
                    // keep history bounded even when steps are chatty
                    let output = output.map(|o| truncate_output(&o, 4000));
                    let output_for_summary = output.clone();

                    if is_solo {
                        if timed_out {
                            logger::warn(&format!(
                                "Step '{}' exceeded its time limit and was terminated",
                                step.name
                            ));
                        }
                        if success {
                            logger::step_success(&step.name, elapsed);
                        } else {
                            logger::step_failure(&step.name, None);
                        }
                    }

                    records_clone.lock().await.push(StepRecord {
                        name: step.name.clone(),
                        status: if success {
                            StepStatus::Success
                        } else {
                            StepStatus::Failed
                        },
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

        println!(
            "{}",
            "DRY RUN — No commands will be executed\n".yellow().bold()
        );

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
                if let Some(cwd) = &step.cwd {
                    println!("  {} {}", "cwd:".dimmed(), cwd.yellow());
                }
                if let Some(timeout) = step.timeout_secs {
                    println!(
                        "  {} {}s",
                        "timeout:".dimmed(),
                        timeout.to_string().yellow()
                    );
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

        Ok(RunResult {
            success: true,
            record,
        })
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
                let dep_idx = *by_name.get(dep.as_str()).ok_or_else(|| {
                    anyhow!("Step '{}' depends on unknown step '{}'", step.name, dep)
                })?;
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

/// Resolve a step's `cwd` against the config directory (relative paths) or
/// leave absolute paths untouched. `None` when the step has no cwd.
fn resolve_cwd(
    cwd: Option<&str>,
    base_dir: Option<&std::path::Path>,
    lookup: &HashMap<String, String>,
) -> Option<String> {
    let cwd = cwd?;
    let p = std::path::PathBuf::from(interpolate(cwd, lookup));
    let resolved = if p.is_absolute() {
        p
    } else if let Some(base) = base_dir {
        base.join(p)
    } else {
        p
    };
    Some(resolved.to_string_lossy().into_owned())
}

fn load_env_file(config: &Config) -> HashMap<String, String> {
    let Some(base) = config.base_dir.as_deref() else {
        return HashMap::new();
    };
    let path = match &config.mission.env_file {
        Some(name) => {
            let p = base.join(name);
            if !p.is_file() {
                logger::warn(&format!("Configured env_file not found: {}", p.display()));
            }
            p
        }
        None => base.join(".env"),
    };
    match std::fs::read_to_string(&path) {
        Ok(content) => parse_env_file(&content).into_iter().collect(),
        Err(_) => HashMap::new(),
    }
}

/// Build the interpolation lookup table (process env, then env_file, then the
/// step's own env, which wins) and the list of secret values to redact from
/// output (env_file and step env values of at least 4 chars).
fn resolve_step_env(
    step: &Step,
    env_file: &HashMap<String, String>,
) -> (HashMap<String, String>, Vec<String>) {
    let mut base: HashMap<String, String> = std::env::vars().collect();
    base.extend(env_file.iter().map(|(k, v)| (k.clone(), v.clone())));

    let mut secrets: Vec<String> = Vec::new();
    for value in env_file.values() {
        if value.len() >= 4 {
            secrets.push(value.clone());
        }
    }

    if let Some(env) = &step.env {
        let mut resolved: HashMap<String, String> = HashMap::new();
        for (key, value) in env {
            let value = interpolate(value, &base);
            if value.len() >= 4 {
                secrets.push(value.clone());
            }
            resolved.insert(key.clone(), value);
        }
        base.extend(resolved);
    }

    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    secrets.dedup();
    (base, secrets)
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

#[allow(clippy::too_many_arguments)]
async fn execute_step(
    step: &Step,
    max_attempts: u32,
    log_file: Option<&str>,
    emit_output: bool,
    timeout_secs: Option<u64>,
    secrets: &[String],
    lookup: &HashMap<String, String>,
) -> (bool, u128, u32, Option<String>, bool) {
    let mut attempts = 0u32;
    let start = Instant::now();

    loop {
        attempts += 1;
        let (ok, output, timed_out) =
            match spawn_step(step, log_file, emit_output, timeout_secs, secrets, lookup).await {
                Ok((ok, output, timed_out)) => (ok, output, timed_out),
                Err(e) => (false, e.to_string(), false),
            };

        if ok {
            return (
                true,
                start.elapsed().as_millis(),
                attempts,
                Some(output),
                false,
            );
        }
        if attempts < max_attempts {
            logger::step_retry(&step.name, attempts, max_attempts);
            let sleep_ms = retry_backoff_ms(attempts);
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
        } else {
            return (
                false,
                start.elapsed().as_millis(),
                attempts,
                Some(output),
                timed_out,
            );
        }
    }
}

/// Spawn a step, streaming output live (when `emit_output`), capturing it for
/// history, and enforcing an optional per-attempt timeout. On Unix the child
/// runs in its own process group so a timeout can kill the whole tree
/// (shell + grandchildren like docker/cargo). Secrets are redacted from both
/// live output and the captured buffer.
#[allow(clippy::too_many_arguments)]
async fn spawn_step(
    step: &Step,
    log_file: Option<&str>,
    emit_output: bool,
    timeout_secs: Option<u64>,
    secrets: &[String],
    lookup: &HashMap<String, String>,
) -> Result<(bool, String, bool)> {
    let cmd_str = interpolate(&step.cmd, lookup);
    let cwd = step.cwd.as_deref().map(|c| interpolate(c, lookup));

    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", &cmd_str]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", &cmd_str]);
        c
    };

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    if let Some(dir) = &cwd {
        cmd.current_dir(dir);
    }

    // Own process group so the whole tree can be killed on timeout.
    #[cfg(unix)]
    cmd.process_group(0);

    if let Some(ref env_vars) = step.env {
        for (k, v) in env_vars {
            cmd.env(k, interpolate(v, lookup));
        }
    }

    let mut child = cmd.spawn()?;
    #[cfg(unix)]
    let child_pid = child.id().expect("spawned child has a pid") as i32;

    let (tx, mut rx) = mpsc::channel::<(u8, String)>(64);

    let mut readers = Vec::new();
    if let Some(stream) = child.stdout.take() {
        let tx = tx.clone();
        readers.push(tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if tx.send((0u8, line.clone())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }
    if let Some(stream) = child.stderr.take() {
        let tx = tx.clone();
        readers.push(tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if tx.send((1u8, line.clone())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }));
    }
    drop(tx);

    // Single consumer serializes writes (no interleaved lines) and redacts
    // secrets before anything reaches the terminal or the capture buffer.
    let secrets = secrets.to_vec();
    let printer = tokio::spawn(async move {
        let mut buf: Vec<String> = Vec::new();
        while let Some((kind, line)) = rx.recv().await {
            let (line, _) = redact(&line, &secrets);
            if emit_output {
                if kind == 0 {
                    print!("{}", line);
                    let _ = std::io::stdout().flush();
                } else {
                    eprint!("{}", line);
                    let _ = std::io::stderr().flush();
                }
            }
            buf.push(line);
        }
        buf
    });

    let (status, timed_out) = match timeout_secs {
        Some(secs) => {
            let wait = child.wait();
            match tokio::time::timeout(Duration::from_secs(secs), wait).await {
                Ok(result) => (result?, false),
                Err(_) => {
                    #[cfg(unix)]
                    unsafe {
                        libc::kill(-child_pid, libc::SIGKILL);
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = child.start_kill();
                    }
                    let _ = child.wait().await;
                    // Placeholder status; `timed_out` forces failure below.
                    (std::process::ExitStatus::default(), true)
                }
            }
        }
        None => (child.wait().await?, false),
    };

    for reader in readers {
        let _ = reader.await;
    }
    let mut combined = printer.await.unwrap_or_default().join("");
    if timed_out {
        combined.push_str(&format!(
            "\n[connor] timed out after {}s and was terminated",
            timeout_secs.unwrap_or(0)
        ));
    }

    if let Some(path) = log_file {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let ts = Utc::now().format("%Y-%m-%d %H:%M:%S");
            let _ = writeln!(file, "[{}] [{}]\n{}\n", ts, step.name, combined.trim());
        }
    }

    Ok((status.success() && !timed_out, combined, timed_out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, Mission, Step};

    fn mk_config(steps: Vec<Step>) -> Config {
        Config {
            mission: Mission {
                name: "m".into(),
                target: None,
                retry_limit: Some(3),
                stop_on_failure: Some(true),
                env_file: None,
                timeout_secs: None,
            },
            steps,
            base_dir: None,
        }
    }

    fn mk_step(name: &str) -> Step {
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

    fn empty_lookup() -> HashMap<String, String> {
        HashMap::new()
    }

    #[test]
    fn stages_parallel_then_dependent() {
        let cfg = mk_config(vec![
            Step {
                name: "lint".into(),
                cmd: "x".into(),
                depends_on: None,
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
            Step {
                name: "test".into(),
                cmd: "x".into(),
                depends_on: None,
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
            Step {
                name: "build".into(),
                cmd: "x".into(),
                depends_on: Some(vec!["lint".into(), "test".into()]),
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
        ]);
        let r = Runner::new(cfg, None, true, None);
        let stages = r.build_stages().unwrap();
        assert_eq!(stages.len(), 2);
        assert_eq!(
            stages[0]
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["lint", "test"]
        );
        assert_eq!(
            stages[1]
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["build"]
        );
    }

    #[test]
    fn build_stages_errors_on_unknown_dep() {
        let cfg = mk_config(vec![Step {
            name: "a".into(),
            cmd: "x".into(),
            depends_on: Some(vec!["missing".into()]),
            retry: None,
            env: None,
            cwd: None,
            timeout_secs: None,
        }]);
        let r = Runner::new(cfg, None, true, None);
        let err = r.build_stages().unwrap_err().to_string();
        assert!(err.contains("depends on unknown step"));
    }

    #[test]
    fn build_stages_errors_on_cycle() {
        let cfg = mk_config(vec![
            Step {
                name: "a".into(),
                cmd: "x".into(),
                depends_on: Some(vec!["b".into()]),
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
            Step {
                name: "b".into(),
                cmd: "x".into(),
                depends_on: Some(vec!["a".into()]),
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
        ]);
        let r = Runner::new(cfg, None, true, None);
        let err = r.build_stages().unwrap_err().to_string();
        assert!(err.contains("Dependency cycle detected"));
    }

    #[test]
    fn build_stages_errors_on_duplicate_names() {
        let cfg = mk_config(vec![
            Step {
                name: "a".into(),
                cmd: "x".into(),
                depends_on: None,
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
            Step {
                name: "a".into(),
                cmd: "x".into(),
                depends_on: None,
                retry: None,
                env: None,
                cwd: None,
                timeout_secs: None,
            },
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

    #[test]
    fn max_attempts_is_one_for_no_retries() {
        let step = mk_step("a");
        let (ok, _elapsed, attempts, _out, timed_out) = tokio_test_block_on(execute_step(
            &step,
            1,
            None,
            false,
            None,
            &[],
            &empty_lookup(),
        ));
        assert!(ok);
        assert_eq!(attempts, 1);
        assert!(!timed_out);
    }

    #[test]
    fn timeout_kills_step_and_marks_timed_out() {
        let step = Step {
            name: "slow".into(),
            cmd: "sleep 30".into(),
            depends_on: None,
            retry: None,
            env: None,
            cwd: None,
            timeout_secs: Some(1),
        };
        let (ok, elapsed, attempts, output, timed_out) = tokio_test_block_on(execute_step(
            &step,
            1,
            None,
            false,
            Some(1),
            &[],
            &empty_lookup(),
        ));
        assert!(!ok);
        assert!(timed_out);
        assert_eq!(attempts, 1);
        // Generous bound: the point is that it was killed at ~1s, not that it
        // survived the full 30s. The bound is loose so CPU starvation in
        // parallel test runs can't make this flaky.
        assert!(elapsed < 10_000, "killed at ~1s, took {elapsed}ms");
        assert!(
            output
                .as_deref()
                .unwrap_or("")
                .contains("timed out after 1s"),
            "output should mention the timeout: {:?}",
            output
        );
    }

    #[test]
    fn timeout_applies_per_attempt() {
        let step = Step {
            name: "flaky".into(),
            cmd: "sleep 5".into(),
            depends_on: None,
            retry: Some(1),
            env: None,
            cwd: None,
            timeout_secs: Some(1),
        };
        let (_ok, _elapsed, attempts, _out, _timed_out) = tokio_test_block_on(execute_step(
            &step,
            2,
            None,
            false,
            Some(1),
            &[],
            &empty_lookup(),
        ));
        assert_eq!(attempts, 2);
    }

    #[test]
    fn interpolates_env_file_vars_into_cmd() {
        let step = Step {
            name: "echo".into(),
            cmd: "printf '%s' ${FROM_FILE}".into(),
            depends_on: None,
            retry: None,
            env: None,
            cwd: None,
            timeout_secs: None,
        };
        let lookup = HashMap::from([("FROM_FILE".to_string(), "interpolated".to_string())]);
        let (ok, output, _timed_out) =
            tokio_test_block_on(spawn_step(&step, None, false, None, &[], &lookup)).unwrap();
        assert!(ok);
        assert_eq!(output, "interpolated");
    }

    #[test]
    fn step_env_wins_over_lookup() {
        let step = Step {
            name: "echo".into(),
            cmd: "printf '%s' ${FOO}".into(),
            depends_on: None,
            retry: None,
            env: Some(HashMap::from([("FOO".to_string(), "step".to_string())])),
            cwd: None,
            timeout_secs: None,
        };
        // In production the lookup is pre-merged by resolve_step_env, so the
        // step's own value wins; here we pass that merged map directly.
        let lookup = HashMap::from([("FOO".to_string(), "step".to_string())]);
        let (ok, output, _timed_out) =
            tokio_test_block_on(spawn_step(&step, None, false, None, &[], &lookup)).unwrap();
        assert!(ok);
        assert_eq!(output, "step");
    }

    #[test]
    fn cwd_is_applied_to_command() {
        let dir = std::env::temp_dir().join(format!("connor-cwd-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let step = Step {
            name: "pwd".into(),
            cmd: "pwd".into(),
            depends_on: None,
            retry: None,
            env: None,
            cwd: Some(dir.to_string_lossy().into_owned()),
            timeout_secs: None,
        };
        let (ok, output, _timed_out) =
            tokio_test_block_on(spawn_step(&step, None, false, None, &[], &empty_lookup()))
                .unwrap();
        assert!(ok);
        let expected = dir.canonicalize().unwrap();
        assert!(
            output.contains(expected.to_str().unwrap()),
            "output should contain cwd: {:?}",
            output
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn secrets_are_redacted_from_captured_output() {
        let step = Step {
            name: "leak".into(),
            cmd: "echo value=supersecret123".into(),
            depends_on: None,
            retry: None,
            env: None,
            cwd: None,
            timeout_secs: None,
        };
        let secrets = vec!["supersecret123".to_string()];
        let (ok, output, _timed_out) = tokio_test_block_on(spawn_step(
            &step,
            None,
            false,
            None,
            &secrets,
            &empty_lookup(),
        ))
        .unwrap();
        assert!(ok);
        assert!(
            output.contains("***"),
            "output should be redacted: {:?}",
            output
        );
        assert!(!output.contains("supersecret123"));
    }

    #[test]
    fn resolve_cwd_joins_relative_path_onto_base_dir() {
        let lookup = empty_lookup();
        let base = std::path::Path::new("/project");
        assert_eq!(
            resolve_cwd(Some("backend"), Some(base), &lookup).as_deref(),
            Some("/project/backend")
        );
        assert_eq!(
            resolve_cwd(Some("/abs/path"), Some(base), &lookup).as_deref(),
            Some("/abs/path")
        );
        assert_eq!(
            resolve_cwd(
                Some("${DIR}"),
                Some(base),
                &HashMap::from([("DIR".to_string(), "sub".to_string())])
            )
            .as_deref(),
            Some("/project/sub")
        );
        assert_eq!(resolve_cwd(None, Some(base), &lookup), None);
    }

    #[test]
    fn resolve_step_env_collects_secrets_from_env_file_and_step() {
        let step = Step {
            name: "s".into(),
            cmd: "x".into(),
            depends_on: None,
            retry: None,
            env: Some(HashMap::from([
                ("TOKEN".to_string(), "${FILE_TOKEN}".to_string()),
                ("SHORT".to_string(), "ab".to_string()),
            ])),
            cwd: None,
            timeout_secs: None,
        };
        let file = HashMap::from([
            ("FILE_TOKEN".to_string(), "file-token-value".to_string()),
            ("SHORT_FILE".to_string(), "cd".to_string()),
        ]);
        let (lookup, secrets) = resolve_step_env(&step, &file);
        assert_eq!(
            lookup.get("TOKEN").map(String::as_str),
            Some("file-token-value")
        );
        assert_eq!(lookup.get("SHORT").map(String::as_str), Some("ab"));
        assert!(secrets.contains(&"file-token-value".to_string()));
        assert!(!secrets.contains(&"cd".to_string()));
        assert!(!secrets.contains(&"ab".to_string()));
    }

    #[cfg(test)]
    fn tokio_test_block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }
}
