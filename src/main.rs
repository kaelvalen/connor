mod config;
mod history;
mod logger;
mod runner;
mod webhook;

use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use config::Config;
use history::{load_history, save_run, RunStatus, StepStatus};
use runner::Runner;

#[derive(Parser)]
#[command(name = "connor")]
#[command(about = "Resistance CI/CD — I'll be back.")]
#[command(version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to mission config
    #[arg(short, long)]
    config: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute the mission pipeline
    Run {
        /// Start from a specific step
        #[arg(long)]
        from: Option<String>,

        /// Preview steps without executing
        #[arg(long)]
        dry_run: bool,

        /// Write step output to log file
        #[arg(long)]
        log: Option<String>,

        /// Webhook URL to notify on completion
        #[arg(long)]
        webhook: Option<String>,
    },
    /// Retry from last failed step
    Retry {
        /// Webhook URL to notify on completion
        #[arg(long)]
        webhook: Option<String>,
    },
    /// Show pipeline status from last run
    Status,
    /// Show run history
    History {
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// Initialize a new connor.toml
    Init,
    /// Watch for file changes and re-run pipeline
    Watch {
        /// Glob pattern to watch (default: src/**)
        #[arg(short, long, default_value = ".")]
        path: String,

        /// Debounce delay in ms
        #[arg(short, long, default_value = "500")]
        debounce: u64,

        /// Webhook URL to notify on each run
        #[arg(long)]
        webhook: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.clone().unwrap_or_else(|| config::find_config().to_string());

    match cli.command {
        Commands::Init => cmd_init(),
        Commands::Run { from, dry_run, log, webhook } => {
            cmd_run(&config_path, from, dry_run, log, webhook).await
        }
        Commands::Retry { webhook } => cmd_retry(&config_path, webhook).await,
        Commands::Status => cmd_status(),
        Commands::History { limit } => cmd_history(limit),
        Commands::Watch { path, debounce, webhook } => {
            cmd_watch(&config_path, &path, debounce, webhook).await
        }
    }
}

fn cmd_init() -> Result<()> {
    let path = "connor.toml";
    if std::path::Path::new(path).exists() {
        println!("{}", "Mission briefing already exists: connor.toml".yellow());
        return Ok(());
    }
    std::fs::write(path, Config::default_toml())?;
    println!("{}", "✓ Mission briefing created: connor.toml".green().bold());
    println!("{}", "  Edit it and run `connor run` to begin.".dimmed());
    Ok(())
}

async fn cmd_run(
    config_path: &str,
    from: Option<String>,
    dry_run: bool,
    log: Option<String>,
    webhook: Option<String>,
) -> Result<()> {
    logger::banner();

    let config = Config::load(config_path)?;
    let runner = Runner::new(config, from, dry_run, log);
    let result = runner.run().await?;

    if !dry_run {
        save_run(result.record.clone())?;

        if let Some(url) = webhook {
            logger::info(&format!("Notifying webhook: {}", url));
            if let Err(e) = webhook::notify(&url, &result.record).await {
                logger::warn(&format!("Webhook failed: {}", e));
            }
        }
    }

    if !result.success {
        std::process::exit(1);
    }

    Ok(())
}

async fn cmd_retry(config_path: &str, webhook: Option<String>) -> Result<()> {
    let failed_step = history::last_failed_step();

    match failed_step {
        None => {
            println!("{}", "No failed missions in history. Resistance is holding.".green());
        }
        Some(step) => {
            println!(
                "{} Retrying from step: {}",
                "↺ SENDING RESISTANCE —".yellow().bold(),
                step.cyan().bold()
            );
            cmd_run(config_path, Some(step), false, None, webhook).await?;
        }
    }

    Ok(())
}

fn cmd_status() -> Result<()> {
    let history = load_history();

    match history.first() {
        None => {
            println!("{}", "No mission history found. Run `connor run` first.".dimmed());
        }
        Some(run) => {
            println!("\n{}", "━━━ LAST MISSION STATUS ━━━".cyan().bold());
            println!("{} {}", "Mission:".dimmed(), run.mission.white().bold());
            println!(
                "{} {}",
                "Time:   ".dimmed(),
                run.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string().dimmed()
            );
            println!("{} {}ms", "Elapsed:".dimmed(), run.elapsed_ms);

            match &run.status {
                RunStatus::Success => println!(
                    "{} {}",
                    "Status: ".dimmed(),
                    "✓ JUDGMENT DAY AVERTED".green().bold()
                ),
                RunStatus::Failed(step) => println!(
                    "{} {} {}",
                    "Status: ".dimmed(),
                    "✗ SKYNET WINS —".red().bold(),
                    format!("failed at '{}'", step).white()
                ),
            }

            println!("\n{}", "Steps:".dimmed());
            for step in &run.steps {
                let icon = match step.status {
                    StepStatus::Success => "✓".green().bold(),
                    StepStatus::Failed => "✗".red().bold(),
                    StepStatus::Skipped => "○".dimmed().bold(),
                };
                let attempts_str = if step.attempts > 1 {
                    format!(" ({} attempts)", step.attempts)
                } else {
                    String::new()
                };
                println!(
                    "  {} {} {}{}",
                    icon,
                    step.name.white(),
                    format!("{}ms", step.elapsed_ms).dimmed(),
                    attempts_str.yellow()
                );
            }
            println!();
        }
    }

    Ok(())
}

fn cmd_history(limit: usize) -> Result<()> {
    let history = load_history();

    if history.is_empty() {
        println!("{}", "No missions in the logs.".dimmed());
        return Ok(());
    }

    println!("\n{}", "━━━ MISSION HISTORY ━━━".cyan().bold());

    for (i, run) in history.iter().take(limit).enumerate() {
        let status_str = match &run.status {
            RunStatus::Success => "✓ AVERTED".green().bold(),
            RunStatus::Failed(_) => "✗ SKYNET ".red().bold(),
        };

        let steps_ok = run.steps.iter().filter(|s| s.status == StepStatus::Success).count();
        let steps_total = run
            .steps
            .iter()
            .filter(|s| s.status != StepStatus::Skipped)
            .count();

        println!(
            "  {} {} {} {} {}/{}",
            format!("[{}]", i + 1).dimmed(),
            run.timestamp.format("%m-%d %H:%M").to_string().dimmed(),
            status_str,
            run.mission.white(),
            steps_ok,
            steps_total
        );
    }

    println!();
    Ok(())
}

async fn cmd_watch(
    config_path: &str,
    watch_path: &str,
    debounce_ms: u64,
    webhook: Option<String>,
) -> Result<()> {
    use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    println!(
        "{} Watching {} for changes. Press Ctrl+C to abort.",
        "◈ RESISTANCE WATCH MODE".cyan().bold(),
        watch_path.white().bold()
    );
    println!("{}", "  Any change will trigger a mission run.\n".dimmed());

    let (tx, rx) = mpsc::channel::<notify::Result<Event>>();

    let mut watcher = RecommendedWatcher::new(tx, NotifyConfig::default())?;
    watcher.watch(std::path::Path::new(watch_path), RecursiveMode::Recursive)?;

    // Run once immediately
    cmd_run(config_path, None, false, None, webhook.clone()).await.ok();

    let debounce = Duration::from_millis(debounce_ms);
    let mut last_run = Instant::now();

    loop {
        match rx.recv() {
            Ok(Ok(event)) => {
                // filter to meaningful events only
                use notify::EventKind::*;
                let relevant = matches!(
                    event.kind,
                    Create(_) | Modify(_) | Remove(_)
                );

                if !relevant {
                    continue;
                }

                // skip .connor_history.json and log files to avoid infinite loops
                let is_internal = event.paths.iter().any(|p| {
                    p.to_string_lossy().contains(".connor_history")
                        || p.extension().map(|e| e == "log").unwrap_or(false)
                });

                if is_internal {
                    continue;
                }

                if last_run.elapsed() < debounce {
                    continue;
                }

                last_run = Instant::now();

                if let Some(path) = event.paths.first() {
                    logger::watch_trigger(&path.to_string_lossy());
                }

                cmd_run(config_path, None, false, None, webhook.clone())
                    .await
                    .ok();
            }
            Ok(Err(e)) => {
                logger::warn(&format!("Watch error: {}", e));
            }
            Err(_) => break,
        }
    }

    Ok(())
}