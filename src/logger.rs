use colored::Colorize;

pub fn banner() {
    println!("{}", r#"
   ██████╗ ██████╗ ███╗   ██╗███╗   ██╗ ██████╗ ██████╗ 
  ██╔════╝██╔═══██╗████╗  ██║████╗  ██║██╔═══██╗██╔══██╗
  ██║     ██║   ██║██╔██╗ ██║██╔██╗ ██║██║   ██║██████╔╝
  ██║     ██║   ██║██║╚██╗██║██║╚██╗██║██║   ██║██╔══██╗
  ╚██████╗╚██████╔╝██║ ╚████║██║ ╚████║╚██████╔╝██║  ██║
   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝  ╚═╝
"#.red().bold());
    println!("  {} {}\n", "RESISTANCE CI/CD".cyan().bold(), "— I'll be back.".dimmed());
}

pub fn mission_start(name: &str, target: &str) {
    println!("{} {}", "◈ MISSION:".yellow().bold(), name.white().bold());
    println!("{} {}\n", "◈ TARGET: ".yellow().bold(), target.cyan());
}

pub fn step_start(step: &str, index: usize, total: usize) {
    println!(
        "{} [{}/{}] {}",
        "▶ DEPLOYING".blue().bold(),
        index,
        total,
        step.white().bold()
    );
}

pub fn step_success(step: &str, elapsed_ms: u128) {
    println!(
        "{} {} {}",
        "✓ SECURED".green().bold(),
        step.white(),
        format!("({}ms)", elapsed_ms).dimmed()
    );
}

pub fn step_failure(step: &str, code: Option<i32>) {
    println!(
        "{} {} {}",
        "✗ SKYNET ATTACK DETECTED".red().bold(),
        format!("on step: {}", step).white(),
        code.map(|c| format!("[exit {}]", c))
            .unwrap_or_default()
            .dimmed()
    );
}

pub fn step_retry(step: &str, attempt: u32, max: u32) {
    println!(
        "{} {} {}/{}...",
        "↺ SENDING RESISTANCE".yellow().bold(),
        step.white(),
        attempt,
        max
    );
}

pub fn step_output(step: &str, output: &str) {
    if output.trim().is_empty() {
        return;
    }

    println!("{}", format!("  ── output: {} ──", step).dimmed());
    for line in output.lines() {
        println!("{}", format!("  {}", line).dimmed());
    }
    println!("{}", "  ─────────────────────".dimmed());
}

pub fn pipeline_success(elapsed_ms: u128) {
    println!("\n{}", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━".green());
    println!(
        "  {} {}",
        "✓ JUDGMENT DAY AVERTED.".green().bold(),
        format!("Total: {}ms", elapsed_ms).dimmed()
    );
    println!("{}", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n".green());
}

pub fn pipeline_failure(failed_step: &str) {
    println!("\n{}", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━".red());
    println!(
        "  {} {}",
        "✗ SKYNET WINS.".red().bold(),
        format!("Failed at: {}", failed_step).white()
    );
    println!("  {}", "Connor is down. Use `connor retry` to send resistance.".dimmed());
    println!("{}", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n".red());
}

pub fn info(msg: &str) {
    println!("{} {}", "ℹ".cyan(), msg);
}

pub fn warn(msg: &str) {
    println!("{} {}", "⚠".yellow(), msg.yellow());
}

pub fn parallel_stage_start(steps: Vec<&str>, from: usize, total: usize) {
    let names = steps.join(" + ");
    println!(
        "{} [{}/{}] {} {}",
        "▶ PARALLEL".magenta().bold(),
        from,
        total,
        names.white().bold(),
        "[concurrent]".dimmed()
    );
}

pub fn parallel_stage_end(success: bool) {
    if success {
        println!("{}", "  ✓ Stage secured".green());
    } else {
        println!("{}", "  ✗ Stage compromised".red());
    }
}

pub fn watch_trigger(path: &str) {
    println!(
        "\n{} {} — re-running mission...\n",
        "◈ CHANGE DETECTED:".yellow().bold(),
        path.white()
    );
}