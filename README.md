# Connor — Resistance CI/CD

```
   ██████╗ ██████╗ ███╗   ██╗███╗   ██╗ ██████╗ ██████╗
  ██╔════╝██╔═══██╗████╗  ██║████╗  ██║██╔═══██╗██╔══██╗
  ██║     ██║   ██║██╔██╗ ██║██╔██╗ ██║██║   ██║██████╔╝
  ██║     ██║   ██║██║╚██╗██║██║╚██╗██║██║   ██║██╔══██╗
  ╚██████╗╚██████╔╝██║ ╚████║██║ ╚████║╚██████╔╝██║  ██║
   ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝  ╚═╝

  RESISTANCE CI/CD — I'll be back.
```

A terminal-native CI/CD pipeline runner written in Rust. Fast, no server, no YAML hell. Define your mission in TOML, run it anywhere.

---

## Features

| Feature                   | Description                                        |
| ------------------------- | -------------------------------------------------- |
| **Parallel execution**    | Steps with no shared dependencies run concurrently |
| **Dependency graph**      | `depends_on` builds a DAG, auto-sorted into stages |
| **Retry logic**           | Per-step and global retry limits with backoff      |
| **Live output**           | Streams output in real time, redacts secrets       |
| **Per-step timeouts**     | Kills hung steps (whole process tree) automatically|
| **Per-step working dir**  | `cwd` for monorepo-friendly layouts                |
| **Env interpolation**     | `.env` + `${VAR}` expansion in commands and env    |
| **Dry run**               | Preview execution plan without running anything    |
| **Watch mode**            | Re-run on file changes, debounced                  |
| **Webhook notifications** | POST run results to any HTTP endpoint              |
| **Log file**              | Append step output to a log file                   |
| **Run history**           | Last 50 runs persisted to `.connor_history.json`   |
| **Smart retry**           | `connor retry` picks up from last failed step      |
| **JSON output**           | `status`/`history --json` for scripting            |

---

## Install

```bash
git clone https://github.com/kaelvalen/connor
cd connor
cargo install --path .
```

**NixOS / nix:**

```bash
nix run github:kaelvalen/connor        # build & run from flake
nix build .#connor                     # local flake build
nix develop                            # dev shell with rust toolchain
```

**Requirements:** Rust 1.75+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)

---

## Quick Start

```bash
# Initialize a config
connor init

# Edit connor.toml, then run
connor run

# Preview what would run (no execution)
connor run --dry-run

# Show last run status
connor status
```

---

## Mission Config (`connor.toml`)

See [`connor.toml.example`](connor.toml.example) for a full template.

```toml
[mission]
name = "operation-cleanup"
target = "production"
retry_limit = 3          # extra tries per step, on top of the first attempt
stop_on_failure = true   # halt pipeline on first failure
timeout_secs = 600       # default per-step timeout (seconds)
env_file = ".env"        # KEY=VALUE pairs for ${VAR} interpolation

[[steps]]
name = "lint"
cmd = "cargo clippy -- -D warnings"
timeout_secs = 120       # override the mission default for this step
cwd = "backend"          # run in a subdirectory

[[steps]]
name = "test"
cmd = "cargo test"

[[steps]]
name = "build"
cmd = "cargo build --release"
depends_on = ["lint", "test"]   # runs after both lint and test complete
retry = 2                        # extra retries for this step only

[[steps]]
name = "deploy"
cmd = "rsync -av target/release/app user@host:/opt/app"
depends_on = ["build"]
env = { DEPLOY_TOKEN = "${DEPLOY_TOKEN}", TARGET_HOST = "prod-01" }
```

### Timeouts

Every step (and each retry attempt) has a timeout. If it fires, Connor kills
the **entire process tree** (shell + grandchildren like `docker` or `cargo`)
and the step fails. Default via `mission.timeout_secs`, override per step with
`timeout_secs`. No timeout is applied if neither is set. On Windows, only the
shell process is killed (best-effort).

### Env & Secrets

- `env_file` (default `.env`, resolved relative to the config file) and step
  `env` values are interpolated into `cmd`, `cwd`, and env values with
  `${VAR}` syntax. Precedence: process env → env file → step `env`.
- Secret values (step `env` and env-file values, ≥4 chars) are **redacted**
  (`***`) from live output, the capture buffer, history, and log files.

```bash
# .env — never commit this (it's gitignored)
DEPLOY_TOKEN=shhh-this-is-a-secret
```

```toml
[[steps]]
name = "deploy"
cmd = "deploy.sh ${TARGET_HOST}"
env = { DEPLOY_TOKEN = "${DEPLOY_TOKEN}" }
```

### Parallel Execution

Steps with no dependency relationship are automatically grouped into parallel stages:

```
lint ─┐
      ├─▶ build ─▶ deploy
test ─┘
```

```toml
[[steps]]
name = "lint"
cmd = "eslint src/"

[[steps]]
name = "test"
cmd = "jest"

[[steps]]
name = "build"
cmd = "webpack --mode production"
depends_on = ["lint", "test"]   # lint and test run in parallel
```

Connor detects the DAG and schedules `lint` and `test` concurrently. `build` waits for both.

---

## Commands

### `connor run`

```bash
connor run                          # run all steps
connor run --from deploy            # start from a specific step
connor run --dry-run                # preview execution plan
connor run --log connor.log         # write output to log file
connor run --webhook https://...    # notify URL on completion
connor run --retry-limit 5          # override mission retry_limit
connor run --target staging         # override mission target
```

### `connor retry`

Reads the last failed run from history, resumes from the failed step:

```bash
connor retry
connor retry --webhook https://...
```

### `connor status`

Shows detailed breakdown of the last run:

```
━━━ LAST MISSION STATUS ━━━
Mission: operation-cleanup
Time:    2026-04-29 19:30:20 UTC
Elapsed: 2341ms
Status:  ✓ JUDGMENT DAY AVERTED

Steps:
  ✓ lint       34ms
  ✓ test       1820ms
  ✓ build      480ms
  ✓ deploy     7ms
```

For scripting: `connor status --json` prints the full run record as JSON
(step statuses, timings, captured output).

### `connor history`

```bash
connor history          # last 10 runs
connor history -l 25    # last 25 runs
connor history --json   # last 10 runs as JSON (pairs with -l)
```

Output:

```
━━━ MISSION HISTORY ━━━
  [1] 04-29 19:30  ✓ AVERTED  operation-cleanup  4/4
  [2] 04-29 18:12  ✗ SKYNET   operation-hotfix   2/4
  [3] 04-29 17:55  ✓ AVERTED  operation-cleanup  3/3
```

### `connor watch`

Re-runs the pipeline whenever files change:

```bash
connor watch                        # watch current directory
connor watch --path src/            # watch specific path
connor watch --debounce 1000        # debounce 1000ms (default: 500ms)
connor watch --webhook https://...  # notify on each run
```

Ignores `.connor_history.json` and `.log` files to avoid feedback loops.

### `connor init`

Creates a `connor.toml` template in the current directory.

### `connor validate`

Checks the mission config without running anything: parses TOML, validates the
dependency graph (duplicate/unknown/self-deps, cycles), and verifies the
configured `env_file` exists. Exits non-zero on any problem.

### `connor completions`

```bash
connor completions bash   # or zsh, fish, elvish, powershell
# e.g. source <(connor completions zsh)
```

Config lookup walks **up** parent directories, git-style — run `connor` from
anywhere inside your project.

---

## Webhook Payload

When `--webhook` is provided, Connor POSTs JSON on pipeline completion:

```json
{
  "mission": "operation-cleanup",
  "status": "failed",
  "elapsed_ms": 1234,
  "timestamp": "2026-04-29T19:30:20Z",
  "failed_step": "test",
  "steps": [
    { "name": "lint",  "status": "success", "elapsed_ms": 34, "attempts": 1 },
    { "name": "test",  "status": "failed",  "elapsed_ms": 800, "attempts": 3 },
    { "name": "build", "status": "skipped", "elapsed_ms": 0,   "attempts": 0 }
  ]
}
```

Compatible with Slack incoming webhooks, Discord webhooks, or any custom endpoint.

---

## Config Reference

### `[mission]`

| Key               | Type    | Default     | Description                               |
| ----------------- | ------- | ----------- | ----------------------------------------- |
| `name`            | string  | required    | Mission identifier                        |
| `target`          | string  | `"unknown"` | Deployment target label                   |
| `retry_limit`     | int     | `3`         | Extra tries per step, after the first     |
| `stop_on_failure` | bool    | `true`      | Halt on first failure                     |
| `timeout_secs`    | int     | none        | Default timeout per step, in seconds      |
| `env_file`        | string  | `".env"`    | Env file path, relative to the config dir |

### `[[steps]]`

| Key           | Type     | Default  | Description                                              |
| ------------- | -------- | -------- | -------------------------------------------------------- |
| `name`        | string   | required | Step identifier (used in `depends_on`)                   |
| `cmd`         | string   | required | Shell command to execute                                 |
| `depends_on`  | string[] | `[]`     | Steps that must succeed before this one                  |
| `retry`       | int      | `0`      | Additional retries for this step (adds to `retry_limit`) |
| `env`         | map      | `{}`     | Environment variables injected at runtime                |
| `cwd`         | string   | config dir | Working directory for this step                        |
| `timeout_secs`| int      | mission  | Timeout for this step, in seconds                        |

---

## Output Glossary

| Symbol                     | Meaning                   |
| -------------------------- | ------------------------- |
| `▶ DEPLOYING`              | Sequential step starting  |
| `▶ PARALLEL`               | Concurrent stage starting |
| `✓ SECURED`                | Step succeeded            |
| `✗ SKYNET ATTACK DETECTED` | Step failed               |
| `↺ SENDING RESISTANCE`     | Retrying step             |
| `⚠ Step exceeded time limit` | Step killed by timeout  |
| `✓ JUDGMENT DAY AVERTED`   | Pipeline succeeded        |
| `✗ SKYNET WINS`            | Pipeline failed           |

---

## Project Structure

```
connor/
├── src/
│   ├── main.rs       — CLI (clap): all commands and flags
│   ├── runner.rs     — pipeline engine: DAG stages, parallel execution, retry, timeouts
│   ├── config.rs     — TOML parsing, Config/Step structs, config discovery
│   ├── env.rs        — .env parsing, ${VAR} interpolation, secret redaction
│   ├── history.rs    — run persistence (.connor_history.json)
│   ├── logger.rs     — themed terminal output
│   └── webhook.rs    — HTTP POST notifications
├── connor.toml.example — template mission config
├── flake.nix        — NixOS/nix packaging and dev shell
├── Cargo.toml
└── connor.toml       — your mission config (not committed)
```

---

## Use with CI

Connor runs anywhere a shell exists. No daemon, no setup.

```yaml
# GitHub Actions
- name: Run Connor
  run: |
    cargo build --release
    ./target/release/connor run --webhook ${{ secrets.SLACK_WEBHOOK }}
```

```bash
# Makefile
deploy:
    connor run --log deploy.log --webhook $$WEBHOOK_URL
```

---

## License

MIT — see [LICENSE](LICENSE).
