use std::collections::HashMap;

/// Parse a `.env`-style file: `KEY=VALUE` lines, `#` comments, optional
/// `export ` prefix, optional surrounding quotes.
pub fn parse_env_file(content: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some(eq) = line.find('=') else {
            continue;
        };
        let key = line[..eq].trim();
        let mut value = line[eq + 1..].trim().to_string();
        if value.len() >= 2 {
            let first = value.chars().next().unwrap();
            let last = value.chars().last().unwrap();
            if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
                value = value[1..value.len() - 1].to_string();
            }
        }
        if key.is_empty() {
            continue;
        }
        out.push((key.to_string(), value));
    }
    out
}

/// Replace every `${NAME}` occurrence using `lookup`. Unknown variables are
/// left as-is so the shell can still resolve them.
pub fn interpolate(input: &str, lookup: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' && i + 1 < chars.len() && chars[i + 1] == '{' {
            if let Some(end) = chars[i + 2..].iter().position(|&c| c == '}') {
                let name: String = chars[i + 2..i + 2 + end].iter().collect();
                if let Some(value) = lookup.get(&name) {
                    out.push_str(value);
                    i += 2 + end + 1;
                    continue;
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// Replace all occurrences of `secrets` with `***`. Returns the redacted text
/// and whether any secret was found (leaked). Longest secrets are masked
/// first so short ones cannot re-expose parts of longer ones.
pub fn redact(output: &str, secrets: &[String]) -> (String, bool) {
    let mut ordered: Vec<&String> = secrets.iter().filter(|s| !s.is_empty()).collect();
    ordered.sort_by_key(|s| std::cmp::Reverse(s.len()));
    let mut leaked = false;
    let mut out = output.to_string();
    for secret in ordered {
        if out.contains(secret.as_str()) {
            leaked = true;
            out = out.replace(secret, "***");
        }
    }
    (out, leaked)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn parses_basic_lines() {
        let pairs = parse_env_file("FOO=bar\nBAZ=qux\n");
        assert_eq!(
            pairs,
            vec![("FOO".into(), "bar".into()), ("BAZ".into(), "qux".into())]
        );
    }

    #[test]
    fn ignores_comments_blanks_and_malformed() {
        let pairs = parse_env_file("# comment\n\nNO_EQUALS\nFOO = bar\nEXPORTED=1\n");
        assert_eq!(
            pairs,
            vec![
                ("FOO".into(), "bar".into()),
                ("EXPORTED".into(), "1".into())
            ]
        );
    }

    #[test]
    fn handles_export_prefix_and_quotes() {
        let pairs = parse_env_file("export TOKEN=\"abc123\"\nexport SPACED='a b c'\n");
        assert_eq!(
            pairs,
            vec![
                ("TOKEN".into(), "abc123".into()),
                ("SPACED".into(), "a b c".into())
            ]
        );
    }

    #[test]
    fn interpolates_known_vars_and_keeps_unknown() {
        let lookup = map(&[("NAME", "world")]);
        assert_eq!(interpolate("hello ${NAME}!", &lookup), "hello world!");
        assert_eq!(interpolate("${MISSING}", &lookup), "${MISSING}");
        assert_eq!(interpolate("no vars", &lookup), "no vars");
        assert_eq!(interpolate("${A}${B}", &lookup), "${A}${B}");
    }

    #[test]
    fn interpolates_empty_braces_literally() {
        let lookup = map(&[]);
        assert_eq!(interpolate("${}", &lookup), "${}");
    }

    #[test]
    fn redacts_all_occurrences() {
        let secrets = vec!["supersecret".to_string()];
        let (out, leaked) = redact("key=supersecret end", &secrets);
        assert_eq!(out, "key=*** end");
        assert!(leaked);
    }

    #[test]
    fn redact_reports_no_leak() {
        let (out, leaked) = redact("all clean", &["sekrit".to_string()]);
        assert_eq!(out, "all clean");
        assert!(!leaked);
    }

    #[test]
    fn redact_masks_longest_first() {
        // "abc" inside "abcd" must be masked as part of "abcd".
        let secrets = vec!["abc".to_string(), "abcd".to_string()];
        let (out, leaked) = redact("value: abcd xyz", &secrets);
        assert_eq!(out, "value: *** xyz");
        assert!(leaked);
    }

    #[test]
    fn redact_ignores_empty_secrets() {
        let (out, _) = redact("hello", &["".to_string()]);
        assert_eq!(out, "hello");
    }
}
