use std::fs;
use std::path::{Path, PathBuf};

const TRACKER: &str = include_str!("CLIENT_TEST_SUITE_PARITY.md");

#[test]
fn migrated_suite_files_are_live_only_and_use_java_parity_comments() {
    for (suite, status) in tracker_entries() {
        if status != "migrated" {
            continue;
        }

        let path = suite_file_path(&suite);
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read suite file {}: {err}", path.display()));

        assert!(
            !contains_mock_usage(&text),
            "migrated suite {} still uses mock thin-server coverage in {}",
            suite,
            path.display()
        );
        assert!(
            !contains_github_blob_link(&text),
            "migrated suite {} still contains GitHub blob links in {}",
            suite,
            path.display()
        );
        assert!(
            text.contains("Java parity:"),
            "migrated suite {} must include Java parity comments in {}",
            suite,
            path.display()
        );
    }
}

#[test]
fn live_only_suite_files_do_not_use_github_blob_links() {
    for (suite, _) in tracker_entries() {
        let path = suite_file_path(&suite);
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read suite file {}: {err}", path.display()));

        if contains_mock_usage(&text) {
            continue;
        }

        assert!(
            !contains_github_blob_link(&text),
            "live-backed suite {} still contains GitHub blob links in {}",
            suite,
            path.display()
        );
    }
}

#[test]
fn tracked_suite_files_use_java_parity_comment_prefix_only() {
    for (suite, _) in tracker_entries() {
        let path = suite_file_path(&suite);
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read suite file {}: {err}", path.display()));

        assert!(
            !text.contains("Java reference:"),
            "tracked suite {} still uses `Java reference:` comments in {}",
            suite,
            path.display()
        );
    }
}

#[test]
fn mock_regression_files_do_not_declare_java_parity() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let entries = fs::read_dir(&tests_dir)
        .unwrap_or_else(|err| panic!("failed to read tests dir {}: {err}", tests_dir.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|err| panic!("failed to read tests dir entry: {}", err));
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("expected UTF-8 test file name");

        if !file_name.ends_with("_mock_regression_test.rs") {
            continue;
        }

        let text = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("failed to read regression file {}: {err}", path.display())
        });

        assert!(
            !text.contains("Java parity:"),
            "mock regression file {} must not declare Java parity comments",
            path.display()
        );
    }
}

fn tracker_entries() -> Vec<(String, String)> {
    let mut in_current_status = false;

    TRACKER
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line == "Current status:" {
                in_current_status = true;
                return None;
            }
            if line == "Layout notes:" {
                in_current_status = false;
                return None;
            }
            if !in_current_status {
                return None;
            }
            if !line.starts_with("- `") {
                return None;
            }

            let remainder = line.strip_prefix("- `")?;
            let (suite, remainder) = remainder.split_once("`: ")?;
            Some((suite.to_string(), remainder.to_string()))
        })
        .collect()
}

fn suite_file_path(suite: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let file_name = format!("{}.rs", camel_to_snake(suite));
    root.join(file_name)
}

fn camel_to_snake(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 8);

    for (idx, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }

    out
}

fn contains_mock_usage(text: &str) -> bool {
    text.contains("spawn_mock_thin_server") || text.contains("MockThinServer")
}

fn contains_github_blob_link(text: &str) -> bool {
    text.contains("github.com/apache") && text.contains("/ignite/blob/")
}
