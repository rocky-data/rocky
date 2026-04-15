use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rocky_lang::fmt::format_rocky;
use tracing::info;

/// Execute `rocky fmt` — format `.rocky` files.
///
/// When `check_only` is true, reports which files would change and returns
/// `Err` (exit code 1) if any file needs formatting. Otherwise, rewrites
/// files in place.
///
/// `paths` can be individual `.rocky` files or directories (searched
/// recursively). If empty, defaults to the current directory.
pub fn run_fmt(paths: &[PathBuf], check_only: bool) -> Result<()> {
    let files = collect_rocky_files(paths)?;

    if files.is_empty() {
        info!("no .rocky files found");
        return Ok(());
    }

    let indent = "    "; // 4 spaces, matching Rocky convention
    let mut unformatted: Vec<PathBuf> = Vec::new();

    for file in &files {
        let original =
            std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
        let formatted = format_rocky(&original, indent);

        if formatted == original {
            continue;
        }

        if check_only {
            eprintln!("would reformat: {}", file.display());
            unformatted.push(file.clone());
        } else {
            std::fs::write(file, &formatted)
                .with_context(|| format!("writing {}", file.display()))?;
            eprintln!("reformatted: {}", file.display());
        }
    }

    if check_only && !unformatted.is_empty() {
        anyhow::bail!("{} file(s) would be reformatted", unformatted.len());
    }

    let verb = if check_only { "checked" } else { "formatted" };
    info!("{verb} {} file(s)", files.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// File discovery
// ---------------------------------------------------------------------------

/// Collect all `.rocky` files from the given paths. Directories are walked
/// recursively; plain files are included as-is if they have a `.rocky`
/// extension.
fn collect_rocky_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let roots: Vec<PathBuf> = if paths.is_empty() {
        vec![PathBuf::from(".")]
    } else {
        paths.to_vec()
    };

    let mut files = Vec::new();
    for root in &roots {
        if root.is_file() {
            if root.extension().is_some_and(|e| e == "rocky") {
                files.push(root.clone());
            }
        } else if root.is_dir() {
            walk_dir(root, &mut files)?;
        } else {
            anyhow::bail!("path does not exist: {}", root.display());
        }
    }
    files.sort();
    Ok(files)
}

fn walk_dir(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_dir(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "rocky") {
            out.push(path);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_fmt_check_mode() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rocky");
        std::fs::write(&file, "  from orders   \n\n\n\n\nwhere true\n").unwrap();

        let result = run_fmt(std::slice::from_ref(&file), true);
        assert!(result.is_err());

        // File should be unchanged in check mode
        let content = std::fs::read_to_string(&file).unwrap();
        assert_eq!(content, "  from orders   \n\n\n\n\nwhere true\n");
    }

    #[test]
    fn test_run_fmt_write_mode() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rocky");
        std::fs::write(&file, "  from orders   \n\n\n\n\nwhere true\n").unwrap();

        run_fmt(std::slice::from_ref(&file), false).unwrap();

        let content = std::fs::read_to_string(&file).unwrap();
        assert_eq!(content, "from orders\n\n\nwhere true\n");
    }

    #[test]
    fn test_run_fmt_directory() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("models");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("a.rocky"), "  from x  \n").unwrap();
        std::fs::write(sub.join("b.rocky"), "from y\n").unwrap();
        std::fs::write(sub.join("c.sql"), "SELECT 1;\n").unwrap(); // not .rocky

        run_fmt(&[dir.path().to_path_buf()], false).unwrap();

        assert_eq!(
            std::fs::read_to_string(sub.join("a.rocky")).unwrap(),
            "from x\n"
        );
        // b.rocky was already formatted
        assert_eq!(
            std::fs::read_to_string(sub.join("b.rocky")).unwrap(),
            "from y\n"
        );
        // .sql untouched
        assert_eq!(
            std::fs::read_to_string(sub.join("c.sql")).unwrap(),
            "SELECT 1;\n"
        );
    }
}
