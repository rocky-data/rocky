//! `rocky connector init` — scaffold a child connector.
//!
//! Writes a directory with a `connector.toml` manifest, a `main.py` program
//! and the `rocky_connector.py` helper. The helper is embedded in the binary,
//! so the copy a scaffold gets matches the protocol this Rocky speaks. The
//! scaffold loads as written: `rocky load --connector <name>` runs it once
//! the `[connector.<name>]` block it prints is in `rocky.toml`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// The helper module, as `rocky connector init` writes it.
pub const HELPER_PY: &str = include_str!("connector_data/rocky_connector.py");
const MAIN_PY: &str = include_str!("connector_data/main.py");
const MANIFEST_TOML: &str = include_str!("connector_data/connector.toml");
const TEST_SPEC_TOML: &str = include_str!("connector_data/connector-test.toml");

/// The file name the helper is written under.
pub const HELPER_FILE_NAME: &str = "rocky_connector.py";

/// Languages `rocky connector init --lang` accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ConnectorLang {
    /// Python 3.8 or newer, standard library only, plus the `duckdb` CLI.
    Python,
}

/// Write the scaffold for connector `name` into `dir`, or into
/// `connectors/<name>` when `dir` is `None`. Returns the directory.
///
/// # Errors
///
/// Fails when `name` is not a plain identifier, when the directory already
/// exists, or when a file cannot be written.
pub fn scaffold_connector(name: &str, dir: Option<&Path>, lang: ConnectorLang) -> Result<PathBuf> {
    // The same rule `[connector.<name>]` applies, so the printed block is
    // valid as written.
    rocky_sql::validation::validate_identifier(name)
        .map_err(|e| anyhow::anyhow!("`{name}` is not a valid connector name: {e}"))?;
    let dir = dir.map_or_else(|| Path::new("connectors").join(name), Path::to_path_buf);
    if dir.exists() {
        bail!(
            "{} already exists; pick another directory with --dir, or remove it",
            dir.display()
        );
    }
    if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    // `create_dir`, not `create_dir_all`: it fails if another process made
    // the directory since the check, so the cleanup below only ever removes
    // a directory this call created.
    std::fs::create_dir(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let written = match lang {
        ConnectorLang::Python => write_python(name, &dir),
    };
    if let Err(e) = written {
        // A partial scaffold would make the rerun refuse with "already
        // exists".
        let _ = std::fs::remove_dir_all(&dir);
        return Err(e);
    }
    Ok(dir)
}

fn write_python(name: &str, dir: &Path) -> Result<()> {
    let write = |file: &str, body: &str| -> Result<()> {
        let path = dir.join(file);
        std::fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))
    };
    write("connector.toml", &MANIFEST_TOML.replace("__NAME__", name))?;
    write("main.py", &MAIN_PY.replace("__NAME__", name))?;
    write(
        super::connector_test::TEST_SPEC_FILE_NAME,
        &TEST_SPEC_TOML.replace("__NAME__", name),
    )?;
    write(HELPER_FILE_NAME, HELPER_PY)?;
    // Python writes `__pycache__/` beside the helper on each load.
    write(".gitignore", "__pycache__/\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let main = dir.join("main.py");
        std::fs::set_permissions(&main, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to make {} executable", main.display()))?;
    }
    Ok(())
}

/// The `[connector.<name>]` block to paste into `rocky.toml`.
pub(crate) fn config_block(name: &str, dir: &Path) -> String {
    let manifest = dir.join("connector.toml");
    let manifest = toml::Value::String(manifest.to_string_lossy().replace('\\', "/"));
    format!(
        "[connector.{name}]\n\
         manifest   = {manifest}\n\
         output_uri = \"file://./.rocky/landing/{name}\"\n\
         target     = {{ catalog = \"<catalog>\", schema = \"<schema>\", table = \"{name}\" }}\n"
    )
}

/// `rocky connector init <name>`.
///
/// # Errors
///
/// See [`scaffold_connector`].
pub fn run_connector_init(name: &str, dir: Option<&Path>, lang: ConnectorLang) -> Result<()> {
    let dir = scaffold_connector(name, dir, lang)?;
    let block = config_block(name, &dir);
    {
        println!("Created connector `{name}` in {}", dir.display());
        println!();
        println!("Next steps:");
        println!("  1. Add this block to rocky.toml, with your target catalog and schema.");
        println!("     `manifest` is relative to the directory that holds rocky.toml:");
        println!();
        for line in block.lines() {
            println!("     {line}");
        }
        println!();
        println!("  2. Replace fetch() in {}.", dir.join("main.py").display());
        println!(
            "  3. Set `license` in {}.",
            dir.join("connector.toml").display()
        );
        println!("  4. Check it: rocky connector test {name}");
        println!("  5. Run: rocky load --connector {name}");
        println!();
        println!("The connector needs python3 (3.8 or newer) and the duckdb CLI on PATH.");
        if cfg!(not(unix)) {
            println!(
                "On this system `./main.py` does not start through its `#!` line: set \
                 `command` in connector.toml to a program that runs it."
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::connector::{ConnectorManifest, PROTOCOL_VERSION, RESULT_FILE_NAME};

    fn python_constant(name: &str) -> String {
        let prefix = format!("{name} = ");
        HELPER_PY
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("the helper defines no {name}"))
            .trim()
            .to_string()
    }

    #[test]
    fn the_helper_speaks_this_rocky_protocol() {
        assert_eq!(
            python_constant("PROTOCOL_VERSION"),
            PROTOCOL_VERSION.to_string()
        );
        assert_eq!(
            python_constant("RESULT_FILE"),
            format!("\"{RESULT_FILE_NAME}\"")
        );
        // Raise this, and the constant, when the helper changes in a way a
        // copy in a user's repository should be told about.
        assert_eq!(python_constant("HELPER_VERSION"), "1");
    }

    #[test]
    fn the_scaffold_manifest_loads_and_sets_no_license() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("demo");
        scaffold_connector("demo_src", Some(&dir), ConnectorLang::Python).unwrap();
        let manifest = ConnectorManifest::load(&dir.join("connector.toml")).unwrap();
        assert_eq!(manifest.name, "demo_src");
        assert_eq!(manifest.state_schema_version, 1);
        assert!(manifest.license.is_none());
        assert!(manifest.support_tier.is_none());
        assert_eq!(
            std::fs::read_to_string(dir.join(HELPER_FILE_NAME)).unwrap(),
            HELPER_PY
        );
        assert_eq!(
            std::fs::read_to_string(dir.join(".gitignore")).unwrap(),
            "__pycache__/\n"
        );
        let main = std::fs::read_to_string(dir.join("main.py")).unwrap();
        assert!(!main.contains("__NAME__"));
        assert!(main.contains("STATE_SCHEMA_VERSION = 1"));
        let spec = crate::commands::ConnectorTestSpec::load(
            &dir.join(crate::commands::TEST_SPEC_FILE_NAME),
        )
        .unwrap();
        assert_eq!(spec.runs.len(), 3);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join("main.py"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o111, 0o111);
        }
    }

    #[test]
    fn an_existing_directory_is_refused_and_left_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("taken");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("keep.txt"), "mine").unwrap();
        let err = scaffold_connector("demo", Some(&dir), ConnectorLang::Python).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
    }

    #[test]
    fn a_name_that_is_not_an_identifier_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        for name in ["../escape", "a/b", "", "has space", "x;drop"] {
            let dir = tmp.path().join("never");
            let err = scaffold_connector(name, Some(&dir), ConnectorLang::Python).unwrap_err();
            assert!(
                err.to_string().contains("not a valid connector name"),
                "{name}: {err}"
            );
            assert!(!dir.exists(), "{name}");
        }
    }

    #[test]
    fn a_directory_with_a_quote_still_prints_valid_toml() {
        let block = config_block("demo", Path::new("odd\"dir"));
        let value: toml::Value = toml::from_str(&block).unwrap();
        assert_eq!(
            value["connector"]["demo"]["manifest"].as_str(),
            Some("odd\"dir/connector.toml")
        );
    }

    #[test]
    fn the_printed_block_parses_as_a_connector_config() {
        let block = config_block("demo", Path::new("connectors/demo"));
        let block = block
            .replace("<catalog>", "poc")
            .replace("<schema>", "main");
        let value: toml::Value = toml::from_str(&block).unwrap();
        let connector: rocky_core::connector::ConnectorConfig =
            value["connector"]["demo"].clone().try_into().unwrap();
        assert_eq!(connector.manifest, "connectors/demo/connector.toml");
        let problems =
            rocky_core::connector::check_connector_block("demo", &connector, Path::new("."));
        assert!(problems.is_empty(), "{problems:?}");
    }
}
