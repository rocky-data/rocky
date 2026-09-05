//! With the `ui` feature on, the browser UI's built files are embedded from
//! `engine/ui/dist`. `include_dir!` needs the directory to exist at compile
//! time, and a build with the feature but no `dist/` (CI's `--all-features`
//! lint, a checkout that never ran `npm run build`) must still compile. So
//! this script writes the one line the `ui` module includes: the real
//! `include_dir!` when `dist/index.html` exists, an empty directory
//! otherwise. `rocky serve --ui` then refuses an empty embed at start and
//! names the command to run; the release workflow builds `dist/` before every
//! target, and its smoke test loads the page.

use std::path::Path;

fn main() {
    let ui = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui");
    let dist = ui.join("dist");
    let index = dist.join("index.html");
    // The parent too: a `dist/` that appears or disappears changes `ui/`'s
    // own mtime, which is the only signal when `dist/` did not exist at the
    // last build (or was moved into place with old timestamps).
    println!("cargo:rerun-if-changed={}", ui.display());
    println!("cargo:rerun-if-changed={}", dist.display());
    println!("cargo:rerun-if-changed={}", index.display());
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_UI");

    let out = Path::new(&std::env::var_os("OUT_DIR").expect("OUT_DIR is set by cargo"))
        .join("ui_assets.rs");
    let source = if index.is_file() {
        let dist = dist
            .canonicalize()
            .expect("dist exists, so it canonicalizes")
            .display()
            .to_string()
            .replace('\\', "/");
        format!(
            "/// `engine/ui/dist`, as built by `npm run build`.\n\
             static DIST: ::include_dir::Dir<'static> = ::include_dir::include_dir!({dist:?});\n"
        )
    } else {
        if std::env::var_os("CARGO_FEATURE_UI").is_some() {
            println!(
                "cargo:warning=the `ui` feature is on but {} has no index.html; the \
                 embedded UI is empty and `rocky serve --ui` will refuse to start. \
                 Build it first: `cd engine/ui && npm ci && npm run build`.",
                dist.display()
            );
        }
        "/// No `engine/ui/dist` at build time: an empty embed, refused by `--ui`.\n\
         static DIST: ::include_dir::Dir<'static> = ::include_dir::Dir::new(\"\", &[]);\n"
            .to_string()
    };
    std::fs::write(&out, source).expect("write the generated ui_assets.rs");
}
