//! # The isolation rule, enforced (LLD §1)
//!
//! `src/mtconnect/**` is the owned MTConnect client, and it imports **nothing** from
//! `edgecommons`. That is what keeps the protocol code testable without a runtime, portable into a
//! crate of its own if it ever outgrows the module tree, and impossible to leak topics, envelopes,
//! metrics or the credential vault into.
//!
//! This test is the CI grep the design names, run as an ordinary test so a violation fails the same
//! `cargo test` a developer already runs.

use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read src/mtconnect") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn the_owned_client_imports_nothing_from_edgecommons() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("mtconnect");
    assert!(root.is_dir(), "src/mtconnect must exist");

    let mut files = Vec::new();
    rust_sources(&root, &mut files);
    assert!(files.len() >= 9, "the module tree of LLD §1: {} files found", files.len());

    let mut violations = Vec::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("read source");
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            if code.contains("edgecommons") {
                violations.push(format!("{}:{}: {}", file.display(), n + 1, line.trim()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "src/mtconnect/** must not reference edgecommons - the seam has leaked:\n{}",
        violations.join("\n")
    );
}

#[test]
fn the_module_tree_is_the_one_the_design_names() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join("mtconnect");
    for module in [
        "mod.rs",
        "config.rs",
        "client.rs",
        "stream.rs",
        "multipart.rs",
        "xml.rs",
        "model.rs",
        "observations.rs",
        "selection.rs",
        "sequence.rs",
        "error.rs",
    ] {
        assert!(root.join(module).is_file(), "src/mtconnect/{module} is missing");
    }
}
