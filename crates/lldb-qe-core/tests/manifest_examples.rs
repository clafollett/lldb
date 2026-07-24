//! Guard: the example manifests shipped in `manifests/` stay valid as the schema evolves.

use std::path::PathBuf;

use lldb_qe_core::manifest::Manifest;

fn manifests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests")
}

#[test]
fn shipped_manifests_parse_and_validate() {
    for name in ["empty.toml", "tpch.toml"] {
        let path = manifests_dir().join(name);
        Manifest::from_path(&path)
            .unwrap_or_else(|e| panic!("example manifest {name} failed to parse/validate: {e:#}"));
    }
}

#[test]
fn tpch_example_declares_eight_tables() {
    let m = Manifest::from_path(&manifests_dir().join("tpch.toml")).unwrap();
    let tables = &m.catalogs[0].namespaces[0].tables;
    assert_eq!(tables.len(), 8, "TPC-H example should list all 8 tables");
}
