//! Guard: the example manifests shipped in `manifests/` stay valid as the schema evolves.

use std::path::PathBuf;

use lldb_qe_core::manifest::{CatalogBackend, Manifest};

fn manifests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests")
}

#[test]
fn shipped_manifests_parse_and_validate() {
    for name in ["empty.toml", "tpch.toml", "shared-catalog.toml"] {
        let path = manifests_dir().join(name);
        Manifest::from_path(&path)
            .unwrap_or_else(|e| panic!("example manifest {name} failed to parse/validate: {e:#}"));
    }
}

#[test]
fn shared_catalog_example_declares_a_uri_less_sql_backend() {
    // The example exists to show the shape a fleet actually deploys: `kind = "sql"` with no
    // `uri`, so the Postgres credential comes from the environment rather than from a file in
    // git. A future edit that "helpfully" inlines a URI here should fail this test.
    let m = Manifest::from_path(&manifests_dir().join("shared-catalog.toml")).unwrap();
    assert_eq!(m.catalogs[0].backend, CatalogBackend::Sql { uri: None });
    // …and a `file://` warehouse, because iceberg 0.10 has no object-store StorageFactory.
    let warehouse = m.catalogs[0].warehouse.as_deref().expect("a warehouse");
    assert!(warehouse.starts_with("file://"), "got {warehouse}");
}

#[test]
fn tpch_example_declares_eight_tables() {
    let m = Manifest::from_path(&manifests_dir().join("tpch.toml")).unwrap();
    let tables = &m.catalogs[0].namespaces[0].tables;
    assert_eq!(tables.len(), 8, "TPC-H example should list all 8 tables");
}
