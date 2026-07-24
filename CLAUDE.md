# lldb — build notes for Claude

Distributed analytical query engine: DataFusion + Arrow Flight + Iceberg on object storage.
Learning-grade POC. Trunk-based — commit working milestones straight to `main`.

## Version pins — do NOT bump independently

`iceberg-datafusion` 0.10 caps `datafusion` at `^53.1`; everything downstream follows.

| Crate | Pin | Reason |
| - | - | - |
| datafusion / datafusion-proto | 53.1 | capped by iceberg-datafusion 0.10 |
| iceberg / iceberg-datafusion | 0.10 | latest |
| arrow / arrow-flight / parquet | 58.4 | datafusion 53.1 → arrow ^58; ONE version tree-wide |
| object_store | 0.13 | datafusion 53.1 → object_store ^0.13.1 |

Coordinator and workers must run the identical build — serialized DataFusion plans are not
cross-version compatible. Bumping DataFusion to 54 waits on `iceberg-datafusion` 0.11.

Every binary is stamped with `version+git-sha` (workspace version + the commit, injected by
`lldb-qe-core/build.rs`): both binaries report it via `--version` and a startup log line, so an
operator can confirm a whole fleet is the identical build. CI builds one image, tags it by that
version, and the compose cluster runs that single tag for every role (`LLDB_IMAGE`).

## Layout

- `crates/lldb-qe-core` — storage (`storage.rs`, incl. S3), config-as-data catalog
  (`manifest.rs` + `catalog.rs`), session, Flight transport, plan codec, shared CLI/logging
  config (`config.rs`)
- `crates/lldb-qe-coordinator`, `crates/lldb-qe-worker` — thin clap/env-configured binaries
- `manifests/` — example catalog manifests (config-as-data); TPC-H is just one of them
- `Dockerfile` / `docker-compose.yml` — one image, both roles; a MinIO + worker-fleet cluster
- `infra/` — AWS CDK (TypeScript): ECS Fargate worker fleet, S3 warehouse, ECR. **CDK, not
  Terraform.** Deploys one pinned `imageTag` to every role; synth fails on `latest`
- `data/` — generated TPC-H + local Iceberg warehouse (gitignored)

## Catalogs are config, not code

Do NOT hardcode schemas. Declare tables in a `Manifest` (see `manifest.rs`) and load them with
`catalog::apply_manifest`. `tpch_manifest` / `register_tpch_parquet` are thin TPC-H seeds over
that generic path — add new schemas as manifests, not bespoke loaders.

## Commands

```
tpchgen-cli -s 1 --format=parquet --output-dir data/sf1   # test data
cargo test                                                # unit + integration (data-absent tests skip)
cargo fmt --all && cargo clippy --all-targets
docker compose up --build                                 # full containerized cluster
LLDB_DOCKER=1 cargo test --test distributed_cluster       # cross-container smoke test (needs a daemon)
cd infra && npm ci && npm test                            # CDK assertion tests
cd infra && npx cdk synth -c imageTag=<version+sha>       # emit CloudFormation
```

## Testing bar

Every module carries a `#[cfg(test)] mod tests`; end-to-end paths get a `tests/` integration
test. No milestone lands without green tests.
