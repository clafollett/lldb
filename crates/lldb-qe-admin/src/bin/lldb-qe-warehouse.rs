//! `lldb-qe-warehouse` — the control plane's hands: create, list, resize, suspend, resume and
//! drop [virtual warehouses](lldb_qe_control::warehouse).
//!
//! # What this binary does, precisely
//!
//! It **edits rows**. A warehouse row is the *desired state* of a pool of compute; this is the
//! tool that states the desire. Nothing here talks to ECS, to Docker, or to any orchestrator —
//! see the module docs on [`lldb_qe_control::warehouse`] for why the engine stays declarative (the
//! short version: the AWS SDK is a large dependency in a workspace whose whole build story is
//! "one `arrow`/`object_store`/`datafusion` tree-wide", and baking one cloud's API into the
//! control plane would make the abstraction less portable than the thing it abstracts).
//!
//! So every mutation prints the **apply step** a human still owes it — the `aws ecs
//! update-service` or `docker compose up --scale` that makes the world match the row. That line
//! is not decoration: it is the difference between a tool that looks like it scaled your fleet
//! and one that tells you the truth about what it did.
//!
//! # Why a separate binary
//!
//! Same reasoning as `lldb-qe-migrate`: control-plane operations are one-shot, they have an exit
//! code, and they must not be reachable from the query path. Shipping them inside the coordinator
//! would mean any process that can run a query can also resize the fleet — which is exactly the
//! separation accounts/RBAC (#19) exists to enforce. It lives in the same image so the binary
//! that writes a warehouse row is the same build as the one that reads it.
//!
//!   lldb-qe-warehouse --metadata-url postgres://lldb@localhost/lldb create --name analytics --size 4
//!   lldb-qe-warehouse --metadata-url postgres://lldb@localhost/lldb list
//!   lldb-qe-warehouse --metadata-url postgres://lldb@localhost/lldb resize --name analytics --size 8
//!   lldb-qe-warehouse --metadata-url postgres://lldb@localhost/lldb suspend --name analytics
//!   lldb-qe-warehouse --metadata-url postgres://lldb@localhost/lldb resume  --name analytics
//!
//! # Not a fleet member
//!
//! fleet-posture-allow: it edits desired-state rows; binds no port, never handed LLDB_FLEET_TOKEN.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use lldb_qe_control::warehouse::{Warehouse, WarehouseState};
use lldb_qe_control::{Account, ServicesArgs, ServicesDb, init_tracing, redact_url};

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-warehouse",
    about = "Manage virtual warehouses: named, resizable, suspendable compute pools",
    long_about = "Writes DESIRED state to the services database. Something else makes it real — a CDK deploy, \
`aws ecs update-service --desired-count`, or `docker compose up --scale`. The engine carries no \
orchestrator SDK on purpose, so the same rows can drive any of them. Every mutation prints the \
apply command to run next, with the desired count filled in and the cluster and service names left \
as placeholders — the engine does not know them.",
    version = lldb_qe_control::BUILD_VERSION
)]
struct Cli {
    /// Tenant that owns the warehouse. Warehouse names are unique *within* an account, so two
    /// tenants may each have an `analytics` — every operation here is scoped by this.
    #[arg(long, env = "LLDB_ACCOUNT", default_value = "default", global = true)]
    account: String,

    #[command(flatten)]
    services: ServicesArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Define a new warehouse.
    Create {
        #[arg(long)]
        name: String,
        /// Desired worker count.
        #[arg(long, default_value_t = 1)]
        size: i32,
        /// Create it suspended — defined, sized, but with no compute (and no bill) until resumed.
        #[arg(long)]
        suspended: bool,
        /// Succeed (returning the existing warehouse **unchanged**) if the name is already taken,
        /// instead of failing. For bootstrap scripts that run on every deploy — it will not
        /// resize or resume what it finds, since that would undo an operator's deliberate change.
        #[arg(long)]
        if_not_exists: bool,
    },
    /// List this account's warehouses.
    List,
    /// Change a warehouse's desired worker count. Legal while running *or* suspended.
    Resize {
        #[arg(long)]
        name: String,
        #[arg(long)]
        size: i32,
    },
    /// Release a warehouse's compute. Its size is retained for the next resume.
    Suspend {
        #[arg(long)]
        name: String,
    },
    /// Bring a suspended warehouse back to its size.
    Resume {
        #[arg(long)]
        name: String,
    },
    /// Drop a warehouse definition entirely. Query history survives it.
    Delete {
        #[arg(long)]
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_control::BUILD_VERSION,
        "starting lldb-qe-warehouse"
    );

    // Like `lldb-qe-migrate` and unlike the coordinator, this binary is *about* the control
    // plane — there is no meaningful unconfigured behaviour, so say so with the flag names.
    let Some(url) = cli.services.resolve_url()? else {
        bail!(
            "no services database configured: set --metadata-url (LLDB_METADATA_URL), or \
             --metadata-host (LLDB_METADATA_HOST) plus the other --metadata-* parts"
        );
    };
    let db = cli
        .services
        .connect()
        .await?
        .expect("a url resolved, so connect() cannot report `unconfigured`");
    db.health_check()
        .await
        .with_context(|| format!("services database at {} is not answering", redact_url(&url)))?;

    let account = resolve_account(&db, &cli.account).await?;
    let result = run(&db, &account, cli.command).await;
    db.close().await;
    result
}

/// Turn `--account` into a row, or explain how to make one. Deliberately not `ensure_account`:
/// creating a tenant as a side effect of a typo'd warehouse command is how ghost tenants
/// accumulate.
async fn resolve_account(db: &ServicesDb, name: &str) -> Result<Account> {
    db.account_by_name(name).await?.with_context(|| {
        format!(
            "account `{name}` does not exist in the services database; create it with \
             `lldb-qe-migrate --seed-account {name}`"
        )
    })
}

async fn run(db: &ServicesDb, account: &Account, command: Command) -> Result<()> {
    match command {
        Command::Create {
            name,
            size,
            suspended,
            if_not_exists,
        } => {
            let state = if suspended {
                WarehouseState::Suspended
            } else {
                // Running by default: you almost always want to query the warehouse you just
                // made, and `--suspended` is right there for the case where you do not.
                WarehouseState::Running
            };
            let warehouse = if if_not_exists {
                db.ensure_warehouse(account.id, &name, size, state).await
            } else {
                db.create_warehouse(account.id, &name, size, state).await
            }
            .with_context(|| format!("creating warehouse `{name}`"))?;
            print_header();
            print_warehouse(&warehouse);
            print_apply_hint(&warehouse);
        }
        Command::List => {
            let warehouses = db.list_warehouses(account.id).await?;
            if warehouses.is_empty() {
                println!(
                    "account `{}` has no warehouses (create one with \
                     `lldb-qe-warehouse create --name analytics --size 2`)",
                    account.name
                );
                return Ok(());
            }
            print_header();
            for warehouse in &warehouses {
                print_warehouse(warehouse);
            }
        }
        Command::Resize { name, size } => {
            let warehouse = db.resize_warehouse(account.id, &name, size).await?;
            print_header();
            print_warehouse(&warehouse);
            print_apply_hint(&warehouse);
        }
        Command::Suspend { name } => {
            let warehouse = db.suspend_warehouse(account.id, &name).await?;
            print_header();
            print_warehouse(&warehouse);
            print_apply_hint(&warehouse);
        }
        Command::Resume { name } => {
            let warehouse = db.resume_warehouse(account.id, &name).await?;
            print_header();
            print_warehouse(&warehouse);
            print_apply_hint(&warehouse);
        }
        Command::Delete { name } => {
            if db.delete_warehouse(account.id, &name).await? {
                println!("deleted warehouse `{name}` from account `{}`", account.name);
                // The service name is CloudFormation-generated (the stack does not set
                // `serviceName`), so naming it here would be a guess an operator would paste and
                // watch fail. Point at the output that carries the real one, exactly as
                // `print_apply_hint` does for the other transitions.
                println!(
                    "  apply: remove `{name}` from the CDK stack's `warehouses` and deploy, or \
                     `aws ecs delete-service --cluster <ClusterName> \
                     --service <{name} from the WarehouseServices output> --force`"
                );
            } else {
                // Not an error: `delete` is the one operation where "it is already gone" is the
                // outcome the caller wanted. Report it plainly so a script can tell.
                println!(
                    "no warehouse named `{name}` for account `{}` — nothing to delete",
                    account.name
                );
            }
        }
    }
    Ok(())
}

fn print_header() {
    println!("{:<24} {:>5}  {:<10} UPDATED", "NAME", "SIZE", "STATE");
}

fn print_warehouse(warehouse: &Warehouse) {
    println!(
        "{:<24} {:>5}  {:<10} {}",
        warehouse.name,
        warehouse.size,
        warehouse.state,
        warehouse.updated_at.to_rfc3339()
    );
}

/// The half of the operation this binary does *not* perform.
///
/// A warehouse row is desired state. Printing the exact command that reconciles the world with it
/// keeps that boundary honest — and gives whoever ran this a copy-pasteable next step instead of
/// a false impression that compute just changed.
fn print_apply_hint(warehouse: &Warehouse) {
    let desired = match warehouse.state {
        WarehouseState::Running => warehouse.size,
        WarehouseState::Suspended => 0,
    };
    println!(
        "  apply: aws ecs update-service --cluster <ClusterName> \
         --service <{} from the WarehouseServices output> --desired-count {desired}",
        warehouse.name
    );
    println!(
        "         (or redeploy the CDK stack with -c warehouses={}:{}:{}; locally, scale the \
         compose service aliased `{}`: docker compose up -d --scale <service>={desired})",
        warehouse.name, warehouse.size, warehouse.state, warehouse.name
    );
}
