//! `lldb-qe-auth` — the control plane's identity desk: users, API keys, roles and grants.
//!
//! # What this binary does, precisely
//!
//! It **edits rows** in the services database, exactly like `lldb-qe-warehouse` does for compute.
//! The rows it edits are what [`lldb_qe_core::auth`] authenticates against and what
//! [`lldb_qe_core::rbac`] authorizes against, so this is where a deployment goes from "reachable by
//! anyone" to "reachable by the people you issued keys to".
//!
//! # Why a separate binary, and what that implies about *its* security
//!
//! Same reasoning as `lldb-qe-migrate` and `lldb-qe-warehouse`, and here it is load-bearing rather
//! than tidy: **this tool has no authentication of its own.** Its credential is the services
//! database's own — whoever holds `--metadata-url` can create a user, issue a key and grant it
//! everything. That is deliberate and it is the standard bootstrap shape (you cannot authenticate
//! the tool that issues the first credential), but it means the Postgres password *is* the root
//! credential of the whole deployment. Treat it accordingly: it belongs in a secret store, not in a
//! shell history, and this binary belongs on an operator's machine rather than in the query path.
//!
//! Shipping these operations inside the coordinator would have meant any process that can run a
//! query can also grant itself privileges, which is precisely the separation this issue exists to
//! create. It lives in the same image so the binary that writes a grant is the same build as the
//! one that reads it.
//!
//! # The token is shown exactly once
//!
//! `key create` prints the token and nothing stores it — not this binary, not the database, not a
//! log line. Losing it means issuing another key, which is the correct behaviour and the one every
//! credential system with a "recover my token" feature gets wrong.
//!
//!   lldb-qe-auth --metadata-url postgres://lldb@localhost/lldb user create --name alice
//!   lldb-qe-auth --metadata-url postgres://lldb@localhost/lldb key create --user alice --name cli
//!   lldb-qe-auth --metadata-url postgres://lldb@localhost/lldb role create --name analyst
//!   lldb-qe-auth --metadata-url postgres://lldb@localhost/lldb role assign --role analyst --user alice
//!   lldb-qe-auth --metadata-url postgres://lldb@localhost/lldb \
//!     grant --role analyst --privilege SELECT --object-type namespace --object-name lldb.sales
//!   lldb-qe-auth --metadata-url postgres://lldb@localhost/lldb show

use anyhow::{Context, Result, bail};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use lldb_qe_core::auth::{Role, User};
use lldb_qe_core::rbac::{ObjectRef, ObjectType, Privilege};
use lldb_qe_core::{Account, ServicesArgs, ServicesDb, init_tracing, redact_url};

#[derive(Debug, Parser)]
#[command(
    name = "lldb-qe-auth",
    about = "Manage accounts' users, API keys, roles and grants",
    version = lldb_qe_core::BUILD_VERSION
)]
struct Cli {
    /// Tenant every operation here is scoped to. User, role and key names are unique *within* an
    /// account, so two tenants may each have an `alice` and an `analyst`.
    #[arg(long, env = "LLDB_ACCOUNT", default_value = "default", global = true)]
    account: String,

    #[command(flatten)]
    services: ServicesArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage the humans and services that hold credentials.
    #[command(subcommand)]
    User(UserCommand),
    /// Manage API keys.
    #[command(subcommand)]
    Key(KeyCommand),
    /// Manage roles and who holds them.
    #[command(subcommand)]
    Role(RoleCommand),
    /// Give a role a privilege on an object.
    Grant(GrantArgs),
    /// Take a privilege away from a role. Exact, not covering — see the note it prints.
    Revoke(GrantArgs),
    /// Print this account's users, roles, grants and keys.
    Show,
}

#[derive(Debug, Subcommand)]
enum UserCommand {
    Create {
        #[arg(long)]
        name: String,
    },
    List,
    /// Disable a user: every one of their keys stops authenticating, reversibly and without
    /// revoking them one by one.
    Disable {
        #[arg(long)]
        name: String,
    },
    /// Undo `disable`.
    Enable {
        #[arg(long)]
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum KeyCommand {
    /// Issue a key. **Prints the token once**; it cannot be recovered afterwards.
    Create {
        /// User the key authenticates as.
        #[arg(long)]
        user: String,
        /// Label for the key ("cli-laptop", "grafana"), unique per user. This is how you revoke it.
        #[arg(long)]
        name: String,
        /// Expire the key this many days from now. Unset means it never expires — right for a
        /// service, wrong for a human.
        #[arg(long)]
        expires_in_days: Option<i64>,
    },
    List,
    Revoke {
        #[arg(long)]
        user: String,
        #[arg(long)]
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum RoleCommand {
    Create {
        #[arg(long)]
        name: String,
    },
    List,
    /// Give a user a role. Idempotent.
    Assign {
        #[arg(long)]
        role: String,
        #[arg(long)]
        user: String,
    },
    /// Take a role away from a user.
    Unassign {
        #[arg(long)]
        role: String,
        #[arg(long)]
        user: String,
    },
}

/// The shape `grant` and `revoke` share — they name exactly the same tuple, and letting them drift
/// would mean a revoke that cannot address a grant.
#[derive(Debug, clap::Args)]
struct GrantArgs {
    #[arg(long)]
    role: String,
    /// SELECT | INSERT | DELETE | UPDATE | USAGE | CANCEL | ALL (case-insensitive).
    ///
    /// CANCEL is held on a warehouse and permits stopping any query running on it. USAGE — which
    /// every submitter to that warehouse already needs — deliberately does not imply it; ALL does.
    #[arg(long)]
    privilege: String,
    /// catalog | namespace | table | warehouse.
    #[arg(long)]
    object_type: String,
    /// The object: `<catalog>`, `<catalog>.<namespace>`, `<catalog>.<namespace>.<table>`, or a
    /// bare warehouse name. It must match the *resolved* name a query produces — an unqualified
    /// `orders` is rejected rather than silently never matching.
    #[arg(long)]
    object_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    tracing::info!(
        version = lldb_qe_core::BUILD_VERSION,
        "starting lldb-qe-auth"
    );

    // Like `lldb-qe-migrate` and `lldb-qe-warehouse`, and unlike the coordinator: this binary is
    // *about* the control plane, so there is no meaningful unconfigured behaviour.
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
/// creating a tenant as a side effect of a typo'd user command is how ghost tenants accumulate —
/// and a ghost tenant here would hold real credentials.
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
        Command::User(cmd) => run_user(db, account, cmd).await,
        Command::Key(cmd) => run_key(db, account, cmd).await,
        Command::Role(cmd) => run_role(db, account, cmd).await,
        Command::Grant(args) => {
            let (role, privilege, object) = resolve_grant(db, account, &args).await?;
            db.grant(account.id, role.id, privilege, &object).await?;
            println!("granted {privilege} on {object} to role `{}`", role.name);
            Ok(())
        }
        Command::Revoke(args) => {
            let (role, privilege, object) = resolve_grant(db, account, &args).await?;
            if db.revoke(role.id, privilege, &object).await? {
                println!("revoked {privilege} on {object} from role `{}`", role.name);
                // The one thing a person revoking a privilege must be told, because the tool
                // cannot know whether it is true and the consequence is "I revoked it and they
                // still have access".
                println!(
                    "  note: this removed exactly that grant. A broader grant (on the namespace \
                     or catalog) may still cover the same object — check with `lldb-qe-auth show`."
                );
            } else {
                println!(
                    "role `{}` did not hold {privilege} on {object} — nothing to revoke",
                    role.name
                );
            }
            Ok(())
        }
        Command::Show => show(db, account).await,
    }
}

async fn run_user(db: &ServicesDb, account: &Account, command: UserCommand) -> Result<()> {
    match command {
        UserCommand::Create { name } => {
            let user = db.create_user(account.id, &name).await?;
            print_users(&[user]);
            println!(
                "  next: `lldb-qe-auth key create --user {name} --name cli` to issue a credential"
            );
        }
        UserCommand::List => {
            let users = db.list_users(account.id).await?;
            if users.is_empty() {
                println!(
                    "account `{}` has no users (create one with \
                     `lldb-qe-auth user create --name alice`)",
                    account.name
                );
                return Ok(());
            }
            print_users(&users);
        }
        UserCommand::Disable { name } => {
            let user = db.set_user_disabled(account.id, &name, true).await?;
            print_users(&[user]);
            println!("  every API key belonging to `{name}` now fails authentication");
        }
        UserCommand::Enable { name } => {
            let user = db.set_user_disabled(account.id, &name, false).await?;
            print_users(&[user]);
        }
    }
    Ok(())
}

async fn run_key(db: &ServicesDb, account: &Account, command: KeyCommand) -> Result<()> {
    match command {
        KeyCommand::Create {
            user,
            name,
            expires_in_days,
        } => {
            let user = resolve_user(db, account, &user).await?;
            let expires_at = expires_in_days.map(|days| Utc::now() + Duration::days(days));
            let (key, token) = db
                .create_api_key(account.id, user.id, &name, expires_at)
                .await?;
            println!("issued API key `{}` for user `{}`", key.name, user.name);
            println!(
                "  expires: {}",
                key.expires_at
                    .map(|at| at.to_rfc3339())
                    .unwrap_or_else(|| "never".to_string())
            );
            // On its own line, unadorned, so a `| tail -1` in a bootstrap script gets the token and
            // nothing else.
            println!("  this token is shown ONCE and is not recoverable:");
            println!("{}", token.into_secret());
        }
        KeyCommand::List => {
            let keys = db.list_api_keys(account.id).await?;
            if keys.is_empty() {
                println!("account `{}` has no API keys", account.name);
                return Ok(());
            }
            let users = db.list_users(account.id).await?;
            println!(
                "{:<20} {:<20} {:<14} {:<10} LAST USED",
                "USER", "KEY", "PREFIX", "STATE"
            );
            for key in &keys {
                let owner = users
                    .iter()
                    .find(|u| u.id == key.user_id)
                    .map(|u| u.name.as_str())
                    .unwrap_or("<deleted>");
                let state = if key.revoked_at.is_some() {
                    "revoked"
                } else if !key.is_usable_at(Utc::now()) {
                    "expired"
                } else {
                    "active"
                };
                println!(
                    "{:<20} {:<20} {:<14} {:<10} {}",
                    owner,
                    key.name,
                    key.token_prefix,
                    state,
                    key.last_used_at
                        .map(|at| at.to_rfc3339())
                        .unwrap_or_else(|| "never".to_string())
                );
            }
        }
        KeyCommand::Revoke { user, name } => {
            let user = resolve_user(db, account, &user).await?;
            if db.revoke_api_key(account.id, user.id, &name).await? {
                println!("revoked API key `{name}` for user `{}`", user.name);
            } else {
                println!(
                    "user `{}` has no active API key named `{name}` — nothing to revoke",
                    user.name
                );
            }
        }
    }
    Ok(())
}

async fn run_role(db: &ServicesDb, account: &Account, command: RoleCommand) -> Result<()> {
    match command {
        RoleCommand::Create { name } => {
            let role = db.create_role(account.id, &name).await?;
            println!("created role `{}` in account `{}`", role.name, account.name);
            println!(
                "  next: `lldb-qe-auth grant --role {name} --privilege SELECT \
                 --object-type namespace --object-name <catalog>.<namespace>`"
            );
        }
        RoleCommand::List => {
            let roles = db.list_roles(account.id).await?;
            if roles.is_empty() {
                println!("account `{}` has no roles", account.name);
                return Ok(());
            }
            for role in &roles {
                println!("{}", role.name);
            }
        }
        RoleCommand::Assign { role, user } => {
            let role = resolve_role(db, account, &role).await?;
            let user = resolve_user(db, account, &user).await?;
            db.assign_role(account.id, user.id, role.id).await?;
            println!("user `{}` now holds role `{}`", user.name, role.name);
        }
        RoleCommand::Unassign { role, user } => {
            let role = resolve_role(db, account, &role).await?;
            let user = resolve_user(db, account, &user).await?;
            if db.unassign_role(user.id, role.id).await? {
                println!("user `{}` no longer holds role `{}`", user.name, role.name);
            } else {
                println!("user `{}` did not hold role `{}`", user.name, role.name);
            }
        }
    }
    Ok(())
}

/// The whole access picture for one tenant on one screen — which is what an auditor asks for and
/// what a person debugging a `PERMISSION_DENIED` needs.
async fn show(db: &ServicesDb, account: &Account) -> Result<()> {
    println!("account `{}` (id {})", account.name, account.id);

    let users = db.list_users(account.id).await?;
    println!("\nUSERS");
    if users.is_empty() {
        println!("  (none)");
    }
    for user in &users {
        let roles = db.roles_of_user(user.id).await?;
        let roles = if roles.is_empty() {
            "no roles".to_string()
        } else {
            roles
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        println!(
            "  {:<24} {:<10} {roles}",
            user.name,
            if user.is_disabled() {
                "disabled"
            } else {
                "enabled"
            }
        );
    }

    println!("\nGRANTS");
    let grants = db.list_grants(account.id).await?;
    if grants.is_empty() {
        println!("  (none — every query from this account will be denied)");
    }
    for g in &grants {
        println!("  {g}");
    }

    println!("\nAPI KEYS");
    let keys = db.list_api_keys(account.id).await?;
    if keys.is_empty() {
        println!("  (none)");
    }
    for key in &keys {
        let owner = users
            .iter()
            .find(|u| u.id == key.user_id)
            .map(|u| u.name.as_str())
            .unwrap_or("<deleted>");
        println!(
            "  {:<24} {:<20} {}",
            format!("{owner}/{}", key.name),
            key.token_prefix,
            if key.revoked_at.is_some() {
                "revoked"
            } else if key.is_usable_at(Utc::now()) {
                "active"
            } else {
                "expired"
            }
        );
    }
    Ok(())
}

/// Parse and validate a `grant`/`revoke` tuple, resolving the role. Validation happens *here*, at
/// the moment a human types it, because an object name with the wrong number of segments can never
/// match anything and would otherwise be discovered as a mysterious permission denial later.
async fn resolve_grant(
    db: &ServicesDb,
    account: &Account,
    args: &GrantArgs,
) -> Result<(Role, Privilege, ObjectRef)> {
    let privilege: Privilege = args.privilege.parse()?;
    let object_type: ObjectType = args.object_type.parse()?;
    let object = ObjectRef::new(object_type, args.object_name.clone())?;
    let role = resolve_role(db, account, &args.role).await?;
    Ok((role, privilege, object))
}

async fn resolve_user(db: &ServicesDb, account: &Account, name: &str) -> Result<User> {
    db.user_by_name(account.id, name).await?.with_context(|| {
        format!(
            "no user named `{name}` in account `{}` (create one with \
             `lldb-qe-auth user create --name {name}`)",
            account.name
        )
    })
}

async fn resolve_role(db: &ServicesDb, account: &Account, name: &str) -> Result<Role> {
    db.role_by_name(account.id, name).await?.with_context(|| {
        format!(
            "no role named `{name}` in account `{}` (create one with \
             `lldb-qe-auth role create --name {name}`)",
            account.name
        )
    })
}

fn print_users(users: &[User]) {
    println!("{:<24} {:<10} CREATED", "NAME", "STATE");
    for user in users {
        println!(
            "{:<24} {:<10} {}",
            user.name,
            if user.is_disabled() {
                "disabled"
            } else {
                "enabled"
            },
            user.created_at.to_rfc3339()
        );
    }
}
